package hub

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/turbot/go-kit/helpers"
	typehelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/logging"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
	"github.com/turbot/steampipe-postgres-fdw/settings"
	"github.com/turbot/steampipe-postgres-fdw/types"
	"github.com/turbot/steampipe/pkg/constants"
	"github.com/turbot/steampipe/pkg/filepaths"
	"github.com/turbot/steampipe/pkg/steampipeconfig"
	"github.com/turbot/steampipe/pkg/steampipeconfig/modconfig"
	"github.com/turbot/steampipe/pkg/utils"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	rowBufferSize          = 100
	scanMetadataBufferSize = 5000
)

// Hub is a structure representing plugin hub
type Hub struct {
	connections *connectionFactory

	// list of iterators currently executing scans
	runningIterators     map[Iterator]struct{}
	runningIteratorsLock sync.RWMutex

	// cacheSettings
	cacheSettings *settings.HubCacheSettings

	// telemetry properties
	// callback function to shutdown telemetry
	telemetryShutdownFunc func()
	hydrateCallsCounter   metric.Int64Counter

	// array of scan metadata
	// we append to this every time a scan completes (either due to end of data, or Postgres terminating)
	scanMetadata map[int64][]ScanMetadata
	// map of scan totals, keyed by query timestamp (which is a unique query identifier
	queryRowSummary        map[int64]*QueryRowSummary
	scanMetadataLock       sync.RWMutex
	scanMetadataExclusions map[int64]struct{}
}

// global hub instance
var hubSingleton *Hub

// mutex protecting hub creation
var hubMux sync.Mutex

//// lifecycle ////

func GetHub() (*Hub, error) {
	logging.LogTime("GetHub start")

	// lock access to singleton
	hubMux.Lock()
	defer hubMux.Unlock()

	var err error
	if hubSingleton == nil {
		hubSingleton, err = newHub()
		if err != nil {
			return nil, err
		}
	}
	logging.LogTime("GetHub end")
	return hubSingleton, err
}

func newHub() (*Hub, error) {
	hub := &Hub{
		runningIterators:       make(map[Iterator]struct{}),
		scanMetadata:           make(map[int64][]ScanMetadata),
		queryRowSummary:        make(map[int64]*QueryRowSummary),
		scanMetadataExclusions: make(map[int64]struct{}),
	}
	hub.connections = newConnectionFactory(hub)

	// TODO CHECK TELEMETRY ENABLED?
	if err := hub.initialiseTelemetry(); err != nil {
		return nil, err
	}

	// NOTE: Steampipe determine it's install directory from the input arguments (with a default)
	// as we are using shared Steampipe code we must set the install directory.
	// we can derive it from the working directory (which is underneath the install directectory)
	steampipeDir, err := getInstallDirectory()
	if err != nil {
		return nil, err
	}
	filepaths.SteampipeDir = steampipeDir

	log.Printf("[INFO] newHub Hub.LoadConnectionConfig ")
	if _, err := hub.LoadConnectionConfig(); err != nil {
		return nil, err
	}

	hub.cacheSettings = settings.NewCacheSettings(hub.clearConnectionCache, hub.getServerCacheEnabled())

	return hub, nil
}

func (h *Hub) initialiseTelemetry() error {
	log.Printf("[TRACE] init telemetry")
	shutdownTelemetry, err := telemetry.Init(constants.FdwName)
	if err != nil {
		return fmt.Errorf("failed to initialise telemetry: %s", err.Error())
	}

	h.telemetryShutdownFunc = shutdownTelemetry

	hydrateCalls, err := otel.GetMeterProvider().Meter(constants.FdwName).Int64Counter(
		fmt.Sprintf("%s-hydrate_calls_total", constants.FdwName),
		metric.WithDescription("The total number of hydrate calls"),
	)
	if err != nil {
		log.Printf("[WARN] init telemetry failed to create hydrateCallsCounter")
		return err
	}
	h.hydrateCallsCounter = hydrateCalls
	return nil
}

// get the install folder - derive from our working folder
func getInstallDirectory() (string, error) {
	// we need to do this as we are sharing steampipe code to read the config
	// and steampipe may set the install folder from a cmd line arg, so it cannot be hard coded
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return path.Join(wd, "../../.."), nil
}

func (h *Hub) addIterator(iterator Iterator) {
	h.runningIteratorsLock.Lock()
	defer h.runningIteratorsLock.Unlock()

	h.runningIterators[iterator] = struct{}{}

	// if this is a scan iterator, add the scan metadata
}

// RemoveIterator removes an iterator from list of running iterators
func (h *Hub) RemoveIterator(iterator Iterator) {
	h.runningIteratorsLock.Lock()
	defer h.runningIteratorsLock.Unlock()

	delete(h.runningIterators, iterator)
}

// EndScan is called when Postgres terminates the scan (because it has received enough rows of data)
func (h *Hub) EndScan(iter Iterator, limit int64) {
	log.Printf("[INFO] EndScan for iterator %p, status %s", iter, iter.Status())
	// is the iterator still running? If so it means postgres is stopping a scan before all rows have been read
	if iter.Status() == QueryStatusStarted {
		// we normally add the scan metadata when the scan completes - if the scan has not completed, we need to
		// add the metadata here
		h.AddScanMetadata(iter)
		log.Printf("[TRACE] ending scan before iterator complete - limit: %v, iterator: %p", limit, iter)
		iter.Close()
	}
}

func (h *Hub) AddScanMetadata(iter Iterator) {
	h.scanMetadataLock.Lock()
	defer h.scanMetadataLock.Unlock()

	// nothing to do for an in memory iterator
	if _, ok := iter.(*inMemoryIterator); ok {
		return
	}
	queryTimestamp := iter.GetQueryTimestamp()
	// ensure we only keep scan metadata for this query
	h.removeStaleScanMetadata(queryTimestamp)

	log.Printf("[TRACE] AddScanMetadata for iterator %p (%s)", iter, iter.ConnectionName())
	// get the id of the last metadata item we currently have
	// (id starts at 1)

	metadataForQuery := h.scanMetadata[queryTimestamp]
	id := 1
	metadataLen := len(metadataForQuery)
	if metadataLen > 0 {
		id = metadataForQuery[metadataLen-1].Id + 1
	}
	ctx := iter.GetTraceContext().Ctx

	connectionName := iter.ConnectionName()
	connectionPlugin, err := h.getConnectionPlugin(connectionName)
	if err != nil {
		log.Printf("[WARN] AddScanMetadata for iterator %p (%s) failed - error getting connectionPlugin: %s", iter, iter.ConnectionName(), err.Error())
		return
	}

	// get scan metadata from iterator
	scanMetadata := iter.GetScanMetadata()

	for _, m := range scanMetadata {
		// set ID
		m.Id = id
		id++
		log.Printf("[TRACE] got metadata table: %s cache hit: %v, rows fetched %d, hydrate calls: %d",
			m.Table, m.CacheHit, m.RowsFetched, m.HydrateCalls)
		// read the scan metadata from the iterator and add to our stack
		metadataForQuery = append(metadataForQuery, m)

		// hydrate metric labels
		labels := []attribute.KeyValue{
			attribute.String("table", m.Table),
			attribute.String("connection", connectionName),
			attribute.String("plugin", connectionPlugin.PluginName),
		}
		log.Printf("[TRACE] update hydrate calls counter with %d", m.HydrateCalls)
		h.hydrateCallsCounter.Add(ctx, m.HydrateCalls, metric.WithAttributes(labels...))

	}

	// limit size of scan metadataLen
	h.trimScanMetadata(metadataLen)
	log.Printf("[INFO] AddScanMetadata %d entries after trimming ", len(h.scanMetadata))
}

func (h *Hub) trimScanMetadata(metadataLen int) {
	// now trim scan metadata - max 1000 items
	if metadataLen > scanMetadataBufferSize {
		startOffset := metadataLen - scanMetadataBufferSize
		h.scanMetadata = h.scanMetadata[startOffset:]
	}
}

// Close shuts down all plugin clients
func (h *Hub) Close() {
	log.Println("[TRACE] hub: close")

	if h.telemetryShutdownFunc != nil {
		log.Println("[TRACE] shutdown telemetry")
		h.telemetryShutdownFunc()
	}
}

// Abort shuts down currently running queries
func (h *Hub) Abort() {
	log.Printf("[INFO] Hub Abort")
	// for all running iterators
	h.runningIteratorsLock.RLock()
	defer h.runningIteratorsLock.RUnlock()

	for iter := range h.runningIterators {
		// read the scan metadata from the iterator and add to our stack
		h.AddScanMetadata(iter)
		// close the iterator
		iter.Close()
	}
	// clear running iterators
	h.runningIterators = nil
}

//// public fdw functions ////

// GetSchema returns the schema for a name. Load the plugin for the connection if needed
func (h *Hub) GetSchema(remoteSchema string, localSchema string) (*proto.Schema, error) {
	log.Printf("[TRACE] Hub GetSchema %s %s", remoteSchema, localSchema)
	pluginFQN := remoteSchema
	connectionName := localSchema
	log.Printf("[TRACE] getSchema remoteSchema: %s, name %s\n", remoteSchema, connectionName)

	return h.connections.getSchema(pluginFQN, connectionName)
}

// GetIterator creates and returns an iterator
func (h *Hub) GetIterator(columns []string, quals *proto.Quals, unhandledRestrictions int, limit int64, opts types.Options, queryTimestamp int64) (Iterator, error) {
	logging.LogTime("GetIterator start")
	qualMap, err := h.buildQualMap(quals)
	connectionName := opts["connection"]
	table := opts["table"]
	log.Printf("[TRACE] Hub GetIterator() table '%s'", table)

	if connectionName == constants.InternalSchema || connectionName == constants.LegacyCommandSchema {
		return h.executeCommandScan(connectionName, table, queryTimestamp)
	}

	// create a span for this scan
	scanTraceCtx := h.traceContextForScan(table, columns, limit, qualMap, connectionName)
	iterator, err := h.startScanForConnection(connectionName, table, qualMap, unhandledRestrictions, columns, limit, scanTraceCtx, queryTimestamp)

	if err != nil {
		log.Printf("[TRACE] Hub GetIterator() failed :( %s", err)
		return nil, err
	}
	log.Printf("[TRACE] Hub GetIterator() created iterator (%p)", iterator)

	return iterator, nil
}

// LoadConnectionConfig loads the connection config and returns whether it has changed
func (h *Hub) LoadConnectionConfig() (bool, error) {
	log.Printf("[INFO] Hub.LoadConnectionConfig ")
	// load connection conFig
	connectionConfig, errorsAndWarnings := steampipeconfig.LoadConnectionConfig()
	if errorsAndWarnings.GetError() != nil {
		log.Printf("[WARN] LoadConnectionConfig failed %v ", errorsAndWarnings)
		return false, errorsAndWarnings.GetError()
	}

	configChanged := steampipeconfig.GlobalConfig == connectionConfig
	steampipeconfig.GlobalConfig = connectionConfig

	return configChanged, nil
}

// GetRelSize is a method called from the planner to estimate the resulting relation size for a scan.
//
//	It will help the planner in deciding between different types of plans,
//	according to their costs.
//	Args:
//	    columns (list): The list of columns that must be returned.
//	    quals (list): A list of Qual instances describing the filters
//	        applied to this scan.
//	Returns:
//	    A struct of the form (expected_number_of_rows, avg_row_width (in bytes))
func (h *Hub) GetRelSize(columns []string, quals []*proto.Qual, opts types.Options) (types.RelSize, error) {
	result := types.RelSize{
		// Default to 1M rows, because these tables are typically expensive
		// relative to standard postgres.
		Rows: 1000000,
		// Width is in bytes, assuming an average of 100 per column.
		Width: 100 * len(columns),
	}
	return result, nil
}

// GetPathKeys Is a method called from the planner to add additional Path to the planner.
//
//	By default, the planner generates an (unparameterized) path, which
//	can be reasoned about like a SequentialScan, optionally filtered.
//	This method allows the implementor to declare other Paths,
//	corresponding to faster access methods for specific attributes.
//	Such a parameterized path can be reasoned about like an IndexScan.
//	For example, with the following query::
//	    select * from foreign_table inner join local_table using(id);
//	where foreign_table is a foreign table containing 100000 rows, and
//	local_table is a regular table containing 100 rows.
//	The previous query would probably be transformed to a plan similar to
//	this one::
//	    ┌────────────────────────────────────────────────────────────────────────────────────┐
//	    │                                     QUERY PLAN                                     │
//	    ├────────────────────────────────────────────────────────────────────────────────────┤
//	    │ Hash Join  (cost=57.67..4021812.67 rows=615000 width=68)                           │
//	    │   Hash Cond: (foreign_table.id = local_table.id)                                   │
//	    │   ->  Foreign Scan on foreign_table (cost=20.00..4000000.00 rows=100000 width=40)  │
//	    │   ->  Hash  (cost=22.30..22.30 rows=1230 width=36)                                 │
//	    │         ->  Seq Scan on local_table (cost=0.00..22.30 rows=1230 width=36)          │
//	    └────────────────────────────────────────────────────────────────────────────────────┘
//	But with a parameterized path declared on the id key, with the knowledge that this key
//	is unique on the foreign side, the following plan might get chosen::
//	    ┌───────────────────────────────────────────────────────────────────────┐
//	    │                              QUERY PLAN                               │
//	    ├───────────────────────────────────────────────────────────────────────┤
//	    │ Nested Loop  (cost=20.00..49234.60 rows=615000 width=68)              │
//	    │   ->  Seq Scan on local_table (cost=0.00..22.30 rows=1230 width=36)   │
//	    │   ->  Foreign Scan on remote_table (cost=20.00..40.00 rows=1 width=40)│
//	    │         Filter: (id = local_table.id)                                 │
//	    └───────────────────────────────────────────────────────────────────────┘
//	Returns:
//	    A list of tuples of the form: (key_columns, expected_rows),
//	    where key_columns is a tuple containing the columns on which
//	    the path can be used, and expected_rows is the number of rows
//	    this path might return for a simple lookup.
//	    For example, the return value corresponding to the previous scenario would be::
//	        [(('id',), 1)]
func (h *Hub) GetPathKeys(opts types.Options) ([]types.PathKey, error) {
	connectionName := opts["connection"]
	table := opts["table"]

	log.Printf("[TRACE] hub.GetPathKeys for connection '%s`, table `%s`", connectionName, table)

	// get the schema for this connection
	connectionPlugin, err := h.getConnectionPlugin(connectionName)
	if err != nil {
		return nil, err
	}

	connectionSchema, err := connectionPlugin.GetSchema(connectionName)
	if err != nil {
		return nil, err
	}
	tableSchema, ok := connectionSchema.Schema[table]
	if !ok {
		return nil, fmt.Errorf("no schema loaded for connection '%s', table '%s'", connectionName, table)
	}
	var allColumns = make([]string, len(tableSchema.Columns))
	for i, c := range tableSchema.Columns {
		allColumns[i] = c.Name
	}

	var pathKeys []types.PathKey

	// build path keys based on the table key columns
	// NOTE: the schema data has changed in SDK version 1.3 - we must handle plugins using legacy sdk explicitly
	// check for legacy sdk versions
	if tableSchema.ListCallKeyColumns != nil {
		log.Printf("[TRACE] schema response include ListCallKeyColumns, it is using legacy protobuff interface ")
		pathKeys = types.LegacyKeyColumnsToPathKeys(tableSchema.ListCallKeyColumns, tableSchema.ListCallOptionalKeyColumns, allColumns)
	} else if tableSchema.ListCallKeyColumnList != nil {
		log.Printf("[TRACE] schema response include ListCallKeyColumnList, it is using the updated protobuff interface ")
		// generate path keys if there are required list key columns
		// this increases the chances that Postgres will generate a plan which provides the quals when querying the table
		pathKeys = types.KeyColumnsToPathKeys(tableSchema.ListCallKeyColumnList, allColumns)
	}
	// NOTE: in the future we may (optionally) add in path keys for Get call key columns.
	// We do not do this by default as it is likely to actually reduce join performance in the general case,
	// particularly when caching is taken into account

	//var getCallPathKeys []types.PathKey
	//if getKeyColumns := schema.GetCallKeyColumns; getKeyColumns != nil {
	//	getCallPathKeys = types.KeyColumnsToPathKeys(getKeyColumns)
	//}
	//pathKeys := types.MergePathKeys(getCallPathKeys, listCallPathKeys)

	log.Printf("[TRACE] GetPathKeys for connection '%s`, table `%s` returning", connectionName, table)
	return pathKeys, nil
}

// Explain ::  hook called on explain.
//
//	Returns:
//	    An iterable of strings to display in the EXPLAIN output.
func (h *Hub) Explain(columns []string, quals []*proto.Qual, sortKeys []string, verbose bool, opts types.Options) ([]string, error) {
	return make([]string, 0), nil
}

//// internal implementation ////

func (h *Hub) traceContextForScan(table string, columns []string, limit int64, qualMap map[string]*proto.Quals, connectionName string) *telemetry.TraceCtx {
	ctx, span := telemetry.StartSpan(context.Background(), constants.FdwName, "Hub.Scan (%s)", table)
	span.SetAttributes(
		attribute.StringSlice("columns", columns),
		attribute.String("table", table),
		attribute.String("quals", grpc.QualMapToString(qualMap, false)),
		attribute.String("connection", connectionName),
	)
	if limit != -1 {
		span.SetAttributes(attribute.Int64("limit", limit))
	}
	return &telemetry.TraceCtx{Ctx: ctx, Span: span}
}

// startScanForConnection starts a scan for a single connection, using a scanIterator or a legacyScanIterator
func (h *Hub) startScanForConnection(connectionName string, table string, qualMap map[string]*proto.Quals, unhandledRestrictions int, columns []string, limit int64, scanTraceCtx *telemetry.TraceCtx, queryTimestamp int64) (_ Iterator, err error) {
	defer func() {
		if err != nil {
			// close the span in case of errir
			scanTraceCtx.Span.End()
		}
	}()

	log.Printf("[TRACE] Hub startScanForConnection '%s'", connectionName)
	// get connection plugin for this connection
	connectionPlugin, err := h.getConnectionPlugin(connectionName)
	if err != nil {
		log.Printf("[TRACE] getConnectionPlugin failed: %s", err.Error())
		return nil, err
	}

	// ok so this is a multi connection plugin, build list of connections,
	// if this connection is NOT an aggregator, only execute for the named connection

	// get connection config
	connectionConfig, ok := steampipeconfig.GlobalConfig.Connections[connectionName]
	if !ok {
		return nil, fmt.Errorf("no connection config loaded for connection '%s'", connectionName)
	}

	var connectionNames = []string{connectionName}
	if connectionConfig.Type == modconfig.ConnectionTypeAggregator {
		connectionNames = connectionConfig.GetResolveConnectionNames()
		// if there are no connections, do not proceed
		if len(connectionNames) == 0 {
			return nil, errors.New(connectionConfig.GetEmptyAggregatorError())
		}
	}

	// for each connection, determine whether to pushdown the limit
	connectionLimitMap, err := h.buildConnectionLimitMap(table, qualMap, unhandledRestrictions, connectionNames, limit, connectionPlugin)
	if err != nil {
		return nil, err
	}

	if len(qualMap) > 0 {
		log.Printf("[INFO] connection '%s', table '%s', quals %s", connectionName, table, grpc.QualMapToString(qualMap, true))
	} else {
		log.Println("[INFO] --------")
		log.Println("[INFO] no quals")
		log.Println("[INFO] --------")
	}

	log.Printf("[TRACE] startScanForConnection creating a new scan iterator")
	iterator := newScanIterator(h, connectionPlugin, connectionName, table, connectionLimitMap, qualMap, columns, limit, scanTraceCtx, queryTimestamp)
	return iterator, nil
}

func (h *Hub) buildConnectionLimitMap(table string, qualMap map[string]*proto.Quals, unhandledRestrictions int, connectionNames []string, limit int64, connectionPlugin *steampipeconfig.ConnectionPlugin) (map[string]int64, error) {
	log.Printf("[INFO] buildConnectionLimitMap, table: '%s', %d %s, limit: %d", table, len(connectionNames), utils.Pluralize("connection", len(connectionNames)), limit)

	connectionSchema, err := connectionPlugin.GetSchema(connectionNames[0])
	if err != nil {
		return nil, err
	}
	schemaMode := connectionSchema.Mode

	// pushing the limit down or not is dependent on the schema.
	// for a static schema, the limit will be the same for all connections (i.e. we either pushdown for all or none)
	// check once whether we should push down
	if limit != -1 && schemaMode == plugin.SchemaModeStatic {
		log.Printf("[INFO] static schema - using same limit for all connections")
		if !h.shouldPushdownLimit(table, qualMap, unhandledRestrictions, connectionSchema) {
			limit = -1
		}
	}

	// set the limit for the one and only connection
	var connectionLimitMap = make(map[string]int64)
	for _, c := range connectionNames {
		connectionLimit := limit
		// if schema mode is dynamic, check whether we should push down for each connection
		if schemaMode == plugin.SchemaModeDynamic && !h.shouldPushdownLimit(table, qualMap, unhandledRestrictions, connectionSchema) {
			log.Printf("[INFO] not pushing limit down for connection %s", c)
			connectionLimit = -1
		}
		log.Printf("[INFO] pushing limit down for connection %s", c)

		connectionLimitMap[c] = connectionLimit
	}

	return connectionLimitMap, nil
}

// determine whether to include the limit, based on the quals
// we ONLY pushdown the limit if all quals have corresponding key columns,
// and if the qual operator is supported by the key column
func (h *Hub) shouldPushdownLimit(table string, qualMap map[string]*proto.Quals, unhandledRestrictions int, connectionSchema *proto.Schema) bool {
	// if we have any unhandled restrictions, we CANNOT push limit down
	if unhandledRestrictions > 0 {
		return false
	}

	// build a map of all key columns
	tableSchema, ok := connectionSchema.Schema[table]
	if !ok {
		// any errors, just default to NOT pushing down the limit
		return false
	}
	var keyColumnMap = make(map[string]*proto.KeyColumn)
	for _, k := range tableSchema.ListCallKeyColumnList {
		keyColumnMap[k.Name] = k
	}
	for _, k := range tableSchema.GetCallKeyColumnList {
		keyColumnMap[k.Name] = k
	}

	// for every qual, determine if it has a key column and if the operator is supported
	// if NOT, we cannot push down the limit

	for col, quals := range qualMap {
		// check whether this qual is declared as a key column for this table
		if k, ok := keyColumnMap[col]; ok {
			log.Printf("[TRACE] shouldPushdownLimit found key column for column %s: %v", col, k)

			// check whether every qual for this column has a supported operator
			for _, q := range quals.Quals {
				operator := q.GetStringValue()
				if !helpers.StringSliceContains(k.Operators, operator) {
					log.Printf("[INFO] operator '%s' not supported for column '%s'. NOT pushing down limit", operator, col)
					return false
				}
				log.Printf("[TRACE] shouldPushdownLimit operator '%s' is supported for column '%s'.", operator, col)
			}
		} else {
			// no key column defined for this qual - DO NOT push down the limit
			log.Printf("[INFO] shouldPushdownLimit no key column found for column '%s'. NOT pushing down limit", col)
			return false
		}
	}

	// all quals are supported - push down limit
	log.Printf("[INFO] shouldPushdownLimit all quals are supported - pushing down limit")
	return true
}

// StartScan starts a scan (for scanIterators only = legacy iterators will have already started)
func (h *Hub) StartScan(i Iterator) error {
	// iterator must be a scan iterator
	// if iterator is not a scan iterator, do nothing
	iterator, ok := i.(*scanIterator)
	if !ok {
		return nil
	}

	table := iterator.table
	connectionPlugin := iterator.connectionPlugin

	req := &proto.ExecuteRequest{
		Table:        table,
		QueryContext: iterator.queryContext,
		CallId:       iterator.callId,
		// pass connection name - used for aggregators
		Connection:            iterator.ConnectionName(),
		TraceContext:          grpc.CreateCarrierFromContext(iterator.traceCtx.Ctx),
		ExecuteConnectionData: make(map[string]*proto.ExecuteConnectionData),
	}

	// build executeConnectionData map
	for connectionName, limit := range iterator.connectionLimitMap {
		data := &proto.ExecuteConnectionData{}
		if limit != -1 {
			data.Limit = &proto.NullableInt{Value: limit}
		}
		data.CacheTtl = int64(h.cacheTTL(connectionName).Seconds())
		data.CacheEnabled = h.cacheEnabled(connectionName)

		req.ExecuteConnectionData[connectionName] = data
	}

	log.Printf("[INFO] StartScan for table: %s, cache enabled: %v, iterator %p, %d quals (%s)", table, req.CacheEnabled, iterator, len(iterator.queryContext.Quals), iterator.callId)
	stream, ctx, cancel, err := connectionPlugin.PluginClient.Execute(req)
	// format GRPC errors and ignore not implemented errors for backwards compatibility
	err = grpc.HandleGrpcError(err, connectionPlugin.PluginName, "Execute")
	if err != nil {
		log.Printf("[WARN] startScan: plugin Execute function callId: %s returned error: %v\n", iterator.callId, err)
		iterator.setError(err)
		return err
	}
	iterator.Start(stream, ctx, cancel)

	// add iterator to running list
	h.addIterator(iterator)

	return nil
}

// getConnectionPlugin returns the connectionPlugin for the provided connection
// it also makes sure that the plugin is up and running.
// if the plugin is not running, it attempts to restart the plugin - errors if unable
func (h *Hub) getConnectionPlugin(connectionName string) (*steampipeconfig.ConnectionPlugin, error) {
	log.Printf("[TRACE] hub.getConnectionPlugin for connection '%s`", connectionName)

	// get the plugin FQN
	connectionConfig, ok := steampipeconfig.GlobalConfig.Connections[connectionName]
	if !ok {
		log.Printf("[WARN] no connection config loaded for connection '%s'", connectionName)
		return nil, fmt.Errorf("no connection config loaded for connection '%s'", connectionName)
	}
	pluginFQN := connectionConfig.Plugin

	// ask connection map to get or create this connection
	c, err := h.connections.getOrCreate(pluginFQN, connectionName)
	if err != nil {
		log.Printf("[TRACE] getConnectionPlugin getConnectionPlugin failed: %s", err.Error())
		return nil, err
	}

	return c, nil
}

func (h *Hub) cacheEnabled(connectionName string) bool {
	// if the caching is disabled for the server, just return false
	if !h.cacheSettings.ServerCacheEnabled {
		return false
	}

	if h.cacheSettings.ClientCacheEnabled != nil {
		return *h.cacheSettings.ClientCacheEnabled
	}
	// ask the steampipe config for resolved plugin options - this will use default values where needed
	connectionOptions := steampipeconfig.GlobalConfig.GetConnectionOptions(connectionName)

	// the config loading code should ALWAYS populate the connection options, using defaults if needed
	if connectionOptions.Cache == nil {
		panic(fmt.Sprintf("No cache options found for connection %s", connectionName))
	}
	return *connectionOptions.Cache
}

func (h *Hub) cacheTTL(connectionName string) time.Duration {
	// if the cache ttl has been overridden, then enforce the value
	if h.cacheSettings.Ttl != nil {
		return *h.cacheSettings.Ttl
	}

	// ask the steampipe config for resolved plugin options - this will use default values where needed
	connectionOptions := steampipeconfig.GlobalConfig.GetConnectionOptions(connectionName)

	// the config loading code should ALWAYS populate the connection options, using defaults if needed
	if connectionOptions.CacheTTL == nil {
		panic(fmt.Sprintf("No cache options found for connection %s", connectionName))
	}

	ttl := time.Duration(*connectionOptions.CacheTTL) * time.Second

	// would this give data earlier than the cacheClearTime
	now := time.Now()
	if now.Add(-ttl).Before(h.cacheSettings.ClearTime) {
		ttl = now.Sub(h.cacheSettings.ClearTime)
	}
	return ttl
}

func (h *Hub) ApplySetting(key string, value string) error {
	log.Printf("[TRACE] ApplySetting [%s => %s]", key, value)
	return h.cacheSettings.Apply(key, value)
}

func (h *Hub) GetSettingsSchema() map[string]*proto.TableSchema {
	return map[string]*proto.TableSchema{
		constants.ForeignTableSettings: {
			Columns: []*proto.ColumnDefinition{
				{Name: constants.ForeignTableSettingsKeyColumn, Type: proto.ColumnType_STRING},
				{Name: constants.ForeignTableSettingsValueColumn, Type: proto.ColumnType_STRING},
			},
		},
		constants.ForeignTableScanMetadata: {
			Columns: []*proto.ColumnDefinition{
				{Name: "id", Type: proto.ColumnType_INT},
				{Name: "connection", Type: proto.ColumnType_STRING},
				{Name: "table", Type: proto.ColumnType_STRING},
				{Name: "cache_hit", Type: proto.ColumnType_BOOL},
				{Name: "rows_fetched", Type: proto.ColumnType_INT},
				{Name: "hydrate_calls", Type: proto.ColumnType_INT},
				{Name: "start_time", Type: proto.ColumnType_TIMESTAMP},
				{Name: "duration_ms", Type: proto.ColumnType_INT},
				{Name: "columns", Type: proto.ColumnType_JSON},
				{Name: "limit", Type: proto.ColumnType_INT},
				{Name: "quals", Type: proto.ColumnType_JSON},
			},
		},
	}
}

func (h *Hub) GetLegacySettingsSchema() map[string]*proto.TableSchema {
	return map[string]*proto.TableSchema{
		constants.LegacyCommandTableCache: {
			Columns: []*proto.ColumnDefinition{
				{Name: constants.LegacyCommandTableCacheOperationColumn, Type: proto.ColumnType_STRING},
			},
		},
		constants.LegacyCommandTableScanMetadata: {
			Columns: []*proto.ColumnDefinition{
				{Name: "id", Type: proto.ColumnType_INT},
				{Name: "table", Type: proto.ColumnType_STRING},
				{Name: "cache_hit", Type: proto.ColumnType_BOOL},
				{Name: "rows_fetched", Type: proto.ColumnType_INT},
				{Name: "hydrate_calls", Type: proto.ColumnType_INT},
				{Name: "start_time", Type: proto.ColumnType_TIMESTAMP},
				{Name: "duration", Type: proto.ColumnType_DOUBLE},
				{Name: "columns", Type: proto.ColumnType_JSON},
				{Name: "limit", Type: proto.ColumnType_INT},
				{Name: "quals", Type: proto.ColumnType_STRING},
			},
		},
	}
}

func (h *Hub) executeCommandScan(connectionName, table string, queryTimestamp int64) (Iterator, error) {
	switch table {
	case constants.ForeignTableScanMetadata, constants.LegacyCommandTableScanMetadata:
		h.scanMetadataLock.RLock()
		res := &QueryResult{
			Rows: make([]map[string]interface{}, len(h.scanMetadata)),
		}
		for i, m := range h.scanMetadata {
			res.Rows[i] = m.AsResultRow()
		}
		h.scanMetadataLock.RUnlock()
		return newInMemoryIterator(connectionName, res, queryTimestamp), nil
	default:
		return nil, fmt.Errorf("cannot select from command table '%s'", table)
	}
}

func (h *Hub) HandleLegacyCacheCommand(command string) error {
	if err := h.ValidateCacheCommand(command); err != nil {
		return err
	}

	log.Printf("[TRACE] HandleLegacyCacheCommand %s", command)

	switch command {
	case constants.LegacyCommandCacheClear:
		// set the cache clear time for the remote query cache
		h.cacheSettings.Apply(string(settings.SettingKeyCacheClearTimeOverride), "")

	case constants.LegacyCommandCacheOn:
		h.cacheSettings.Apply(string(settings.SettingKeyCacheEnabled), "true")
	case constants.LegacyCommandCacheOff:
		h.cacheSettings.Apply(string(settings.SettingKeyCacheClearTimeOverride), "false")
	}
	return nil
}

func (h *Hub) ValidateCacheCommand(command string) error {
	validCommands := []string{constants.LegacyCommandCacheClear, constants.LegacyCommandCacheOn, constants.LegacyCommandCacheOff}

	if !helpers.StringSliceContains(validCommands, command) {
		return fmt.Errorf("invalid command '%s' - supported commands are %s", command, strings.Join(validCommands, ","))
	}
	return nil
}

func (h *Hub) clearConnectionCache(connection string) error {
	log.Printf("[INFO] clear connection cache for connection '%s'", connection)
	connectionPlugin, err := h.getConnectionPlugin(connection)
	if err != nil {
		log.Printf("[WARN] clearConnectionCache failed for connection %s: %s", connection, err)
		return err
	}

	_, err = connectionPlugin.PluginClient.SetConnectionCacheOptions(&proto.SetConnectionCacheOptionsRequest{ClearCacheForConnection: connection})
	if err != nil {
		log.Printf("[WARN] clearConnectionCache failed for connection %s: SetConnectionCacheOptions returned %s", connection, err)
	}
	log.Printf("[INFO] clear connection cache succeeded")
	return err
}

// resolve the server cache enabled property
func (h *Hub) getServerCacheEnabled() bool {
	var res = true
	if val, ok := os.LookupEnv(constants.EnvCacheEnabled); ok {
		if boolVal, err := typehelpers.ToBool(val); err == nil {
			res = boolVal
		}
	}

	if steampipeconfig.GlobalConfig.DatabaseOptions != nil && steampipeconfig.GlobalConfig.DatabaseOptions.Cache != nil {
		res = *steampipeconfig.GlobalConfig.DatabaseOptions.Cache
	}

	log.Printf("[INFO] Hub.getServerCacheEnabled returning %v", res)

	return res
}

func (h *Hub) removeStaleScanMetadata(timestamp int64) {
	// if we have exceeded our maximum length, remove all  metadata
	// and add the current timestamp to the list of long queries for which we will not save granular timing
	if len(h.scanMetadata) > scanMetadataBufferSize {
		h.scanMetadata = map[int64][]ScanMetadata{}
		h.scanMetadataExclusions[timestamp] = struct{}{}
		return
	}

	// clear all query metadata for previous queries
	for existingTimestamp := range h.scanMetadata {
		if existingTimestamp != timestamp {
			delete(h.queryRowSummary, existingTimestamp)
		}
	}
}
