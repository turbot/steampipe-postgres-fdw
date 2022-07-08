package hub

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v3/logging"
	"github.com/turbot/steampipe-plugin-sdk/v3/telemetry"
	"github.com/turbot/steampipe-postgres-fdw/types"
	"github.com/turbot/steampipe/pkg/constants"
	"github.com/turbot/steampipe/pkg/filepaths"
	"github.com/turbot/steampipe/pkg/steampipeconfig"
	"github.com/turbot/steampipe/pkg/steampipeconfig/modconfig"
	"github.com/turbot/steampipe/pluginmanager"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
)

const (
	rowBufferSize = 100
)

// Hub is a structure representing plugin hub
type Hub struct {
	connections      *connectionFactory
	steampipeConfig  *steampipeconfig.SteampipeConfig
	runningIterators []Iterator

	// if the cache is enabled/disabled by a metacommand, this will be non null
	overrideCacheEnabled *bool

	// the earliest time we will accept cached data from
	// when the there is a cache clear command, this is reset to time.Now()
	cacheClearTime time.Time

	timingLock   sync.Mutex
	lastScanTime time.Time

	// telemetry properties
	// callback function to shutdown telemetry
	telemetryShutdownFunc func()
	hydrateCallsCounter   syncint64.Counter

	// array of scan metadata
	// we append to this every time a scan completes (either due to end of data, or Postgres terminating)
	// the full array is returned whenever a pop_scan_metadata command is received and the array is cleared
	scanMetadata []ScanMetadata
}

// global hub instance
var hubSingleton *Hub

// mutex protecting hub creation
var hubMux sync.Mutex

//// lifecycle ////

// GetHub returns a hub singleton
// if there is an existing hub singleton instance return it, otherwise create it
// if a hub exists, but a different pluginDir is specified, reinitialise the hub with the new dir
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
	hub := &Hub{}
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

	if _, err := hub.LoadConnectionConfig(); err != nil {
		return nil, err
	}

	return hub, nil
}

func (h *Hub) initialiseTelemetry() error {
	log.Printf("[TRACE] init telemetry")
	shutdownTelemetry, err := telemetry.Init(constants.FdwName)
	if err != nil {
		return fmt.Errorf("failed to initialise telemetry: %s", err.Error())
	}

	h.telemetryShutdownFunc = shutdownTelemetry

	meter := global.Meter(constants.FdwName)
	hydrateCalls, err := meter.SyncInt64().Counter(
		fmt.Sprintf("%s/hydrate_calls_total", constants.FdwName),
		instrument.WithDescription("The total number of hydrate calls"),
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
	h.runningIterators = append(h.runningIterators, iterator)
}

// RemoveIterator removes an iterator from list of running iterators
func (h *Hub) RemoveIterator(iterator Iterator) {
	for idx, it := range h.runningIterators {
		if it == iterator {
			// remove from list
			h.runningIterators = append(h.runningIterators[:idx], h.runningIterators[idx+1:]...)
			return
		}
	}
}

// EndScan is called when Postgres terminates the scan (because it has received enough rows of data)
func (h *Hub) EndScan(iter Iterator, limit int64) {
	// is the iterator still running? If so it means postgres is stopping a scan before all rows have been read
	if iter.Status() == QueryStatusStarted {
		h.AddScanMetadata(iter)

		// if we have identified a limit from the query (i.e. it is an ungrouped, unordered query from a single table)
		// then we can cache the result, using the limit in teh
		// but if we have NOT extracted a limit, w e cannot cache the results as we are not certain they are complete
		writeToCache := limit != -1
		log.Printf("[TRACE] ending scan before iterator complete - limit: %v, writeToCache: %v, iterator: %p", limit, writeToCache, iter)
		iter.Close(writeToCache)
	}

	h.RemoveIterator(iter)
}

// AddScanMetadata adds the scan metadata from the given iterator to the hubs array
// we append to this every time a scan completes (either due to end of data, or Postgres terminating)
// the full array is returned whenever a pop_scan_metadata command is received and the array is cleared
func (h *Hub) AddScanMetadata(iter Iterator) {
	log.Printf("[TRACE] AddScanMetadata for iterator %p (%s)", iter, iter.ConnectionName())
	// get the id of the last metadata item we currently have
	// (id starts at 1)
	id := 1
	metadataLen := len(h.scanMetadata)
	if metadataLen > 0 {
		id = h.scanMetadata[metadataLen-1].Id + 1
	}
	ctx := iter.GetTraceContext().Ctx

	// if this is a group iterator, recurse into AddScanMetadata for each underlyinh iterator
	if g, ok := iter.(*groupIterator); ok {
		for _, i := range g.Iterators {
			h.AddScanMetadata(i)
		}
		return
	}

	connectionName := iter.ConnectionName()
	connectionPlugin, _ := h.getConnectionPlugin(connectionName)

	// get list of scan metadata from iterator (may be more than 1 for group_iterator)
	scanMetadata := iter.GetScanMetadata()
	for _, m := range scanMetadata {
		// set ID
		m.Id = id
		id++
		log.Printf("[TRACE] got metadata table: %s cache hit: %v, rows fetched %d, hydrate calls: %d",
			m.Table, m.CacheHit, m.RowsFetched, m.HydrateCalls)
		// read the scan metadata from the iterator and add to our stack
		h.scanMetadata = append(h.scanMetadata, m)

		// hydrate metric labels
		labels := []attribute.KeyValue{
			attribute.String("table", m.Table),
			attribute.String("connection", connectionName),
			attribute.String("plugin", connectionPlugin.PluginName),
		}
		log.Printf("[TRACE] update hydrate calls counter with %d", m.HydrateCalls)
		h.hydrateCallsCounter.Add(ctx, m.HydrateCalls, labels...)
	}

	// now trim scan metadata - max 1000 items
	const maxMetadataItems = 1000
	if metadataLen > maxMetadataItems {
		startOffset := maxMetadataItems - 1000
		h.scanMetadata = h.scanMetadata[startOffset:]
	}
}

// ClearScanMetadata deletes all stored scan metadata. It is called by steampipe after retrieving timing information
// for the previous query
func (h *Hub) ClearScanMetadata() {
	h.scanMetadata = nil
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
	log.Printf("[TRACE] Hub Abort")
	// for all running iterators
	for _, iter := range h.runningIterators {
		// read the scan metadata from the iterator and add to our stack
		h.AddScanMetadata(iter)
		// close the iterator, telling it NOT to cache its results
		iter.Close(false)

		// remove it from the saved list of iterators
		h.RemoveIterator(iter)
	}
}

//// public fdw functions ////

// GetSchema returns the schema for a name. Load the plugin for the connection if needed
func (h *Hub) GetSchema(remoteSchema string, localSchema string) (*proto.Schema, error) {
	log.Printf("[TRACE] Hub GetSchema %s %s", remoteSchema, localSchema)
	pluginFQN := remoteSchema
	connectionName := localSchema
	log.Printf("[TRACE] getSchema remoteSchema: %s, name %s\n", remoteSchema, connectionName)

	// if this is an aggregate connection, get the name of the first child connection
	// - we will use this to retrieve the schema
	if h.IsAggregatorConnection(connectionName) {
		connectionName = h.GetAggregateConnectionChild(connectionName)
		log.Printf("[TRACE] getSchema %s is an aggregator - getting schema for first child %s\n", localSchema, connectionName)
	}

	return h.connections.getSchema(pluginFQN, connectionName)
}

// Scan starts a table scan and returns an iterator
func (h *Hub) Scan(columns []string, quals *proto.Quals, limit int64, opts types.Options) (Iterator, error) {
	logging.LogTime("Scan start")
	qualMap, err := h.buildQualMap(quals)
	connectionName := opts["connection"]
	table := opts["table"]
	log.Printf("[TRACE] Hub Scan() table '%s'", table)

	if connectionName == constants.CommandSchema {
		return h.executeCommandScan(table)
	}

	// create a span for this scan
	scanTraceCtx := h.traceContextForScan(table, columns, limit, qualMap, connectionName)

	connectionConfig, _ := h.steampipeConfig.Connections[connectionName]

	var iterator Iterator
	// if this is an aggregate connection, create a group iterator
	if h.IsAggregatorConnection(connectionName) {
		iterator, err = NewGroupIterator(connectionName, table, qualMap, columns, limit, connectionConfig, h, scanTraceCtx)
		log.Printf("[TRACE] Hub Scan() created aggregate iterator (%p)", iterator)

	} else {
		iterator, err = h.startScanForConnection(connectionName, table, qualMap, columns, limit, scanTraceCtx)
		log.Printf("[TRACE] Hub Scan() created iterator (%p)", iterator)
	}

	if err != nil {
		log.Printf("[TRACE] Hub Scan() failed :( %s", err)
		return nil, err
	}

	// store the iterator
	h.addIterator(iterator)
	return iterator, nil
}

// LoadConnectionConfig loads the connection config and returns whether it has changed
func (h *Hub) LoadConnectionConfig() (bool, error) {
	// load connection conFig
	connectionConfig, err := steampipeconfig.LoadConnectionConfig()
	if err != nil {
		log.Printf("[WARN] LoadConnectionConfig failed %v ", err)
		return false, err
	}

	configChanged := h.steampipeConfig == connectionConfig
	h.steampipeConfig = connectionConfig

	return configChanged, nil
}

// GetRelSize is a method called from the planner to estimate the resulting relation size for a scan.
//        It will help the planner in deciding between different types of plans,
//        according to their costs.
//        Args:
//            columns (list): The list of columns that must be returned.
//            quals (list): A list of Qual instances describing the filters
//                applied to this scan.
//        Returns:
//            A struct of the form (expected_number_of_rows, avg_row_width (in bytes))
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
//        By default, the planner generates an (unparameterized) path, which
//        can be reasoned about like a SequentialScan, optionally filtered.
//        This method allows the implementor to declare other Paths,
//        corresponding to faster access methods for specific attributes.
//        Such a parameterized path can be reasoned about like an IndexScan.
//        For example, with the following query::
//            select * from foreign_table inner join local_table using(id);
//        where foreign_table is a foreign table containing 100000 rows, and
//        local_table is a regular table containing 100 rows.
//        The previous query would probably be transformed to a plan similar to
//        this one::
//            ┌────────────────────────────────────────────────────────────────────────────────────┐
//            │                                     QUERY PLAN                                     │
//            ├────────────────────────────────────────────────────────────────────────────────────┤
//            │ Hash Join  (cost=57.67..4021812.67 rows=615000 width=68)                           │
//            │   Hash Cond: (foreign_table.id = local_table.id)                                   │
//            │   ->  Foreign Scan on foreign_table (cost=20.00..4000000.00 rows=100000 width=40)  │
//            │   ->  Hash  (cost=22.30..22.30 rows=1230 width=36)                                 │
//            │         ->  Seq Scan on local_table (cost=0.00..22.30 rows=1230 width=36)          │
//            └────────────────────────────────────────────────────────────────────────────────────┘
//        But with a parameterized path declared on the id key, with the knowledge that this key
//        is unique on the foreign side, the following plan might get chosen::
//            ┌───────────────────────────────────────────────────────────────────────┐
//            │                              QUERY PLAN                               │
//            ├───────────────────────────────────────────────────────────────────────┤
//            │ Nested Loop  (cost=20.00..49234.60 rows=615000 width=68)              │
//            │   ->  Seq Scan on local_table (cost=0.00..22.30 rows=1230 width=36)   │
//            │   ->  Foreign Scan on remote_table (cost=20.00..40.00 rows=1 width=40)│
//            │         Filter: (id = local_table.id)                                 │
//            └───────────────────────────────────────────────────────────────────────┘
//        Returns:
//            A list of tuples of the form: (key_columns, expected_rows),
//            where key_columns is a tuple containing the columns on which
//            the path can be used, and expected_rows is the number of rows
//            this path might return for a simple lookup.
//            For example, the return value corresponding to the previous scenario would be::
//                [(('id',), 1)]
func (h *Hub) GetPathKeys(opts types.Options) ([]types.PathKey, error) {

	connectionName := opts["connection"]
	table := opts["table"]

	log.Printf("[TRACE] hub.GetPathKeys for connection '%s`, table `%s`", connectionName, table)

	// if this is an aggregate connection, get the first child connection
	if h.IsAggregatorConnection(connectionName) {
		connectionName = h.GetAggregateConnectionChild(connectionName)
		log.Printf("[TRACE] connection is an aggregate - using child connection: %s", connectionName)
	}

	// get the schema for this connection
	connectionPlugin, err := h.getConnectionPlugin(connectionName)
	if err != nil {
		return nil, err
	}
	schema := connectionPlugin.Schema.Schema[table]
	var allColumns = make([]string, len(schema.Columns))
	for i, c := range schema.Columns {
		allColumns[i] = c.Name
	}

	var pathKeys []types.PathKey

	// build path keys based on the table key columns
	// NOTE: the schema data has changed in SDK version 1.3 - we must handle plugins using legacy sdk explicitly
	// check for legacy sdk versions
	if schema.ListCallKeyColumns != nil {
		log.Printf("[TRACE] schema response include ListCallKeyColumns, it is using legacy protobuff interface ")
		pathKeys = types.LegacyKeyColumnsToPathKeys(schema.ListCallKeyColumns, schema.ListCallOptionalKeyColumns, allColumns)
	} else if schema.ListCallKeyColumnList != nil {
		log.Printf("[TRACE] schema response include ListCallKeyColumnList, it is using the updated protobuff interface ")
		// generate path keys if there are required list key columns
		// this increases the chances that Postgres will generate a plan which provides the quals when querying the table
		pathKeys = types.KeyColumnsToPathKeys(schema.ListCallKeyColumnList, allColumns)
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
//        Returns:
//            An iterable of strings to display in the EXPLAIN output.
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

// startScanForConnection starts a scan for a single connection, using a scanIterator
func (h *Hub) startScanForConnection(connectionName string, table string, qualMap map[string]*proto.Quals, columns []string, limit int64, scanTraceCtx *telemetry.TraceCtx) (_ Iterator, err error) {
	defer func() {
		if err != nil {
			// close the span in case of errir
			scanTraceCtx.Span.End()
		}
	}()

	connectionPlugin, err := h.getConnectionPlugin(connectionName)
	if err != nil {
		return nil, err
	}

	// determine whether to include the limit, based on the quals
	// we ONLY pushgdown the limit is all quals have corresponding key columns,
	// and if the qual operator is supported by the key column
	if limit != -1 && !h.shouldPushdownLimit(table, qualMap, connectionPlugin) {
		limit = -1
	}

	if len(qualMap) > 0 {
		log.Printf("[INFO] connection '%s', table '%s', quals %s", connectionName, table, grpc.QualMapToString(qualMap, true))
	} else {
		log.Println("[INFO] --------")
		log.Println("[INFO] no quals")
		log.Println("[INFO] --------")
	}

	// cache not enabled - create a scan iterator
	log.Printf("[TRACE] startScanForConnection creating a new scan iterator")
	queryContext := proto.NewQueryContext(columns, qualMap, limit)
	iterator := newScanIterator(h, connectionPlugin, table, qualMap, columns, limit, scanTraceCtx)

	if err := h.startScan(iterator, queryContext, scanTraceCtx); err != nil {
		return nil, err
	}

	return iterator, nil
}

// determine whether to include the limit, based on the quals
// we ONLY pushdown the limit is all quals have corresponding key columns,
// and if the qual operator is supported by the key column
func (h *Hub) shouldPushdownLimit(table string, qualMap map[string]*proto.Quals, connectionPlugin *steampipeconfig.ConnectionPlugin) bool {
	// build a map of all key columns
	tableSchema, ok := connectionPlugin.Schema.Schema[table]
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
			log.Printf("[INFO] shouldPushdownLimit no key column found for column %s. NOT pushing down limit", col)
			return false
		}
	}

	// all quals are supported - push down limit
	log.Printf("[INFO] shouldPushdownLimit all quals are supported - pushing down limit")
	return true
}

// split startScan into a separate function to allow iterator to restart the scan
func (h *Hub) startScan(iterator *scanIterator, queryContext *proto.QueryContext, traceCtx *telemetry.TraceCtx) error {
	// ensure we do not call execute too frequently
	h.throttle()

	table := iterator.table
	c := iterator.connection

	callId := grpc.BuildCallId(c.ConnectionName)

	req := &proto.ExecuteRequest{
		Table:        table,
		QueryContext: queryContext,
		Connection:   c.ConnectionName,
		CacheEnabled: h.cacheEnabled(c),
		CacheTtl:     int64(h.cacheTTL(c.ConnectionName).Seconds()),
		CallId:       callId,
		TraceContext: grpc.CreateCarrierFromContext(traceCtx.Ctx),
	}

	log.Printf("[INFO] StartScan for table: %s, callId %s, cache enabled: %v, iterator %p", table, callId, req.CacheEnabled, iterator)
	stream, ctx, cancel, err := c.PluginClient.Execute(req)
	// format GRPC errors and ignore not implemented errors for backwards compatibility
	err = grpc.HandleGrpcError(err, c.ConnectionName, "Execute")
	if err != nil {
		log.Printf("[WARN] startScan: plugin Execute function callId: %s returned error: %v\n", callId, err)
		iterator.setError(err)
		return err
	}
	iterator.Start(stream, ctx, cancel)
	return nil
}

// getConnectionPlugin returns the connectionPlugin for the provided connection
// it also makes sure that the plugin is up and running.
// if the plugin is not running, it attempts to restart the plugin - errors if unable
func (h *Hub) getConnectionPlugin(connectionName string) (*steampipeconfig.ConnectionPlugin, error) {
	log.Printf("[TRACE] hub.getConnectionPlugin for connection '%s`", connectionName)

	// get the plugin FQN
	connectionConfig, ok := h.steampipeConfig.Connections[connectionName]
	if !ok {
		return nil, fmt.Errorf("no connection config loaded for connection '%s'", connectionName)
	}
	pluginFQN := connectionConfig.Plugin

	// ask connection map to get or create this connection
	c, err := h.connections.getOrCreate(pluginFQN, connectionName)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// load the given plugin connection into the connection map and return the schema
func (h *Hub) createConnectionPlugin(pluginFQN, connectionName string) (*steampipeconfig.ConnectionPlugin, error) {
	// load the config for this connection
	connection, ok := h.steampipeConfig.Connections[connectionName]
	if !ok {
		log.Printf("[WARN] no config found for connection %s", connectionName)
		return nil, fmt.Errorf("no config found for connection %s", connectionName)
	}

	log.Printf("[TRACE] createConnectionPlugin plugin %s, connection %s, config: %s\n", pluginmanager.PluginFQNToSchemaName(pluginFQN), connectionName, connection.Config)

	connectionPlugins, res := steampipeconfig.CreateConnectionPlugins([]*modconfig.Connection{connection})
	if res.Error != nil {
		return nil, res.Error
	}
	if connectionPlugins[connection.Name] == nil {
		if len(res.Warnings) > 0 {
			return nil, fmt.Errorf("%s", strings.Join(res.Warnings, ","))
		}
		return nil, fmt.Errorf("unknown failure")
	}
	return connectionPlugins[connection.Name], nil
}

func (h *Hub) cacheEnabled(connection *steampipeconfig.ConnectionPlugin) bool {
	if h.overrideCacheEnabled != nil {
		res := *h.overrideCacheEnabled
		log.Printf("[TRACE] cacheEnabled overrideCacheEnabled %v", *h.overrideCacheEnabled)
		return res
	}
	// ask the steampipe config for resolved plugin options - this will use default values where needed
	connectionOptions := h.steampipeConfig.GetConnectionOptions(connection.ConnectionName)

	// the config loading code should ALWAYS populate the connection options, using defaults if needed
	if connectionOptions.Cache == nil {
		panic(fmt.Sprintf("No cache options found for connection %s", connection))
	}
	return *connectionOptions.Cache
}

func (h *Hub) cacheTTL(connectionName string) time.Duration {
	// ask the steampipe config for resolved plugin options - this will use default values where needed
	connectionOptions := h.steampipeConfig.GetConnectionOptions(connectionName)

	// the config loading code shouls ALWAYS populate the connection options, using defaults if needed
	if connectionOptions.CacheTTL == nil {
		panic(fmt.Sprintf("No cache options found for connection %s", connectionName))
	}

	ttl := time.Duration(*connectionOptions.CacheTTL) * time.Second

	// would this give data earlier than the cacheClearTime
	now := time.Now()
	if now.Add(-ttl).Before(h.cacheClearTime) {
		ttl = now.Sub(h.cacheClearTime)
	}
	return ttl
}

// IsAggregatorConnection returns  whether the connection with the given name is of type "aggregate"
func (h *Hub) IsAggregatorConnection(connectionName string) bool {
	connectionConfig, ok := h.steampipeConfig.Connections[connectionName]
	return ok && connectionConfig.Type == modconfig.ConnectionTypeAggregator
}

// GetAggregateConnectionChild returns the name of first child connection of the aggregate connection with the given name
func (h *Hub) GetAggregateConnectionChild(connectionName string) string {
	if !h.IsAggregatorConnection(connectionName) {
		panic(fmt.Sprintf("GetAggregateConnectionChild called for connection %s which is not an aggregate", connectionName))
	}
	aggregateConnection := h.steampipeConfig.Connections[connectionName]
	// get first child
	return aggregateConnection.FirstChild().Name
}

func (h *Hub) GetCommandSchema() map[string]*proto.TableSchema {
	return map[string]*proto.TableSchema{
		constants.CommandTableCache: {
			Columns: []*proto.ColumnDefinition{
				{Name: constants.CommandTableCacheOperationColumn, Type: proto.ColumnType_STRING},
			},
		},
		constants.CommandTableScanMetadata: {
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

func (h *Hub) HandleCacheCommand(command string) error {
	if err := h.ValidateCacheCommand(command); err != nil {
		return err
	}

	log.Printf("[TRACE] HandleCacheCommand %s", command)

	switch command {
	case constants.CommandCacheClear:
		log.Printf("[TRACE] commandCacheClear")
		// set the cache clear time for the remote query cache
		h.cacheClearTime = time.Now()
	case constants.CommandCacheOn:
		enabled := true
		h.overrideCacheEnabled = &enabled
		log.Printf("[TRACE] commandCacheOn, overrideCacheEnabled: %v", enabled)
	case constants.CommandCacheOff:
		enabled := false
		h.overrideCacheEnabled = &enabled
		log.Printf("[TRACE] commandCacheOff, overrideCacheEnabled: %v", enabled)
	}
	return nil
}

func (h *Hub) ValidateCacheCommand(command string) error {
	validCommands := []string{constants.CommandCacheClear, constants.CommandCacheOn, constants.CommandCacheOff}

	if !helpers.StringSliceContains(validCommands, command) {
		return fmt.Errorf("invalid command '%s' - supported commands are %s", command, strings.Join(validCommands, ","))
	}
	return nil
}

// ensure we do not call execute too frequently
// NOTE: this is a workaround for legacy plugin - it is not necessary for plugins built with sdk > 0.8.0
func (h *Hub) throttle() {
	minScanInterval := 10 * time.Millisecond
	h.timingLock.Lock()
	defer h.timingLock.Unlock()
	timeSince := time.Since(h.lastScanTime)
	if timeSince < minScanInterval {
		sleepTime := minScanInterval - timeSince
		time.Sleep(sleepTime)
	}
	h.lastScanTime = time.Now()
}

func (h *Hub) executeCommandScan(table string) (Iterator, error) {
	switch table {
	case constants.CommandTableScanMetadata:
		res := &QueryResult{
			Rows: make([]map[string]interface{}, len(h.scanMetadata)),
		}
		for i, m := range h.scanMetadata {
			res.Rows[i] = m.AsResultRow()
		}
		return newInMemoryIterator(constants.CommandSchema, res), nil
	default:
		return nil, fmt.Errorf("cannot select from command table '%s'", table)
	}
}
