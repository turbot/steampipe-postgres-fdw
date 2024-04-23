package hub

import (
	"context"
	"fmt"
	"github.com/turbot/steampipe/pkg/query/queryresult"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
	"github.com/turbot/steampipe-postgres-fdw/settings"
	"github.com/turbot/steampipe-postgres-fdw/types"
	"github.com/turbot/steampipe/pkg/constants"
	"github.com/turbot/steampipe/pkg/steampipeconfig"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type hubBase struct {
	runningIterators     map[Iterator]struct{}
	runningIteratorsLock sync.RWMutex

	// cacheSettings
	cacheSettings *settings.HubCacheSettings

	// telemetry properties
	// callback function to shutdown telemetry
	telemetryShutdownFunc func()
	hydrateCallsCounter   metric.Int64Counter
	queryTiming           *queryTimingMetadata
}

func newHubBase(enableScanMetadata bool) hubBase {
	h := hubBase{
		runningIterators: make(map[Iterator]struct{}),
	}
	if enableScanMetadata {
		h.queryTiming = newQueryTimingMetadata()
	}
	return h
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
func (h *hubBase) GetRelSize(columns []string, quals []*proto.Qual, opts types.Options) (types.RelSize, error) {
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
func (h *hubBase) getPathKeys(connectionSchema *proto.Schema, opts types.Options) ([]types.PathKey, error) {
	connectionName := opts["connection"]
	table := opts["table"]

	log.Printf("[TRACE] hub.GetPathKeys for connection '%s`, table `%s`", connectionName, table)
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
func (h *hubBase) Explain(columns []string, quals []*proto.Qual, sortKeys []string, verbose bool, opts types.Options) ([]string, error) {
	return make([]string, 0), nil
}

// StartScan starts a scan
func (h *hubBase) StartScan(i Iterator) error {
	log.Printf("[INFO] hubBase StartScan")

	// if iterator is not a pluginIterator, do nothing
	// (i.e. is it an InMemoryIterator
	// This code should never be called for them anyway as they are initialized to be in a `started` state,
	// and the scan is started using the function executeCommandScan)
	iterator, ok := i.(pluginIterator)
	if !ok {
		// unexpected
		log.Printf("[WARN] StartScan called for non-pluginIterator %T", i)
		return nil
	}

	// ask the iterator for the executor interface
	// (the iterator just returns itself - we need to do it this way because of the way nested structs work )
	iterator.Start(i.(pluginExecutor))

	// add iterator to running list
	h.addIterator(iterator)

	return nil
}

// EndScan is called when Postgres terminates the scan (because it has received enough rows of data)
func (h *hubBase) EndScan(iter Iterator, limit int64) {
	// is the iterator still running? If so it means postgres is stopping a scan before all rows have been read
	if iter.Status() == QueryStatusStarted {
		log.Printf("[INFO] ending scan before iterator complete - limit: %v, iterator: %p", limit, iter)
		// we normally add the scan metadata when the scan completes - if the scan has not completed, we need to
		// add the metadata here
		h.AddScanMetadata(iter)
		iter.Close()
	}

	h.RemoveIterator(iter)
}

// AddScanMetadata adds the scan metadata from the given iterator to the hubs array
// we append to this every time a scan completes (either due to end of data, or Postgres terminating)
// the full array is returned whenever a pop_scan_metadata command is received and the array is cleared
func (h *hubBase) AddScanMetadata(i Iterator) {
	// for local hub we do not store scan metadata
	if h.queryTiming == nil {
		return
	}
	// if iterator is not a pluginIterator, do nothing
	iter, ok := i.(pluginIterator)
	if !ok {
		return
	}

	queryTimestamp := iter.GetQueryTimestamp()

	log.Printf("[INFO] AddScanMetadata for iterator %p query timestamp %d (%s)", iter, queryTimestamp, iter.GetConnectionName())

	h.queryTiming.scanMetadataLock.Lock()
	defer h.queryTiming.scanMetadataLock.Unlock()

	ctx := iter.GetTraceContext().Ctx

	connectionName := iter.GetConnectionName()
	pluginName := iter.GetPluginName()

	// get scan metadata from iterator
	scanMetadata := iter.GetScanMetadata()
	querySummary := h.queryTiming.queryRowSummary[queryTimestamp]
	if querySummary == nil {
		querySummary = queryresult.NewQueryRowSummary()
	}

	// ensure we only keep scan metadata for this query, and limit the amount of metadata we keep
	h.queryTiming.removeStaleScanMetadata(queryTimestamp)

	for _, m := range scanMetadata {
		// update summary
		querySummary.Update(m)

		// add the scan metadata to the list
		// (if we have exceeded the max number of scan metadata items, this will keep the slowest items
		h.queryTiming.addScanMetadata(queryTimestamp, m)

		// hydrate metric labels
		labels := []attribute.KeyValue{
			attribute.String("table", m.Table),
			attribute.String("connection", connectionName),
			attribute.String("plugin", pluginName),
		}
		h.hydrateCallsCounter.Add(ctx, m.HydrateCalls, metric.WithAttributes(labels...))
	}
	// write the scan metadata and summary back to the hub
	h.queryTiming.queryRowSummary[queryTimestamp] = querySummary
}

// Close shuts down all plugin clients
func (h *hubBase) Close() {
	log.Println("[TRACE] hub: close")

	if h.telemetryShutdownFunc != nil {
		log.Println("[TRACE] shutdown telemetry")
		h.telemetryShutdownFunc()
	}
}

// Abort shuts down currently running queries
func (h *hubBase) Abort() {
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
	h.runningIterators = make(map[Iterator]struct{})
}

// settings

func (h *hubBase) ApplySetting(key string, value string) error {
	log.Printf("[TRACE] ApplySetting [%s => %s]", key, value)
	return h.cacheSettings.Apply(key, value)
}

func (h *hubBase) GetSettingsSchema() map[string]*proto.TableSchema {
	return map[string]*proto.TableSchema{
		constants.ForeignTableSettings: {
			Columns: []*proto.ColumnDefinition{
				{Name: constants.ForeignTableSettingsKeyColumn, Type: proto.ColumnType_STRING},
				{Name: constants.ForeignTableSettingsValueColumn, Type: proto.ColumnType_STRING},
			},
		},
		constants.ForeignTableScanMetadata: {
			Columns: []*proto.ColumnDefinition{
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
		constants.ForeignTableScanMetadataSummary: {
			Columns: []*proto.ColumnDefinition{
				{Name: "cached_rows_fetched", Type: proto.ColumnType_INT},
				{Name: "uncached_rows_fetched", Type: proto.ColumnType_INT},
				{Name: "hydrate_calls", Type: proto.ColumnType_INT},
				{Name: "duration_ms", Type: proto.ColumnType_INT},
				{Name: "scan_count", Type: proto.ColumnType_INT},
				{Name: "connection_count", Type: proto.ColumnType_INT},
			},
		},
	}
}

func (h *hubBase) ValidateCacheCommand(command string) error {
	validCommands := []string{constants.LegacyCommandCacheClear, constants.LegacyCommandCacheOn, constants.LegacyCommandCacheOff}

	if !helpers.StringSliceContains(validCommands, command) {
		return fmt.Errorf("invalid command '%s' - supported commands are %s", command, strings.Join(validCommands, ","))
	}
	return nil
}

func (h *hubBase) GetConnectionConfigByName(string) *proto.ConnectionConfig {
	// do nothing- only implemented in standalone
	return nil
}

func (h *hubBase) ProcessImportForeignSchemaOptions(types.Options, string) error {
	// do nothing- only implemented in standalone
	return nil
}

func (h *hubBase) executeCommandScan(connectionName, table string, queryTimestamp int64) (Iterator, error) {
	switch table {
	case constants.ForeignTableScanMetadataSummary:
		// we expect to only have metadata for  one query at a time - this is enforced by the metadata writing code
		if summaryCount := len(h.queryTiming.queryRowSummary); summaryCount > 1 {
			return nil, fmt.Errorf(" executeCommandScan for table '%s' - %d summaries in metadata store - there should only be 1. ", table, summaryCount)
		}

		res := &QueryResult{}
		for _, summary := range h.queryTiming.queryRowSummary {
			res.Rows = append(res.Rows, summary.AsResultRow())
		}
		// now we have read the summary, we can clear the cached data
		h.queryTiming.clearSummary()

		return newInMemoryIterator(connectionName, res, queryTimestamp), nil
	case constants.ForeignTableScanMetadata, constants.LegacyCommandTableScanMetadata:
		if metadataCount := len(h.queryTiming.scanMetadata); metadataCount > 1 {
			return nil, fmt.Errorf(" executeCommandScan for table '%s' - %d summaries in metadata store - there should only be 1. ", metadataCount, table)
		}

		res := &QueryResult{}
		for _, scansForQuery := range h.queryTiming.scanMetadata {
			for _, m := range scansForQuery {
				res.Rows = append(res.Rows, m.AsResultRow())
			}
		}
		// now we have read the scan metadata, we can clear the cached data
		h.queryTiming.clearScanMetadata()

		return newInMemoryIterator(connectionName, res, queryTimestamp), nil
	default:
		return nil, fmt.Errorf("cannot select from command table '%s'", table)
	}
}

func (h *hubBase) traceContextForScan(table string, columns []string, limit int64, qualMap map[string]*proto.Quals, connectionName string) *telemetry.TraceCtx {
	ctx, span := telemetry.StartSpan(context.Background(), constants.FdwName, "RemoteHub.Scan (%s)", table)
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

// determine whether to include the limit, based on the quals
// we ONLY pushdown the limit if all quals have corresponding key columns,
// and if the qual operator is supported by the key column
func (h *hubBase) shouldPushdownLimit(table string, qualMap map[string]*proto.Quals, unhandledRestrictions int, connectionSchema *proto.Schema) bool {
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

func (h *hubBase) initialiseTelemetry() error {
	log.Printf("[TRACE] init telemetry")
	shutdownTelemetry, err := telemetry.Init(constants.FdwName)
	if err != nil {
		return fmt.Errorf("failed to initialise telemetry: %s", err.Error())
	}

	h.telemetryShutdownFunc = shutdownTelemetry

	hydrateCalls, err := otel.GetMeterProvider().Meter(constants.FdwName).Int64Counter(
		fmt.Sprintf("%s/hydrate_calls_total", constants.FdwName),
		metric.WithDescription("The total number of hydrate calls"),
	)
	if err != nil {
		log.Printf("[WARN] init telemetry failed to create hydrateCallsCounter")
		return err
	}
	h.hydrateCallsCounter = hydrateCalls
	return nil
}

func (h *hubBase) addIterator(iterator Iterator) {
	h.runningIteratorsLock.Lock()
	defer h.runningIteratorsLock.Unlock()

	h.runningIterators[iterator] = struct{}{}
}

// RemoveIterator removes an iterator from list of running iterators
func (h *hubBase) RemoveIterator(iterator Iterator) {
	h.runningIteratorsLock.Lock()
	defer h.runningIteratorsLock.Unlock()

	delete(h.runningIterators, iterator)
}

func (h *hubBase) GetLegacySettingsSchema() map[string]*proto.TableSchema {
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

func (h *hubBase) HandleLegacyCacheCommand(command string) error {
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

func (h *hubBase) cacheTTL(connectionName string) time.Duration {
	log.Printf("[INFO] cacheTTL 1")
	// if the cache ttl has been overridden, then enforce the value
	if h.cacheSettings.Ttl != nil {
		return *h.cacheSettings.Ttl
	}
	log.Printf("[INFO] cacheTTL 2")

	// ask the steampipe config for resolved plugin options - this will use default values where needed
	connectionOptions := steampipeconfig.GlobalConfig.GetConnectionOptions(connectionName)
	log.Printf("[INFO] cacheTTL 3")

	// the config loading code should ALWAYS populate the connection options, using defaults if needed
	if connectionOptions.CacheTTL == nil {
		panic(fmt.Sprintf("No cache options found for connection %s", connectionName))
	}
	log.Printf("[INFO] cacheTTL 4")

	ttl := time.Duration(*connectionOptions.CacheTTL) * time.Second

	// would this give data earlier than the cacheClearTime
	now := time.Now()
	if now.Add(-ttl).Before(h.cacheSettings.ClearTime) {
		ttl = now.Sub(h.cacheSettings.ClearTime)
	}
	log.Printf("[INFO] cacheTTL 5")
	return ttl
}

// GetSortableFields
func (h *hubBase) GetSortableFields(tableName, connectionName string) map[string]proto.SortOrder {
	return nil
}
