package hub

import (
	"fmt"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/grpc"
	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/logging"
	"github.com/turbot/steampipe-postgres-fdw/hub/cache"
	"github.com/turbot/steampipe-postgres-fdw/types"
	"github.com/turbot/steampipe/constants"
	"github.com/turbot/steampipe/steampipeconfig"
)

const (
	rowBufferSize = 100
)

// Hub :: structure representing plugin hub
type Hub struct {
	connections     *connectionMap
	steampipeConfig *steampipeconfig.SteampipeConfig
	queryCache      *cache.QueryCache
}

// global hub instance
var hubSingleton *Hub

// mutex protecting creation
var hubMux sync.Mutex

//// lifecycle ////

// GetHub :: return hub singleton
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
	hub := &Hub{
		connections: newConnectionMap(),
	}

	// NOTE: steampipe determine it's install directory from the input arguments (with a default)
	// as we are using shared steampipe code we must set the install directory.
	// we can derive it from the working directory (which is underneath the install directectory)
	steampipeDir, err := getInstallDirectory()
	if err != nil {
		return nil, err
	}
	constants.SteampipeDir = steampipeDir

	if _, err := hub.LoadConnectionConfig(); err != nil {
		return nil, err
	}
	if err := hub.createCache(); err != nil {
		return nil, err
	}

	for connectionName, connectionConfig := range hub.steampipeConfig.Connections {
		log.Printf("[DEBUG] create connection %s, plugin %s", connectionName, connectionConfig.Plugin)
		input := &steampipeconfig.ConnectionPluginInput{
			PluginName:        steampipeconfig.PluginFQNToSchemaName(connectionConfig.Plugin),
			ConnectionName:    connectionName,
			ConnectionConfig:  connectionConfig.Config,
			ConnectionOptions: connectionConfig.Options}

		if _, err := hub.createConnectionPlugin(input); err != nil {
			return nil, err
		}
	}

	return hub, nil
}

func getInstallDirectory() (string, error) {
	// set the install folder - derive from our working folder
	// we need to do this as we are sharing steampipe code to read the config
	// and steampipe may set the install folder from a cmd line arg, so it cannot be hard coded
	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return path.Join(wd, "../../.."), nil

}

// shutdown all plugin clients
func (h *Hub) Close() {
	log.Println("[TRACE] hub: close")
	for _, connection := range h.connections.connectionPlugins {
		connection.Plugin.Client.Kill()
	}
}

//// public fdw functions ////

// GetSchema :: return the schema for a name. Load the plugin for the connection if needed
func (h *Hub) GetSchema(remoteSchema string, localSchema string) (*proto.Schema, error) {
	pluginFQN := remoteSchema
	connectionName := localSchema
	log.Printf("[TRACE] GetSchema remoteSchema: %s, name %s\n", remoteSchema, connectionName)

	c := h.connections.get(pluginFQN, connectionName)

	// if we do not have this ConnectionPlugin loaded, create
	if c == nil {
		log.Printf("[TRACE] hub: connection plugin is not loaded - loading\n")

		// load the config for this connection
		connection, ok := h.steampipeConfig.Connections[connectionName]
		if !ok {
			return nil, fmt.Errorf("no config found for connection %s", connectionName)
		}

		var err error
		input := &steampipeconfig.ConnectionPluginInput{
			PluginName:        pluginFQN,
			ConnectionName:    connectionName,
			ConnectionConfig:  connection.Config,
			ConnectionOptions: connection.Options}

		c, err = h.createConnectionPlugin(input)
		if err != nil {
			return nil, err
		}
	}

	return c.Schema, nil
}

// send the locally cached connection config to the plugin
func (h *Hub) SetConnectionConfig(remoteSchema string, localSchema string) error {
	pluginFQN := remoteSchema
	connectionName := localSchema
	log.Printf("[DEBUG] GetSchema remoteSchema: %s, name %s\n", remoteSchema, connectionName)

	c := h.connections.get(pluginFQN, connectionName)

	// if we do not have this ConnectionPlugin loaded, create
	if c == nil {
		log.Printf("[TRACE] connection plugin is not loaded - loading\n")

		// load the config for this connection
		config, ok := h.steampipeConfig.Connections[connectionName]
		if !ok {
			return fmt.Errorf("no config found for connection %s", connectionName)
		}

		input := &steampipeconfig.ConnectionPluginInput{
			PluginName:        pluginFQN,
			ConnectionName:    connectionName,
			ConnectionConfig:  config.Config,
			ConnectionOptions: config.Options}

		var err error
		c, err = h.createConnectionPlugin(input)
		if err != nil {
			return err
		}
	}
	_, err := c.Plugin.Stub.SetConnectionConfig(&proto.SetConnectionConfigRequest{
		ConnectionName:   connectionName,
		ConnectionConfig: c.ConnectionConfig,
	})
	// format GRPC errors and ignore not implemented errors for backwards compatibility
	return steampipeconfig.HandleGrpcError(err, connectionName, "GetSchema")
}

// Scan :: Start a table scan. Returns an iterator
func (h *Hub) Scan(columns []string, quals []*proto.Qual, opts types.Options) (Iterator, error) {
	logging.LogTime("Scan start")

	qualMap, err := h.buildQualMap(quals)
	connectionName := opts["connection"]
	table := opts["table"]

	cacheEnabled := h.cacheEnabled(connectionName)
	cacheTTL := h.cacheTTL(connectionName)
	var cacheString = "caching DISABLED"
	if cacheEnabled {
		cacheString = fmt.Sprintf("caching ENABLED with TTL %d seconds", int(cacheTTL.Seconds()))
	}
	log.Printf("[INFO] executing query for connection %s, %s", connectionName, cacheString)

	if len(qualMap) > 0 {
		log.Printf("[INFO] connection '%s', table '%s', quals %s", connectionName, table, grpc.QualMapToString(qualMap))
	} else {
		log.Println("[INFO] no quals")
	}

	// do we have a cached query result
	if cacheEnabled {
		cachedResult := h.queryCache.Get(connectionName, table, qualMap, columns)
		if cachedResult != nil {
			// we have cache data - return a cache iterator
			return newCacheIterator(cachedResult), nil
		}
	}

	iterator := newScanIterator(h, connectionName, table, qualMap, columns)
	err = h.startScan(iterator, columns, qualMap)
	logging.LogTime("Scan end")
	return iterator, err
}

// GetRelSize ::  Method called from the planner to estimate the resulting relation size for a scan.
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

// GetPathKeys ::  Method called from the planner to add additional Path to the planner.
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

	// get the schema for this connection
	connectionPlugin, err := h.connections.getConnectionPluginForTable(table, connectionName)
	if err != nil {
		return nil, err
	}
	schema := connectionPlugin.Schema.Schema[table]

	// generate path keys if there are required list key columns
	// this increases the chances that Postgres will generate a plan which provides the quals when querying the table
	var pathKeys []types.PathKey
	if listKeyColumns := schema.ListCallKeyColumns; listKeyColumns != nil {
		pathKeys = types.KeyColumnsToPathKeys(listKeyColumns)
	}
	// NOTE: in the future we may (optionally) add in path keys for Get call key caolumns.
	// We do not do this by default as it is likely to actually reduce join performance in the general case,
	// particularly when caching is taken into account

	//var getCallPathKeys []types.PathKey
	//if getKeyColumns := schema.GetCallKeyColumns; getKeyColumns != nil {
	//	getCallPathKeys = types.KeyColumnsToPathKeys(getKeyColumns)
	//}
	//pathKeys := types.MergePathKeys(getCallPathKeys, listCallPathKeys)

	log.Printf("[INFO] GetPathKeys for connection '%s`, table `%s` returning %v", connectionName, table, pathKeys)
	return pathKeys, nil
}

// Explain ::  hook called on explain.
//        Returns:
//            An iterable of strings to display in the EXPLAIN output.
func (h *Hub) Explain(columns []string, quals []*proto.Qual, sortKeys []string, verbose bool, opts types.Options) ([]string, error) {
	return make([]string, 0), nil
}

//// internal implementation ////

// split startScan into a separate function to allow iterator to restart the scan
func (h *Hub) startScan(iterator *scanIterator, columns []string, qualMap map[string]*proto.Quals) error {
	table := iterator.table
	connectionName := iterator.connectionName

	log.Printf("[INFO] StartScan\n  table: %s\n  columns: %v\n", table, columns)
	// get ConnectionPlugin which serves this table
	c, err := h.connections.getConnectionPluginForTable(table, connectionName)
	if err != nil {
		return err
	}
	var queryContext = &proto.QueryContext{
		Columns: columns,
		Quals:   qualMap,
	}

	// if a scanIterator is in progress, fail
	if iterator.inProgress() {
		return fmt.Errorf("cannot start scanIterator while existing scanIterator is incomplete - cancel first")
	}

	req := &proto.ExecuteRequest{
		Table:        table,
		QueryContext: queryContext,
		Connection:   connectionName,
	}
	stream, err := c.Plugin.Stub.Execute(req)
	if err != nil {
		log.Printf("[WARN] startScan: plugin Execute function returned error: %v\n", err)
		// format GRPC errors and ignore not implemented errors for backwards compatibility
		err = steampipeconfig.HandleGrpcError(err, connectionName, "Execute")
		iterator.setError(err)
		return err
	}
	iterator.start(stream)
	return nil
}

// load the given plugin connection into the connection map and return the schema
func (h *Hub) createConnectionPlugin(input *steampipeconfig.ConnectionPluginInput) (*steampipeconfig.ConnectionPlugin, error) {
	log.Printf("[TRACE] createConnectionPlugin plugin %s, conection %s, config: %s\n", input.PluginName, input.ConnectionName, input.ConnectionConfig)

	c, err := steampipeconfig.CreateConnectionPlugin(input)
	if err != nil {
		return nil, err
	}
	if err = h.connections.add(c); err != nil {
		return nil, err
	}
	return c, nil
}

// LoadConnectionConfig :: load the connection config and return whether it has changed
func (h *Hub) LoadConnectionConfig() (bool, error) {
	// TODO currently we need to pass a workspace path to LoadSteampipeConfig - pass empty string for now
	// refactor to allow us to load only connection config and not workspace options
	connectionConfig, err := steampipeconfig.LoadSteampipeConfig("")
	if err != nil {
		log.Printf("[WARN] LoadConnectionConfig failed %v ", err)
		return false, err
	}

	configChanged := h.steampipeConfig == connectionConfig
	h.steampipeConfig = connectionConfig

	return configChanged, nil
}

// create the query cache
func (h *Hub) createCache() error {
	queryCache, err := cache.NewQueryCache()
	if err != nil {
		return err
	}
	h.queryCache = queryCache

	return nil
}

func (h *Hub) cacheEnabled(connectionName string) bool {
	// ask the steampipe config for resolved plugin options - this will use default values where needed
	connectionOptions := h.steampipeConfig.GetConnectionOptions(connectionName)

	// the config loading code shouls ALWAYS populate the connection options, using defaults if needed
	if connectionOptions.Cache == nil {
		panic(fmt.Sprintf("No cache options found for connection %s", connectionName))
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

	return time.Duration(*connectionOptions.CacheTTL) * time.Second
}
