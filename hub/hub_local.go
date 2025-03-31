package hub

import (
	"log"
	"os"
	"time"

	typehelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/pipe-fittings/v2/ociinstaller"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/logging"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
	"github.com/turbot/steampipe-postgres-fdw/settings"
	"github.com/turbot/steampipe-postgres-fdw/types"
	"github.com/turbot/steampipe/pkg/constants"
	"golang.org/x/exp/maps"
)

type HubLocal struct {
	hubBase
	plugin      *grpc.PluginServer
	pluginName  string
	pluginAlias string
	connections map[string]*proto.ConnectionConfig
}

func newLocalHub() (*HubLocal, error) {
	imageRef := ociinstaller.NewImageRef(pluginAlias).DisplayImageRef()

	hub := &HubLocal{
		hubBase: newHubBase(false),
		plugin: plugin.Server(&plugin.ServeOpts{
			PluginFunc: getPluginFunc(),
		}),
		pluginName:  imageRef,
		pluginAlias: pluginAlias,
		connections: make(map[string]*proto.ConnectionConfig),
	}
	hub.cacheSettings = settings.NewCacheSettings(hub.clearConnectionCache, hub.getServerCacheEnabled())

	// TODO CHECK TELEMETRY ENABLED?
	if err := hub.initialiseTelemetry(); err != nil {
		return nil, err
	}

	return hub, nil
}

func (l *HubLocal) SetConnectionConfig(connectionName, configString string) error {
	log.Printf("[INFO] HubLocal SetConnectionConfig: connection: %s, config: %s", connectionName, configString)

	l.connections[connectionName] =
		&proto.ConnectionConfig{
			Connection:      connectionName,
			Plugin:          l.pluginName,
			PluginShortName: l.pluginAlias,
			Config:          configString,
			PluginInstance:  l.pluginName,
		}

	_, err := l.plugin.SetAllConnectionConfigs(&proto.SetAllConnectionConfigsRequest{
		Configs: maps.Values(l.connections),
	})
	return err
}

func (l *HubLocal) UpdateConnectionConfig(connectionName, configString string) error {
	log.Printf("[INFO] HubLocal UpdateConnectionConfig: connection: %s, config: %s", connectionName, configString)

	// if the connection already exists and the config is the same, do nothing
	// this situation could arise when a session is restarted, the server options are loaded again, and
	// the connection config is set again which results in the cache getting cleared
	if conn, ok := l.connections[connectionName]; ok && conn.Config == configString {
		return nil
	}

	l.connections[connectionName] =
		&proto.ConnectionConfig{
			Connection:      connectionName,
			Plugin:          l.pluginName,
			PluginShortName: l.pluginAlias,
			Config:          configString,
			PluginInstance:  l.pluginName,
		}

	_, err := l.plugin.UpdateConnectionConfigs(&proto.UpdateConnectionConfigsRequest{
		Changed: []*proto.ConnectionConfig{l.connections[connectionName]},
	})
	return err
}

func (l *HubLocal) LoadConnectionConfig() (bool, error) {
	// do nothing
	return false, nil
}

func (l *HubLocal) GetSchema(_, connectionName string) (*proto.Schema, error) {
	log.Printf("[INFO] GetSchema")
	res, err := l.plugin.GetSchema(&proto.GetSchemaRequest{Connection: connectionName})

	if err != nil {
		log.Printf("[INFO] GetSchema retry")
		// TODO tactical - if no connection config has been set for this connection, set now
		if err := l.SetConnectionConfig(connectionName, ""); err != nil {

			return nil, err
		}
		res, err = l.plugin.GetSchema(&proto.GetSchemaRequest{Connection: connectionName})
		if err != nil {
			log.Printf("[INFO] GetSchema retry failed")
			return nil, err
		}
	}
	return res.GetSchema(), nil
}

func (l *HubLocal) GetIterator(columns []string, quals *proto.Quals, unhandledRestrictions int, limit int64, sortOrder []*proto.SortColumn, queryTimestamp int64, opts types.Options) (Iterator, error) {
	logging.LogTime("GetIterator start")
	qualMap, err := buildQualMap(quals)
	connectionName := opts["connection"]
	table := opts["table"]
	log.Printf("[TRACE] RemoteHub GetIterator() table '%s'", table)

	if connectionName == constants.InternalSchema || connectionName == constants.LegacyCommandSchema {
		return l.executeCommandScan(connectionName, table, queryTimestamp)
	}

	// create a span for this scan
	scanTraceCtx := l.traceContextForScan(table, columns, limit, qualMap, connectionName)
	iterator, err := l.startScanForConnection(connectionName, table, qualMap, unhandledRestrictions, columns, limit, sortOrder, queryTimestamp, scanTraceCtx)

	if err != nil {
		log.Printf("[TRACE] RemoteHub GetIterator() failed :( %s", err)
		return nil, err
	}
	log.Printf("[TRACE] RemoteHub GetIterator() created iterator (%p)", iterator)

	return iterator, nil
}

func (l *HubLocal) GetPathKeys(opts types.Options) ([]types.PathKey, error) {
	connectionName := opts["connection"]

	connectionSchema, err := l.GetSchema("", connectionName)
	if err != nil {
		return nil, err
	}

	return l.getPathKeys(connectionSchema, opts)

}

func (l *HubLocal) GetConnectionConfigByName(name string) *proto.ConnectionConfig {
	return l.connections[name]
}

func (l *HubLocal) ProcessImportForeignSchemaOptions(opts types.Options, connection string) error {
	// NOTE: if no connection config is passed, set an empty connection config
	config, _ := opts["config"]

	// do we already have this connection
	connectionConfig, ok := l.connections[connection]
	if ok {
		log.Println("[INFO] connection already exists, updating ")
		// we have already set the config - update it
		connectionConfig.Config = config
		return l.UpdateConnectionConfig(connection, config)
	}

	// we have not yet set the config - set it
	return l.SetConnectionConfig(connection, config)
}

// startScanForConnection starts a scan for a single connection, using a scanIterator or a legacyScanIterator
func (l *HubLocal) startScanForConnection(connectionName string, table string, qualMap map[string]*proto.Quals, unhandledRestrictions int, columns []string, limit int64, sortOrder []*proto.SortColumn, queryTimestamp int64, scanTraceCtx *telemetry.TraceCtx) (_ Iterator, err error) {
	defer func() {
		if err != nil {
			// close the span in case of errir
			scanTraceCtx.Span.End()
		}
	}()

	// ok so this is a multi connection plugin, build list of connections,
	// if this connection is NOT an aggregator, only execute for the named connection

	//// get connection config
	//connectionConfig, ok := l.getConnectionconfig(ConnectionName)
	//if !ok {
	//	return nil, fmt.Errorf("no connection config loaded for connection '%s'", ConnectionName)
	//}

	// determine whether to pushdown the limit
	connectionLimitMap, err := l.buildConnectionLimitMap(connectionName, table, qualMap, unhandledRestrictions, limit)
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
	iterator := newScanIteratorLocal(l, connectionName, table, l.pluginName, connectionLimitMap, qualMap, columns, limit, sortOrder, queryTimestamp, scanTraceCtx)
	return iterator, nil
}

func (l *HubLocal) buildConnectionLimitMap(connection, table string, qualMap map[string]*proto.Quals, unhandledRestrictions int, limit int64) (map[string]int64, error) {
	connectionSchema, err := l.GetSchema("", connection)
	if err != nil {
		return nil, err
	}
	schemaMode := connectionSchema.Mode

	// pushing the limit down or not is dependent on the schema.
	// for a static schema, the limit will be the same for all connections (i.e. we either pushdown for all or none)
	// check once whether we should push down
	if limit != -1 && schemaMode == plugin.SchemaModeStatic {
		log.Printf("[TRACE] static schema - using same limit for all connections")
		if !l.shouldPushdownLimit(table, qualMap, unhandledRestrictions, connectionSchema) {
			limit = -1
		}
	}

	// set the limit for the one and only connection
	var connectionLimitMap = make(map[string]int64)
	connectionLimit := limit
	// if schema mode is dynamic, check whether we should push down for each connection
	if schemaMode == plugin.SchemaModeDynamic && !l.shouldPushdownLimit(table, qualMap, unhandledRestrictions, connectionSchema) {
		log.Printf("[INFO] not pushing limit down for connection %s", connection)
		connectionLimit = -1
	}
	connectionLimitMap[connection] = connectionLimit

	return connectionLimitMap, nil
}

func (l *HubLocal) clearConnectionCache(connection string) error {

	_, err := l.plugin.SetConnectionCacheOptions(&proto.SetConnectionCacheOptionsRequest{ClearCacheForConnection: connection})
	if err != nil {
		log.Printf("[WARN] clearConnectionCache failed for connection %s: SetConnectionCacheOptions returned %s", connection, err)
	}
	log.Printf("[INFO] clear connection cache succeeded")
	return err
}

func (l *HubLocal) cacheEnabled(s string) bool {
	// if the caching is disabled for the server, just return false
	if !l.cacheSettings.ServerCacheEnabled {
		return false
	}

	if l.cacheSettings.ClientCacheEnabled != nil {
		return *l.cacheSettings.ClientCacheEnabled
	}

	if envStr, ok := os.LookupEnv(constants.EnvCacheEnabled); ok {
		// set this so that we don't keep looking up the env var
		l.cacheSettings.SetEnabled(envStr)
		return l.cacheEnabled(s)
	}
	return true
}

func (l *HubLocal) cacheTTL(s string) time.Duration {
	log.Printf("[INFO] cacheTTL 1")
	// if the cache ttl has been overridden, then enforce the value
	if l.cacheSettings.Ttl != nil {
		return *l.cacheSettings.Ttl
	}
	if envStr, ok := os.LookupEnv(constants.EnvCacheMaxTTL); ok {
		// set this so that we don't keep looking up the env var
		l.cacheSettings.SetTtl(envStr)
		return l.cacheTTL(s)
	}
	return 10 * time.Hour
}

// resolve the server cache enabled property
func (l *HubLocal) getServerCacheEnabled() bool {
	var res = true
	if val, ok := os.LookupEnv(constants.EnvCacheEnabled); ok {
		if boolVal, err := typehelpers.ToBool(val); err == nil {
			res = boolVal
		}
	}

	log.Printf("[INFO] Hub.getServerCacheEnabled returning %v", res)

	return res
}
