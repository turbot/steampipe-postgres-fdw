package hub

import (
	typehelpers "github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe/pkg/steampipeconfig"
	"log"
	"os"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc"
	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/logging"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/telemetry"
	"github.com/turbot/steampipe-postgres-fdw/settings"
	"github.com/turbot/steampipe-postgres-fdw/types"
	"github.com/turbot/steampipe/pkg/constants"
	"github.com/turbot/steampipe/pkg/ociinstaller"
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
	imageRef := ociinstaller.NewSteampipeImageRef(pluginAlias).DisplayImageRef()

	hub := &HubLocal{
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

func (h *HubLocal) SetConnectionConfig(connectionName, configString string) error {
	log.Printf("[INFO] HubLocal SetConnectionConfig: connection: %s, config: %s", connectionName, configString)

	h.connections[connectionName] =
		&proto.ConnectionConfig{
			Connection:      connectionName,
			Plugin:          h.pluginName,
			PluginShortName: h.pluginAlias,
			Config:          configString,
			PluginInstance:  h.pluginName,
		}

	_, err := h.plugin.SetAllConnectionConfigs(&proto.SetAllConnectionConfigsRequest{
		Configs: maps.Values(h.connections),
	})
	return err
}

func (h *HubLocal) UpdateConnectionConfig(connectionName, configString string) error {
	log.Printf("[INFO] HubLocal UpdateConnectionConfig: connection: %s, config: %s", connectionName, configString)
	h.connections[connectionName] =
		&proto.ConnectionConfig{
			Connection:      connectionName,
			Plugin:          h.pluginName,
			PluginShortName: h.pluginAlias,
			Config:          configString,
			PluginInstance:  h.pluginName,
		}

	_, err := h.plugin.UpdateConnectionConfigs(&proto.UpdateConnectionConfigsRequest{
		Changed: []*proto.ConnectionConfig{h.connections[connectionName]},
	})
	return err
}

func (h *HubLocal) LoadConnectionConfig() (bool, error) {
	// do nothing
	return false, nil
}

func (h *HubLocal) GetSchema(_, connectionName string) (*proto.Schema, error) {
	log.Printf("[INFO] GetSchema")
	res, err := h.plugin.GetSchema(&proto.GetSchemaRequest{Connection: connectionName})

	if err != nil {
		log.Printf("[INFO] GetSchema retry")
		// TODO tactical - if no connection config has been set for this connection, set now
		if err := h.SetConnectionConfig(connectionName, ""); err != nil {

			return nil, err
		}
		res, err = h.plugin.GetSchema(&proto.GetSchemaRequest{Connection: connectionName})
		if err != nil {
			log.Printf("[INFO] GetSchema retry failed")
			return nil, err
		}
	}
	return res.GetSchema(), nil
}

func (h *HubLocal) GetIterator(columns []string, quals *proto.Quals, unhandledRestrictions int, limit int64, opts types.Options) (Iterator, error) {
	logging.LogTime("GetIterator start")
	qualMap, err := buildQualMap(quals)
	connectionName := opts["connection"]
	table := opts["table"]
	log.Printf("[TRACE] RemoteHub GetIterator() table '%s'", table)

	if connectionName == constants.InternalSchema || connectionName == constants.LegacyCommandSchema {
		return h.executeCommandScan(connectionName, table)
	}

	// create a span for this scan
	scanTraceCtx := h.traceContextForScan(table, columns, limit, qualMap, connectionName)
	iterator, err := h.startScanForConnection(connectionName, table, qualMap, unhandledRestrictions, columns, limit, scanTraceCtx)

	if err != nil {
		log.Printf("[TRACE] RemoteHub GetIterator() failed :( %s", err)
		return nil, err
	}
	log.Printf("[TRACE] RemoteHub GetIterator() created iterator (%p)", iterator)

	return iterator, nil
}

func (h *HubLocal) GetPathKeys(opts types.Options) ([]types.PathKey, error) {
	connectionName := opts["connection"]

	connectionSchema, err := h.GetSchema("", connectionName)
	if err != nil {
		return nil, err
	}

	return h.getPathKeys(connectionSchema, opts)

}

func (h *HubLocal) GetConnectionConfigByName(name string) *proto.ConnectionConfig {
	return h.connections[name]
}

func (h *HubLocal) ProcessImportForeignSchemaOptions(opts types.Options, connection string) error {
	// NOTE: if no connection config is passed, set an empty connection config
	config, _ := opts["config"]

	// do we already have this connection
	connectionConfig, ok := h.connections[connection]
	if ok {
		// we have already set the config - update it
		connectionConfig.Config = config
		return h.UpdateConnectionConfig(connection, config)
	}

	// we have not yet set the config - set it
	return h.SetConnectionConfig(connection, config)
}

// startScanForConnection starts a scan for a single connection, using a scanIterator or a legacyScanIterator
func (h *HubLocal) startScanForConnection(connectionName string, table string, qualMap map[string]*proto.Quals, unhandledRestrictions int, columns []string, limit int64, scanTraceCtx *telemetry.TraceCtx) (_ Iterator, err error) {
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
	connectionLimitMap, err := h.buildConnectionLimitMap(connectionName, table, qualMap, unhandledRestrictions, limit)
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
	iterator := newScanIteratorLocal(h, connectionName, table, h.pluginName, connectionLimitMap, qualMap, columns, limit, scanTraceCtx)
	return iterator, nil
}

func (h *HubLocal) buildConnectionLimitMap(connection, table string, qualMap map[string]*proto.Quals, unhandledRestrictions int, limit int64) (map[string]int64, error) {
	connectionSchema, err := h.GetSchema("", connection)
	if err != nil {
		return nil, err
	}
	schemaMode := connectionSchema.Mode

	// pushing the limit down or not is dependent on the schema.
	// for a static schema, the limit will be the same for all connections (i.e. we either pushdown for all or none)
	// check once whether we should push down
	if limit != -1 && schemaMode == plugin.SchemaModeStatic {
		log.Printf("[TRACE] static schema - using same limit for all connections")
		if !h.shouldPushdownLimit(table, qualMap, unhandledRestrictions, connectionSchema) {
			limit = -1
		}
	}

	// set the limit for the one and only connection
	var connectionLimitMap = make(map[string]int64)
	connectionLimit := limit
	// if schema mode is dynamic, check whether we should push down for each connection
	if schemaMode == plugin.SchemaModeDynamic && !h.shouldPushdownLimit(table, qualMap, unhandledRestrictions, connectionSchema) {
		log.Printf("[INFO] not pushing limit down for connection %s", connection)
		connectionLimit = -1
	}
	connectionLimitMap[connection] = connectionLimit

	return connectionLimitMap, nil
}

func (h *HubLocal) clearConnectionCache(connection string) error {

	_, err := h.plugin.SetConnectionCacheOptions(&proto.SetConnectionCacheOptionsRequest{ClearCacheForConnection: connection})
	if err != nil {
		log.Printf("[WARN] clearConnectionCache failed for connection %s: SetConnectionCacheOptions returned %s", connection, err)
	}
	log.Printf("[INFO] clear connection cache succeeded")
	return err
}

func (h *HubLocal) cacheEnabled(s string) bool {
	// if the caching is disabled for the server, just return false
	if !h.cacheSettings.ServerCacheEnabled {
		return false
	}

	if h.cacheSettings.ClientCacheEnabled != nil {
		return *h.cacheSettings.ClientCacheEnabled
	}

	if envStr, ok := os.LookupEnv(constants.EnvCacheEnabled); ok {
		// set this so that we don't keep looking up the env var
		h.cacheSettings.SetEnabled(envStr)
		return h.cacheEnabled(s)
	}
	return true
}

func (h *HubLocal) cacheTTL(s string) time.Duration {
	log.Printf("[INFO] cacheTTL 1")
	// if the cache ttl has been overridden, then enforce the value
	if h.cacheSettings.Ttl != nil {
		return *h.cacheSettings.Ttl
	}
	if envStr, ok := os.LookupEnv(constants.EnvCacheMaxTTL); ok {
		// set this so that we don't keep looking up the env var
		h.cacheSettings.SetTtl(envStr)
		return h.cacheTTL(s)
	}
	return 10 * time.Hour
}


// resolve the server cache enabled property
func (h *HubLocal) getServerCacheEnabled() bool {
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
