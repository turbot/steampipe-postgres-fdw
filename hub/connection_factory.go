package hub

import (
	"fmt"
	"github.com/turbot/steampipe/pkg/utils"
	"log"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe/pkg/steampipeconfig"
)

const keySeparator = `\\`

// connectionFactory is responsible for creating and storing connectionPlugins
type connectionFactory struct {
	connectionPlugins map[string]*steampipeconfig.ConnectionPlugin
	hub               *Hub
	connectionLock    sync.Mutex
}

func newConnectionFactory(hub *Hub) *connectionFactory {
	return &connectionFactory{
		connectionPlugins: make(map[string]*steampipeconfig.ConnectionPlugin),
		hub:               hub,
	}
}

// extract the plugin FQN and connection name from a map key
func (f *connectionFactory) parsePluginKey(key string) (pluginFQN, connectionName string) {
	split := strings.Split(key, keySeparator)
	pluginFQN = split[0]
	connectionName = split[1]
	return
}

// if a connection plugin for the plugin and connection, return it. If it does not, create it, store in map and return it
// NOTE: there is special case logic got aggregate connections
func (f *connectionFactory) get(pluginFQN, connectionName string) (*steampipeconfig.ConnectionPlugin, error) {
	log.Printf("[TRACE] connectionFactory get plugin: %s connection %s", pluginFQN, connectionName)

	// if we this is a legacy aggregate connection, return error
	// (it is invalid to try to 'get' a legacy aggregator connection directly)
	if f.hub.IsLegacyAggregatorConnection(connectionName) {
		log.Printf("[WARN] connectionFactory get %s %s called for aggregator connection - invalid (we must iterate through the child connections explicitly)", pluginFQN, connectionName)
		debug.PrintStack()
		return nil, fmt.Errorf("cannot create a connectionPlugin for a legacy aggregator connection")
	}

	f.connectionLock.Lock()
	defer f.connectionLock.Unlock()

	// build a map key for the plugin
	key := f.connectionPluginKey(pluginFQN, connectionName)

	c, gotConnectionPlugin := f.connectionPlugins[key]
	if gotConnectionPlugin && !c.PluginClient.Exited() {
		return c, nil
	}

	// so either we have not yet instantiated the connection plugin, or it has exited
	if !gotConnectionPlugin {
		log.Printf("[TRACE] no connectionPlugin loaded with key %s (len %d)", key, len(f.connectionPlugins))
		for k := range f.connectionPlugins {
			log.Printf("[TRACE] key: %s", k)
		}
	} else {
		log.Printf("[TRACE] connectionPluginwith key %s has exited - reloading", key)
	}

	log.Printf("[TRACE] failed to get plugin: %s connection %s", pluginFQN, connectionName)
	return nil, nil
}

func (f *connectionFactory) connectionPluginKey(pluginFQN string, connectionName string) string {
	return fmt.Sprintf("%s%s%s", pluginFQN, keySeparator, connectionName)
}

func (f *connectionFactory) getOrCreate(pluginFQN, connectionName string) (*steampipeconfig.ConnectionPlugin, error) {
	log.Printf("[TRACE] connectionFactory getOrCreate %s %s", pluginFQN, connectionName)
	c, err := f.get(pluginFQN, connectionName)
	if err != nil {
		return nil, err
	}
	if c != nil {
		return c, nil
	}

	// otherwise create the connection plugin, setting connection config
	return f.createConnectionPlugin(pluginFQN, connectionName)
}

func (f *connectionFactory) createConnectionPlugin(pluginFQN string, connectionName string) (*steampipeconfig.ConnectionPlugin, error) {
	f.connectionLock.Lock()
	defer f.connectionLock.Unlock()
	log.Printf("[TRACE] connectionFactory.createConnectionPlugin create connection %s", connectionName)

	// load the config for this connection
	connection, ok := steampipeconfig.GlobalConfig.Connections[connectionName]
	if !ok {
		log.Printf("[WARN] no config found for connection %s", connectionName)
		return nil, fmt.Errorf("no config found for connection %s", connectionName)
	}

	log.Printf("[TRACE] createConnectionPlugin plugin %s, connection %s, config: %s\n", utils.PluginFQNToSchemaName(pluginFQN), connectionName, connection.Config)

	connectionPlugins, res := steampipeconfig.CreateConnectionPlugins([]string{connection.Name})
	if res.Error != nil {
		return nil, res.Error
	}
	if connectionPlugins[connection.Name] == nil {
		if len(res.Warnings) > 0 {
			return nil, fmt.Errorf("%s", strings.Join(res.Warnings, ","))
		}
		return nil, fmt.Errorf("CreateConnectionPlugins did not return error but '%s' not found in connection map", connection.Name)
	}

	connectionPlugin := connectionPlugins[connection.Name]
	f.add(connectionPlugin, connectionName)

	return connectionPlugin, nil
}

func (f *connectionFactory) add(connectionPlugin *steampipeconfig.ConnectionPlugin, connectionName string) {
	log.Printf("[TRACE] connectionFactory add %s - adding all connections supported by plugin", connectionName)

	// add a map entry for all connections supported by the plugib
	for c := range connectionPlugin.ConnectionMap {
		connectionPluginKey := f.connectionPluginKey(connectionPlugin.PluginName, c)
		log.Printf("[TRACE] add %s (%s)", c, connectionPluginKey)
		// NOTE: there may already be map entries for some connections
		// - this could occur if the filewatcher detects a connection added for a plugin
		if _, ok := f.connectionPlugins[connectionPluginKey]; !ok {
			f.connectionPlugins[connectionPluginKey] = connectionPlugin
		}
	}
}

func (f *connectionFactory) getSchema(pluginFQN, connectionName string) (*proto.Schema, error) {
	log.Printf("[TRACE] connectionFactory getSchema %s %s", pluginFQN, connectionName)
	// do we have this connection already loaded
	c, err := f.get(pluginFQN, connectionName)
	if err != nil {
		return nil, err
	}
	if c != nil {
		log.Printf("[TRACE] already loaded %s %s: ", pluginFQN, connectionName)
		for k := range c.ConnectionMap {
			log.Printf("[TRACE] %s", k)
		}
		log.Printf("[TRACE] %v", c.ConnectionMap[connectionName].Schema)

		return c.ConnectionMap[connectionName].Schema, nil
	}

	// optimisation - find other plugins with the same schema
	// NOTE: this is only relevant for legacy plugins which do not support multiple connections
	log.Printf("[TRACE] searching for other connections using same plugin")
	for _, c := range f.connectionPlugins {
		if c.PluginName == pluginFQN {
			// if this plugin support multiple connections but does not have the schema for this connection
			// there must have been an issue setting connection config
			// the CLI should not have called importForeignSchema for this connection - so just return an error
			if c.SupportedOperations.MultipleConnections {
				return nil, fmt.Errorf("plugin %s is not returning schema for connection %s - check logs for a connection initialisation error", pluginFQN, connectionName)
			}

			log.Printf("[TRACE] found connectionPlugin with same pluginFQN: %s, conneciton map: %v ", c.PluginName, c.ConnectionMap)
			// so we know this connection plugin should have a single connection
			if len(c.ConnectionMap) > 1 {
				return nil, fmt.Errorf("unexpected error: plugin %s does not support multi connections but has %d connections", pluginFQN, len(c.ConnectionMap))
			}
			// get the first and only connection data
			for _, connectionData := range c.ConnectionMap {
				// so we have found another connection with this plugin
				log.Printf("[TRACE] found another connection with this plugin: %v", c.ConnectionMap)

				// if the schema mode is dynamic we cannot reuse the schema
				if connectionData.Schema.Mode == plugin.SchemaModeDynamic {
					log.Printf("[TRACE] dynamic schema - cannot reuse")
					break
				}
				log.Printf("[TRACE] returning schema")
				return connectionData.Schema, nil
			}
		}
	}
	// otherwise create the connection
	log.Printf("[TRACE] creating connection plugin to get schema")
	c, err = f.createConnectionPlugin(pluginFQN, connectionName)
	if err != nil {
		return nil, err
	}
	return c.ConnectionMap[connectionName].Schema, nil
}
