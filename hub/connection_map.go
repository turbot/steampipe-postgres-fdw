package hub

import (
	"fmt"
	"github.com/turbot/steampipe/pkg/steampipeconfig/modconfig"
	"github.com/turbot/steampipe/pluginmanager"
	"log"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/turbot/steampipe-plugin-sdk/v3/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v3/plugin"
	"github.com/turbot/steampipe/pkg/steampipeconfig"
)

const keySeparator = `\\`

// connectionFactory is responsible for creating and storing connectionPlugins
type connectionFactory struct {
	connectionPlugins map[string]*steampipeconfig.ConnectionPlugin
	// map of loaded multi-connection plugins, keyed by plugin FQN
	multiConnectionPlugins map[string]bool
	hub                    *Hub
	connectionLock         sync.Mutex
}

func newConnectionFactory(hub *Hub) *connectionFactory {
	return &connectionFactory{
		connectionPlugins:      make(map[string]*steampipeconfig.ConnectionPlugin),
		multiConnectionPlugins: make(map[string]bool),
		hub:                    hub,
	}
}

// build a map key for the plugin
func (f *connectionFactory) getPluginKey(pluginFQN, connectionName string) string {
	// if we have already loaded this plugin and it supports multi connections, just use FQN
	if f.multiConnectionPlugins[pluginFQN] {
		return pluginFQN
	}

	// otherwise assume a legacy plugin and include connection name in key
	// (if this tr
	return fmt.Sprintf("%s%s%s", pluginFQN, keySeparator, connectionName)
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
	log.Printf("[TRACE] connectionFactory get %s %s", pluginFQN, connectionName)
	f.connectionLock.Lock()
	defer f.connectionLock.Unlock()

	// build a map key for the plugin
	var key string
	// if we have already loaded this plugin and it supports multi connections, just use FQN
	if f.multiConnectionPlugins[pluginFQN] {
		key = pluginFQN
	} else {
		// otherwise try looking for a legacy connection plugin
		key = f.legacyConnectionPluginKey(pluginFQN, connectionName)
	}

	c, gotPluginClient := f.connectionPlugins[key]
	log.Printf("[TRACE] c %v gotPluginClient %v", c, gotPluginClient)

	if gotPluginClient && !c.PluginClient.Exited() {
		return c, nil
	}

	log.Printf("[TRACE] c %v gotPluginClient %v", c, gotPluginClient)

	// if we failed to find the connection plugins, and it is a legacy aggregate connection, return error
	// (it is invalid to try to 'get' a legacy aggregator connection directly)
	if f.hub.IsLegacyAggregatorConnection(connectionName) {
		log.Printf("[WARN] connectionFactory get %s %s called for aggregator connection - invalid (we must iterate through the child connections explicitly)", pluginFQN, connectionName)
		debug.PrintStack()
		return nil, fmt.Errorf("the connectionFactory cannot return or create a connectionPlugin for an aggregate connection")
	}

	return nil, nil
}

func (f *connectionFactory) legacyConnectionPluginKey(pluginFQN string, connectionName string) string {
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
	log.Printf("[TRACE] get returned %v, %v", c, err)

	// otherwise create the connection plugin, setting connection config
	return f.createConnectionPlugin(pluginFQN, connectionName)
}

func (f *connectionFactory) createConnectionPlugin(pluginFQN string, connectionName string) (*steampipeconfig.ConnectionPlugin, error) {
	f.connectionLock.Lock()
	defer f.connectionLock.Unlock()
	log.Printf("[TRACE] connectionFactory.createConnectionPlugin lazy loading connection %s", connectionName)

	// load the config for this connection
	connection, ok := steampipeconfig.GlobalConfig.Connections[connectionName]
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
		return nil, fmt.Errorf("CreateConnectionPlugins did not return error but '%s' not found in connection map", connection.Name)
	}

	connectionPlugin := connectionPlugins[connection.Name]
	f.add(connectionPlugin, connectionName)

	return connectionPlugin, nil
}

func (f *connectionFactory) add(connectionPlugin *steampipeconfig.ConnectionPlugin, connectionName string) {
	// key to add the connection with
	var connectionPluginKey string

	if connectionPlugin.SupportedOperations.MultipleConnections {
		// if this plugin supports multiple connections, add to multiConnectionPlugins map
		f.multiConnectionPlugins[connectionPlugin.PluginName] = true
		// use plugin name as key
		connectionPluginKey = connectionPlugin.PluginName
	} else {
		// for legacy plugins, include the connection name in the key
		connectionPluginKey = f.legacyConnectionPluginKey(connectionPlugin.PluginName, connectionName)
	}

	// add to map
	f.connectionPlugins[connectionPluginKey] = connectionPlugin
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
			// this plugin CANNOT suport multiple connections, otherwise f.get woul dhave returned it
			if c.SupportedOperations.MultipleConnections {
				return nil, fmt.Errorf("unexpected error: plugin %s supports multi connections but was not returned for connection %s", connectionName)
			}

			// so we know this connection plugin has a single connection
			connectionData := c.ConnectionMap[connectionName]
			// so we have found another connection with this plugin
			log.Printf("[TRACE] found another connection with this plugin")

			// if the schema mode is dynamic we cannot reuse the schema
			if connectionData.Schema.Mode == plugin.SchemaModeDynamic {
				log.Printf("[TRACE] dynamic schema - cannot reuse")
				break
			}
			log.Printf("[TRACE] returning schema")
			return connectionData.Schema, nil
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
