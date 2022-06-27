package hub

import (
	"fmt"
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
	hub               *Hub
	connectionLock    sync.Mutex
}

func newConnectionFactory(hub *Hub) *connectionFactory {
	return &connectionFactory{
		connectionPlugins: make(map[string]*steampipeconfig.ConnectionPlugin),
		hub:               hub,
	}
}

// build a map key for the plugin
func (f *connectionFactory) getPluginKey(pluginFQN, connectionName string) string {
	return fmt.Sprintf("%s%s%s", pluginFQN, keySeparator, connectionName)
}

// extract the plugin FQN and conneciton name from a map key
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
	// if this is an aggregate connection, return error
	// (we must iterate through the child connections explicitly)
	if f.hub.IsAggregatorConnection(connectionName) {
		log.Printf("[WARN] connectionFactory get %s %s called for aggregator connection - invalid (we must iterate through the child connections explicitly)", pluginFQN, connectionName)
		debug.PrintStack()
		return nil, fmt.Errorf("the connectionFactory cannot return or create a connectionPlugin for an aggregate connection")
	}

	c, gotPluginClient := f.connectionPlugins[f.getPluginKey(pluginFQN, connectionName)]
	if gotPluginClient && !c.PluginClient.Exited() {
		return c, nil
	}
	return nil, nil
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
	return f.createConnectionPlugin(pluginFQN, connectionName, &steampipeconfig.CreateConnectionPluginOptions{
		SetConnectionConfig: true,
	})
}

func (f *connectionFactory) createConnectionPlugin(pluginFQN string, connectionName string, opts *steampipeconfig.CreateConnectionPluginOptions) (*steampipeconfig.ConnectionPlugin, error) {
	f.connectionLock.Lock()
	defer f.connectionLock.Unlock()
	log.Printf("[TRACE] connectionFactory.createConnectionPlugin lazy loading connection %s", connectionName)
	c, err := f.hub.createConnectionPlugin(pluginFQN, connectionName, opts)
	if err != nil {
		return nil, err
	}

	// add to map
	f.add(c)
	return c, nil
}

func (f *connectionFactory) add(connection *steampipeconfig.ConnectionPlugin) {
	key := f.getPluginKey(connection.PluginName, connection.ConnectionName)
	f.connectionPlugins[key] = connection
}

func (f *connectionFactory) getSchema(pluginFQN, connectionName string) (*proto.Schema, error) {
	log.Printf("[TRACE] connectionFactory getSchema %s %s", pluginFQN, connectionName)
	// do we have this connection already loaded
	c, err := f.get(pluginFQN, connectionName)
	if err != nil {
		return nil, err
	}
	if c != nil {
		log.Printf("[TRACE] already loaded")
		return c.Schema, nil
	}

	log.Printf("[TRACE] searching for other connections using same plugin")
	for _, c := range f.connectionPlugins {
		if c.PluginName == pluginFQN {
			// so we have found another connection with this plugin
			log.Printf("[TRACE] found another connection with this plugin")
			// if the schema mode is dynamic we cannot resuse the schema
			if c.Schema.Mode == plugin.SchemaModeDynamic {
				log.Printf("[TRACE] dynamic schema - cannot reuse")
				break
			}
			log.Printf("[TRACE] returning schema")
			return c.Schema, nil
		}
	}
	// otherwise create the connection, but DO NOT set connection config n(this will have been done by the CLI)
	log.Printf("[TRACE] creating connection plugin to get schema")
	c, err = f.createConnectionPlugin(pluginFQN, connectionName, &steampipeconfig.CreateConnectionPluginOptions{
		SetConnectionConfig: false,
	})
	if err != nil {
		return nil, err
	}
	return c.Schema, nil
}
