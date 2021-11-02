package hub

import (
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/turbot/steampipe/steampipeconfig"
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
	f.connectionLock.Lock()
	defer f.connectionLock.Unlock()
	// if this is an aggregate connection, return error
	// (we must iterate through the child connections explicitly)
	if f.hub.IsAggregatorConnection(connectionName) {
		debug.PrintStack()
		return nil, fmt.Errorf("the connectionFactory cannot return or create a connectionPlugin for an aggregate connection")
	}

	c, gotPluginClient := f.connectionPlugins[f.getPluginKey(pluginFQN, connectionName)]
	if gotPluginClient && c.PluginClient.Exited() {
		log.Printf("[WARN] client for %s has exited - reloading plugin", pluginFQN)
		gotPluginClient = false
	}
	// if we do not have this connection in our map, create it
	if !gotPluginClient {
		var err error
		log.Printf("[TRACE] connectionFactory.get lazy loading connection %s", connectionName)
		if c, err = f.hub.createConnectionPlugin(pluginFQN, connectionName); err != nil {
			return nil, err
		}

		// add to map
		f.add(c)
	}
	return c, nil
}

func (f *connectionFactory) add(connection *steampipeconfig.ConnectionPlugin) {
	key := f.getPluginKey(connection.PluginName, connection.ConnectionName)
	f.connectionPlugins[key] = connection
}
