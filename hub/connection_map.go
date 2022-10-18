package hub

import (
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"sync"

	"github.com/turbot/steampipe-plugin-sdk/v4/grpc/proto"
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
func (f *connectionFactory) get(connectionName string) (*steampipeconfig.ConnectionPlugin, error) {
	log.Printf("[TRACE] connectionFactory get connection %s", connectionName)

	// if we this is a legacy aggregate connection, return error
	// (it is invalid to try to 'get' a legacy aggregator connection directly)
	if f.hub.IsLegacyAggregatorConnection(connectionName) {
		log.Printf("[WARN] connectionFactory get %s called for aggregator connection - invalid (we must iterate through the child connections explicitly)", connectionName)
		debug.PrintStack()
		return nil, fmt.Errorf("cannot create a connectionPlugin for a legacy aggregator connection")
	}

	f.connectionLock.Lock()
	defer f.connectionLock.Unlock()

	// build a map key for the plugin
	key := connectionName

	c, gotConnectionPlugin := f.connectionPlugins[key]
	if gotConnectionPlugin && !c.PluginClient.Exited() {
		return c, nil
	}

	// so either we have not yet instantiated the connection plugin, or it has exited
	if !gotConnectionPlugin {
		log.Printf("[TRACE] no connectionPlugin loaded with key %s", key)
	} else {
		log.Printf("[TRACE] connectionPluginwith key %s has exited - reloading", key)
	}

	log.Printf("[TRACE] failed to get connection %s", connectionName)
	return nil, nil
}

//func (f *connectionFactory) connectionPluginKey(pluginFQN string, connectionName string) string {
//	return fmt.Sprintf("%s%s%s", pluginFQN, keySeparator, connectionName)
//}

func (f *connectionFactory) getOrCreate(connectionName string) (*steampipeconfig.ConnectionPlugin, error) {
	log.Printf("[TRACE] connectionFactory getOrCreate  %s", connectionName)
	c, err := f.get(connectionName)
	if err != nil {
		return nil, err
	}
	if c != nil {
		return c, nil
	}

	// otherwise create the connection plugin, setting connection config
	return f.createConnectionPlugin(connectionName)
}

func (f *connectionFactory) createConnectionPlugin(connectionName string) (*steampipeconfig.ConnectionPlugin, error) {
	f.connectionLock.Lock()
	defer f.connectionLock.Unlock()
	log.Printf("[TRACE] connectionFactory.createConnectionPlugin create connection %s", connectionName)

	// load the config for this connection

	log.Printf("[TRACE] createConnectionPlugin connection %s\n", connectionName)

	connectionPlugins, res := steampipeconfig.CreateConnectionPlugins(connectionName)
	if res.Error != nil {
		return nil, res.Error
	}
	if connectionPlugins[connectionName] == nil {
		if len(res.Warnings) > 0 {
			return nil, fmt.Errorf("%s", strings.Join(res.Warnings, ","))
		}
		return nil, fmt.Errorf("CreateConnectionPlugins did not return error but '%s' not found in connection map", connectionName)
	}

	connectionPlugin := connectionPlugins[connectionName]
	f.add(connectionPlugin, connectionName)

	return connectionPlugin, nil
}

func (f *connectionFactory) add(connectionPlugin *steampipeconfig.ConnectionPlugin, connectionName string) {
	log.Printf("[TRACE] connectionFactory add %s - adding all connections supported by plugin", connectionName)

	// add a map entry for all connections supported by the plugib
	for c := range connectionPlugin.ConnectionMap {
		log.Printf("[TRACE] add %s", c)
		connectionPluginKey := c
		// NOTE: there may already be map entries for some connections
		// - this could occur if the filewatcher detects a connection added for a plugin
		if _, ok := f.connectionPlugins[connectionPluginKey]; !ok {
			f.connectionPlugins[connectionPluginKey] = connectionPlugin
		}
	}
}

func (f *connectionFactory) getSchema(connectionName string) (*proto.Schema, error) {
	log.Printf("[TRACE] connectionFactory getSchema %s", connectionName)
	// do we have this connection already loaded
	c, err := f.get(connectionName)
	if err != nil {
		return nil, err
	}
	if c != nil {
		log.Printf("[TRACE] already loaded %s: ", connectionName)
		for k := range c.ConnectionMap {
			log.Printf("[TRACE] %s", k)
		}
		log.Printf("[TRACE] %v", c.ConnectionMap[connectionName].Schema)

		return c.ConnectionMap[connectionName].Schema, nil
	}

	// otherwise create the connection
	log.Printf("[TRACE] creating connection plugin to get schema")
	c, err = f.createConnectionPlugin(connectionName)
	if err != nil {
		return nil, err
	}
	return c.ConnectionMap[connectionName].Schema, nil
}
