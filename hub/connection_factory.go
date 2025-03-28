package hub

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/turbot/steampipe-postgres-fdw/utils"
	"github.com/turbot/steampipe/pkg/pluginmanager"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe/pkg/steampipeconfig"
)

const keySeparator = `\\`

// connectionFactory is responsible for creating and storing connectionPlugins
type connectionFactory struct {
	connectionPlugins map[string]*steampipeconfig.ConnectionPlugin
	hub               *RemoteHub
	connectionLock    sync.Mutex
}

func newConnectionFactory(hub *RemoteHub) *connectionFactory {
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
		log.Printf("[INFO] connectionFactory.createConnectionPlugin create connection %s - no config found so reloading config!", connectionName)

		// ask hub to reload config - it's possible we are being asked to import a newly added connection
		// TODO remove need for hub to load config at all
		if _, err := f.hub.LoadConnectionConfig(); err != nil {
			log.Printf("[ERROR] LoadConnectionConfig failed %v ", err)
			return nil, err
		}
		// now try to get config again
		connection, ok = steampipeconfig.GlobalConfig.Connections[connectionName]
		if !ok {
			log.Printf("[WARN] no config found for connection %s", connectionName)
			return nil, fmt.Errorf("no config found for connection %s", connectionName)
		}
	}

	log.Printf("[TRACE] createConnectionPlugin plugin %s, connection %s, config: %s\n", utils.PluginFQNToSchemaName(pluginFQN), connectionName, connection.Config)

	// get plugin manager
	pluginManager, err := pluginmanager.GetPluginManager()
	if err != nil {
		return nil, err
	}

	connectionPlugins, res := steampipeconfig.CreateConnectionPlugins(pluginManager, []string{connection.Name})
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
		// we already have a connection plugin -  refetch the schema
		log.Printf("[TRACE] already loaded %s %s: ", pluginFQN, connectionName)

		return c.GetSchema(connectionName)
	}

	//  create the connection
	log.Printf("[TRACE] creating connection plugin to get schema")
	c, err = f.createConnectionPlugin(pluginFQN, connectionName)
	if err != nil {
		return nil, err
	}
	return c.ConnectionMap[connectionName].Schema, nil
}
