package hub

import (
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/turbot/steampipe/steampipeconfig"
)

const keySeparator = `\\`

type connectionMap struct {
	connectionPlugins map[string]*steampipeconfig.ConnectionPlugin
	hub               *Hub
	connectionLock    sync.Mutex
}

func newConnectionMap(hub *Hub) *connectionMap {
	return &connectionMap{
		connectionPlugins: make(map[string]*steampipeconfig.ConnectionPlugin),
		hub:               hub,
	}
}

func (p *connectionMap) getPluginKey(pluginFQN, connectionName string) string {
	return fmt.Sprintf("%s%s%s", pluginFQN, keySeparator, connectionName)
}

func (p *connectionMap) parsePluginKey(key string) (pluginFQN, connectionName string) {
	split := strings.Split(key, keySeparator)
	pluginFQN = split[0]
	connectionName = split[1]
	return
}

func (p *connectionMap) get(pluginFQN, connectionName string) (*steampipeconfig.ConnectionPlugin, error) {
	p.connectionLock.Lock()
	defer p.connectionLock.Unlock()

	c, ok := p.connectionPlugins[p.getPluginKey(pluginFQN, connectionName)]

	// if we do not have this connection in out map, create it
	if !ok {
		var err error
		log.Printf("[TRACE] connectionMap.get lazy loading connection %s", connectionName)
		if c, err = p.hub.createConnectionPlugin(pluginFQN, connectionName); err != nil {
			return nil, err
		}

		// add to map
		p.add(c)
	}
	return c, nil
}

func (p *connectionMap) removeAndKill(pluginFQN, connectionName string) {
	p.connectionLock.Lock()
	defer p.connectionLock.Unlock()
	// kill the connection and instance
	connection, ok := p.connectionPlugins[p.getPluginKey(pluginFQN, connectionName)]
	if ok {
		// if found, kill
		connection.Plugin.Client.Kill()
	}

	// remove from created connectiosn
	delete(p.connectionPlugins, p.getPluginKey(pluginFQN, connectionName))
}

func (p *connectionMap) add(connection *steampipeconfig.ConnectionPlugin) error {
	key := p.getPluginKey(connection.PluginName, connection.ConnectionName)
	p.connectionPlugins[key] = connection
	return nil
}
