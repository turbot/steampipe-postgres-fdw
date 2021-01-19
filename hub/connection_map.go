package hub

import (
	"fmt"
	"log"
	"strings"

	"github.com/turbot/steampipe-plugin-sdk/grpc/proto"
	"github.com/turbot/steampipe/connection_config"
)

const keySeparator = `\\`

type connectionMap struct {
	connectionPlugins  map[string]*connection_config.ConnectionPlugin
	tableConnectionMap map[string]string
	tableColumnMap     map[string][]*proto.ColumnDefinition
}

func newConnectionMap() *connectionMap {
	return &connectionMap{
		connectionPlugins:  make(map[string]*connection_config.ConnectionPlugin),
		tableConnectionMap: make(map[string]string),
		tableColumnMap:     make(map[string][]*proto.ColumnDefinition),
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

func (p *connectionMap) getTableKey(table, connectionName string) string {
	return fmt.Sprintf("%s%s%s", table, keySeparator, connectionName)
}

func (p *connectionMap) get(pluginFQN, connectionName string) *connection_config.ConnectionPlugin {
	return p.connectionPlugins[p.getPluginKey(pluginFQN, connectionName)]
}

func (p *connectionMap) add(connection *connection_config.ConnectionPlugin) error {
	key := p.getPluginKey(connection.PluginName, connection.ConnectionName)
	p.connectionPlugins[key] = connection
	return p.updateTableMap(connection)
}

// add the tables provided by this plugin to the tableConnectionMap
func (p *connectionMap) updateTableMap(connection *connection_config.ConnectionPlugin) error {
	pluginKey := p.getPluginKey(connection.PluginName, connection.ConnectionName)
	log.Printf("[TRACE] updateTableMap for %s\n", pluginKey)

	for table, columns := range connection.Schema.Schema {
		// qualify the table with the schema
		tableKey := p.getTableKey(table, connection.ConnectionName)
		if existingPluginKey, ok := p.tableConnectionMap[tableKey]; ok {
			// this table is already in the map - not valid
			return fmt.Errorf("table %s is implemented by more than 1 plugin: %s and %s\n", tableKey, existingPluginKey, pluginKey)
		}
		log.Printf("[TRACE] set %s = %s\n", tableKey, pluginKey)
		// store the key to the plugin map rather than the plugin iteself - this way if there is an duplicate we know what it is
		p.tableConnectionMap[tableKey] = pluginKey
		p.tableColumnMap[tableKey] = columns.Columns
	}
	return nil
}

// get the plugin which serves the given table
// note: table name will include schema, i.e. the schema name
func (p *connectionMap) getConnectionPluginForTable(table, connectionName string) (*connection_config.ConnectionPlugin, error) {
	tableKey := p.getTableKey(table, connectionName)
	connectionKey := p.tableConnectionMap[tableKey]

	log.Printf("[TRACE] getConnectionPluginForTable table %s, connectionKey %s\n", tableKey, connectionKey)
	connectionPlugin := p.connectionPlugins[connectionKey]
	if connectionPlugin == nil || connectionPlugin.Plugin.Stub == nil {
		log.Printf("[TRACE] connectionPlugins %v\n", p.connectionPlugins)
		return nil, fmt.Errorf("no ConnectionPlugin loaded which provides table '%s'", tableKey)
	}
	return connectionPlugin, nil
}
