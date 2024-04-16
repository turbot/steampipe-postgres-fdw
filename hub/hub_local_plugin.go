package hub

import (
	net "github.com/turbot/steampipe-plugin-net/net"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

var pluginAlias = "net"

func getPluginFunc() plugin.PluginFunc {
	return net.Plugin
}