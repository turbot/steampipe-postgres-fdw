package hub

import (
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

// this is provided to ensure GRPC FDW version builds - it will be replaced by template for standalone version
var pluginAlias string

func getPluginFunc() plugin.PluginFunc {
	panic("this function version should not be called - the templated version should be used")
}
