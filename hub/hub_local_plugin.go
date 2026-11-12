package hub

import (
	"github.com/turbot/steampipe-plugin-sdk/v6/plugin"
)

// this is provided to ensure GRPC FDW version builds - it will be replaced by template for standalone version
//
//nolint:unused // read by newLocalHub in the standalone build
var pluginAlias string

//nolint:unused // called by newLocalHub in the standalone build
func getPluginFunc() plugin.PluginFunc {
	panic("this function version should not be called - the templated version should be used")
}
