package hub

import (
	"github.com/turbot/steampipe-plugin-sdk/v6/plugin"
)

// this is provided to ensure GRPC FDW version builds - it will be replaced by template for standalone version
//
//nolint:unused // only newLocalHub reads it, and that is unused in this build
var pluginAlias string

//nolint:unused // only newLocalHub calls it, and that is unused in this build
func getPluginFunc() plugin.PluginFunc {
	panic("this function version should not be called - the templated version should be used")
}
