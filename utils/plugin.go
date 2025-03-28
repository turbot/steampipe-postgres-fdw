package utils

import (
	"fmt"

	"github.com/turbot/go-kit/helpers"
)

const maxSchemaNameLength = 63

// PluginFQNToSchemaName convert a full plugin name to a schema name
// schemas in postgres are limited to 63 chars - the name may be longer than this, in which case trim the length
// and add a hash to the end to make unique
func PluginFQNToSchemaName(pluginFQN string) string {
	if len(pluginFQN) < maxSchemaNameLength {
		return pluginFQN
	}

	schemaName := TrimSchemaName(pluginFQN) + fmt.Sprintf("-%x", helpers.StringFnvHash(pluginFQN))
	return schemaName
}

func TrimSchemaName(pluginFQN string) string {
	if len(pluginFQN) < maxSchemaNameLength {
		return pluginFQN
	}

	return pluginFQN[:maxSchemaNameLength-9]
}
