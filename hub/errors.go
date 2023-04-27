package hub

import (
	"fmt"
	"github.com/turbot/steampipe/pkg/steampipeconfig/modconfig"
	"strings"
)

func getEmptyAggregatorError(connectionConfig *modconfig.Connection) error {
	patterns := connectionConfig.ConnectionNames
	if len(patterns) == 0 {
		return fmt.Errorf("aggregator '%s' defines no child connections", connectionConfig.Name)
	}
	if len(patterns) == 1 {
		return fmt.Errorf("aggregator '%s' with pattern '%s' matches no connections",
			connectionConfig.Name,
			patterns[0])
	}
	return fmt.Errorf("aggregator '%s' with patterns ['%s'] matches no connections",
		connectionConfig.Name,
		strings.Join(patterns, "','"))
}
