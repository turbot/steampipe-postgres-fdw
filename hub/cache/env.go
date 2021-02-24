package cache

import (
	"os"
	"strings"

	"github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe/connection_config"
)

const CacheEnabledEnvVar = "STEAMPIPE_CACHE"
const CacheTTLEnvVar = "STEAMPIPE_CACHE_TTL"
const defaultTTL = 300

func CacheEnabled(settings *connection_config.Settings) (enabled bool) {
	if settings.Cache != nil {
		enabled = *settings.Cache
	} else {
		enabled = strings.ToUpper(os.Getenv(CacheEnabledEnvVar)) == "TRUE"
	}
	return
}

func CacheTTL(settings *connection_config.Settings) int {
	var ttlSecs int
	if settings.CacheTTL != nil {
		ttlSecs = *settings.CacheTTL
	} else {
		if ttlString, ok := os.LookupEnv(CacheTTLEnvVar); ok {
			if parsed, err := types.ToInt64(ttlString); err == nil {
				ttlSecs = int(parsed)
			}
		}
	}
	if ttlSecs == 0 {
		ttlSecs = defaultTTL
	}
	return ttlSecs
}
