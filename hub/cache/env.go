package cache

import (
	"os"
	"strings"
	"time"
)

const CacheEnabledEnvVar = "STEAMPIPE_CACHE"
const CacheTTLEnvVar = "STEAMPIPE_CACHE_TTL"
const defaultTTL = 5 * time.Minute

func Enabled() bool {
	enabled, ok := os.LookupEnv(CacheEnabledEnvVar)
	// if STEAMPIPE_CACHEis NOT set, or of is set to TRUE, caching is enabled
	return !ok || strings.ToUpper(enabled) == "TRUE"
}
