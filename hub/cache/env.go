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
	enabled := os.Getenv(CacheEnabledEnvVar)
	return strings.ToUpper(enabled) == "TRUE"
}
