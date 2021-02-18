package cache

import (
	"log"
	"os"
	"strings"
	"time"
)

const CacheEnabledEnvVar = "STEAMPIPE_CACHE"
const CacheTTLEnvVar = "STEAMPIPE_CACHE_TTL"
const defaultTTL = 5 * time.Minute

func Enabled() bool {
	enabled := strings.ToUpper(os.Getenv(CacheEnabledEnvVar)) == "TRUE"
	if enabled {
		log.Printf("[INFO] caching ENABLED")
	} else {
		log.Printf("[INFO] caching DISABLED")
	}
	return enabled
}
