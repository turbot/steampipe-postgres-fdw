package cache

import (
	"log"
	"os"
	"strings"

	"github.com/turbot/go-kit/types"
	"github.com/turbot/steampipe/steampipeconfig/options"
)

const CacheEnabledEnvVar = "STEAMPIPE_CACHE"
const CacheTTLEnvVar = "STEAMPIPE_CACHE_TTL"
const defaultTTL = 300

func CacheEnabled(settings *options.Connection) (enabled bool) {
	log.Printf("[WARN] CacheEnabled %+v", settings)
	if settings != nil && settings.Cache != nil {
		log.Printf("[WARN] settings.Cache %+v", settings.Cache)
		enabled = *settings.Cache
	} else if envStr, ok := os.LookupEnv(CacheEnabledEnvVar); ok {
		log.Printf("[WARN] READING ENV")
		enabled = strings.ToUpper(envStr) == "TRUE"
	} else {
		log.Printf("[WARN] DEF TO TRUE")
		// default to enabled
		enabled = true
	}

	return
}

func CacheTTL(settings *options.Connection) int {
	var ttlSecs int
	if settings != nil && settings.CacheTTL != nil {
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
