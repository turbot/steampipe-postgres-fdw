package settings

import (
	"log"
	"time"
)

type setterFunc func(string) error

type HubCacheSettings struct {
	CacheEnabled   *bool
	CacheTtl       *time.Duration
	CacheClearTime time.Time

	// a map of handler function which map settings key to setter functions
	// for individual properties
	setters map[HubSettingKey]setterFunc
}

func NewCacheSettings() *HubCacheSettings {
	hs := &HubCacheSettings{}
	hs.setters = map[HubSettingKey]setterFunc{
		SettingKeyCacheEnabledOverride:   hs.SetCache,
		SettingKeyCacheTtlOverride:       hs.SetCacheTtl,
		SettingKeyCacheClearTimeOverride: hs.SetCacheClearTime,
	}
	return hs
}

func (s *HubCacheSettings) Apply(key string, jsonValue string) error {
	if applySetting, found := s.setters[HubSettingKey(key)]; found {
		return applySetting(jsonValue)
	}
	log.Println("[WARN] trying to apply unknown setting:", key, "=>", jsonValue)
	return nil
}
