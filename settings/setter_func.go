package settings

import (
	"encoding/json"
	"time"
)

func (s *HubCacheSettings) SetCache(jsonValue string) error {
	var enable bool
	if err := json.Unmarshal([]byte(jsonValue), &enable); err != nil {
		return err
	}
	s.CacheEnabled = &enable
	return nil
}

func (s *HubCacheSettings) SetCacheTtl(jsonValue string) error {
	var enable int
	if err := json.Unmarshal([]byte(jsonValue), &enable); err != nil {
		return err
	}
	ttl := time.Duration(enable) * time.Second
	s.CacheTtl = &ttl
	return nil
}

func (s *HubCacheSettings) SetCacheClearTime(_ string) error {
	s.CacheClearTime = time.Now()
	return nil
}
