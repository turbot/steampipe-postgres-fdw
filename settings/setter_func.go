package settings

import (
	"encoding/json"
	"time"
)

func (s *HubCacheSettings) SetEnabled(jsonValue string) error {
	var enable bool
	if err := json.Unmarshal([]byte(jsonValue), &enable); err != nil {
		return err
	}
	s.Enabled = &enable
	return nil
}

func (s *HubCacheSettings) SetCacheTtl(jsonValue string) error {
	var enable int
	if err := json.Unmarshal([]byte(jsonValue), &enable); err != nil {
		return err
	}
	ttl := time.Duration(enable) * time.Second
	s.Ttl = &ttl
	return nil
}

func (s *HubCacheSettings) SetCacheClearTime(_ string) error {
	s.ClearTime = time.Now()
	return nil
}
