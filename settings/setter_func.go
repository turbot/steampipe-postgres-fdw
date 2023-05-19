package settings

import (
	"encoding/json"
	"log"
	"time"
)

func (s *HubCacheSettings) SetEnabled(jsonValue string) error {
	log.Printf("[TRACE] SetEnabled %s", jsonValue)
	var enable bool
	if err := json.Unmarshal([]byte(jsonValue), &enable); err != nil {
		return err
	}
	s.Enabled = &enable
	return nil
}

func (s *HubCacheSettings) SetTtl(jsonValue string) error {
	log.Printf("[TRACE] SetTtl %s", jsonValue)
	var enable int
	if err := json.Unmarshal([]byte(jsonValue), &enable); err != nil {
		return err
	}
	ttl := time.Duration(enable) * time.Second
	s.Ttl = &ttl
	return nil
}

func (s *HubCacheSettings) SetClearTime(_ string) error {
	log.Printf("[TRACE] SetClearTime")
	s.ClearTime = time.Now()
	return nil
}
