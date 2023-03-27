package settings

import (
	"log"
)

type HubSettings map[HubSettingKey]interface{}

func (s HubSettings) Apply(key string, value string) error {
	if applySetting, found := setters[HubSettingKey(key)]; found {
		return applySetting(s, value)
	}
	log.Println("[WARN] trying to apply unknown setting:", key, "=>", value)
	return nil
}

func (s HubSettings) Get(key HubSettingKey) (interface{}, bool) {
	v, found := s[key]
	return v, found
}

func (s HubSettings) Set(key HubSettingKey, value interface{}) {
	s[key] = value

	for k, v := range s {
		log.Printf("[TRACE] fdw setting %v => %v", k, v)
	}
}

func (s HubSettings) Has(key HubSettingKey) bool {
	_, found := s[key]
	return found
}

func NewSettings() HubSettings {
	return HubSettings{}
}
