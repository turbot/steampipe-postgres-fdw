package settings

import (
	"fmt"
	"strconv"
	"time"
)

type setterFunc func(HubSettings, string) error

var setters map[HubSettingKey]setterFunc = map[HubSettingKey]setterFunc{
	SettingKeyCacheEnabledOverride:   setCacheOverride,
	SettingKeyCacheTtlOverride:       setCacheTTL,
	SettingKeyCacheClearTimeOverride: setCacheClearTime,
}

// define the handler functions
func setCacheOverride(hs HubSettings, value string) error {
	if value == "true" {
		hs.Set(SettingKeyCacheEnabledOverride, true)
	} else if value == "false" {
		hs.Set(SettingKeyCacheEnabledOverride, false)
	} else {
		return fmt.Errorf("valid values for '%s' are 'true' and 'false', got '%v'", string(SettingKeyCacheEnabledOverride), value)
	}
	return nil
}
func setCacheClearTime(hs HubSettings, _ string) error {
	hs.Set(SettingKeyCacheClearTimeOverride, time.Now())
	return nil
}

func setCacheTTL(hs HubSettings, value string) error {
	atoi, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("valid value for '%s' is the number of seconds, got '%s'", string(SettingKeyCacheTtlOverride), value)
	}
	hs.Set(SettingKeyCacheTtlOverride, time.Duration(atoi)*time.Second)
	return nil
}
