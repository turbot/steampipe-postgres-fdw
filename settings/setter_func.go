package settings

import (
	"fmt"
	"strconv"
	"time"
)

type setterFunc func(HubSettings, string) error

var setters map[HubSettingKey]setterFunc = map[HubSettingKey]setterFunc{
	SettingKeyCacheEnabledOverride: func(hs HubSettings, value string) error {
		if value == "true" {
			hs.Set(SettingKeyCacheEnabledOverride, true)
		} else if value == "false" {
			hs.Set(SettingKeyCacheEnabledOverride, false)
		} else {
			return fmt.Errorf("valid values for '%s' are 'true' and 'false'. got '%v'", string(SettingKeyCacheEnabledOverride), value)
		}
		return nil
	},
	SettingKeyCacheTtlOverride: func(hs HubSettings, value string) error {
		atoi, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		hs.Set(SettingKeyCacheTtlOverride, time.Duration(atoi)*time.Second)
		return nil
	},
}
