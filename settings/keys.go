package settings

type HubSettingKey string

const (
	SettingKeyCacheEnabled           HubSettingKey = "cache"
	SettingKeyCacheTtlOverride       HubSettingKey = "cache_ttl"
	SettingKeyCacheClearTimeOverride HubSettingKey = "cache_clear_time"
)
