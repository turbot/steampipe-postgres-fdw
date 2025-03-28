package app_specific

// app specific env vars
var EnvUpdateCheck,
	EnvInstallDir,
	EnvInstallDatabase,
	EnvServicePassword,
	EnvMaxParallel,
	EnvDatabaseStartTimeout,
	EnvDashboardStartTimeout,
	EnvSnapshotLocation,
	EnvDatabase,
	EnvWorkspaceProfile,
	EnvDisplayWidth,
	EnvCacheEnabled,
	EnvCacheTTL,
	EnvCacheMaxTTL,
	EnvCacheMaxSize,
	EnvQueryTimeout,
	EnvConnectionWatcher,
	EnvWorkspaceChDir,
	EnvModLocation,
	EnvTelemetry,
	EnvIntrospection,
	EnvWorkspaceProfileLocation,
	EnvMemoryMaxMb,
	EnvMemoryMaxMbPlugin,
	EnvConfigPath,
	EnvLogLevel,
	EnvGitToken,
	EnvPipesToken,
	EnvProfile string

func SetAppSpecificEnvVarKeys(envAppPrefix string) {
	// set prefix
	EnvAppPrefix = envAppPrefix

	EnvUpdateCheck = buildEnv("UPDATE_CHECK")
	EnvInstallDir = buildEnv("INSTALL_DIR")
	EnvInstallDatabase = buildEnv("INITDB_DATABASE_NAME")
	EnvServicePassword = buildEnv("DATABASE_PASSWORD")
	EnvMaxParallel = buildEnv("MAX_PARALLEL")
	EnvDatabaseStartTimeout = buildEnv("DATABASE_START_TIMEOUT")
	EnvDashboardStartTimeout = buildEnv("DASHBOARD_START_TIMEOUT")
	EnvSnapshotLocation = buildEnv("SNAPSHOT_LOCATION")
	EnvDatabase = buildEnv("DATABASE")
	EnvWorkspaceProfile = buildEnv("WORKSPACE")
	EnvDisplayWidth = buildEnv("DISPLAY_WIDTH")
	EnvCacheEnabled = buildEnv("CACHE")
	EnvCacheTTL = buildEnv("CACHE_TTL")
	EnvCacheMaxTTL = buildEnv("CACHE_MAX_TTL")
	EnvCacheMaxSize = buildEnv("CACHE_MAX_SIZE_MB")
	EnvQueryTimeout = buildEnv("QUERY_TIMEOUT")
	EnvConnectionWatcher = buildEnv("CONNECTION_WATCHER")
	EnvWorkspaceChDir = buildEnv("WORKSPACE_CHDIR")
	EnvModLocation = buildEnv("MOD_LOCATION")
	EnvTelemetry = buildEnv("TELEMETRY")
	EnvIntrospection = buildEnv("INTROSPECTION")
	EnvWorkspaceProfileLocation = buildEnv("WORKSPACE_PROFILES_LOCATION")
	EnvMemoryMaxMb = buildEnv("MEMORY_MAX_MB")
	EnvMemoryMaxMbPlugin = buildEnv("PLUGIN_MEMORY_MAX_MB")
	EnvConfigPath = buildEnv("CONFIG_PATH")
	EnvLogLevel = buildEnv("LOG_LEVEL")
	EnvGitToken = buildEnv("GIT_TOKEN")
	EnvPipesToken = buildEnv("PIPES_TOKEN")
	EnvProfile = buildEnv("PROFILE")
}

// buildEnv is a function to construct an application specific env var key
func buildEnv(suffix string) string {
	return EnvAppPrefix + suffix
}
