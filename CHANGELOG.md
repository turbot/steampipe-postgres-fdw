## v2.1.1 [2025-08-14]

_Bug fixes_
- **v2+ Plugin Version Support**: Fixed import path generation in templates to correctly handle v2+ plugin versions. The build process now properly appends the major version to the module path (e.g., `github.com/turbot/steampipe-plugin-aws/v2` for v2.x.x plugins) instead of using the base path which was causing build failures.


## v2.1.0 [2025-07-09]
_Whats new_
- Bump module to v2 and update Go version to 1.24.

_Bug fixes_
- Fix build failure caused by incorrect %s format verb for metadataCount (int). ([#566](https://github.com/turbot/steampipe-postgres-fdw/pull/566))

## v2.0.0 [2025-06-11]
_Breaking changes_
- Increased minimum required `glibc` version to `2.34` due to upgrading the Linux build environment from Ubuntu 20.04 to Ubuntu 22.04 GitHub runners. As a result, the FDW no longer supports older Linux distributions such as Ubuntu 20.04 and Amazon Linux 2.

_Whats new_
- Allow using `pprof` on FDW when `STEAMPIPE_FDW_PPROF` environment variable is set. ([#368](https://github.com/turbot/steampipe-postgres-fdw/issues/368))

_Bug fixes_
- Fix issue where the FDW did not correctly provide planning cost information for key-columns with an `any-of` requirement. This could lead the Postgres planner to choose query plans that do not include filters on those columns, even when filters were present in the query. . ([#558](https://github.com/turbot/steampipe-postgres-fdw/issues/558))

_Dependencies_
- Upgrade `go-jose/v4` to remediate vulnerabilities.

## v1.12.7 [2025-06-03]
_Dependencies_
- Upgrade `golang.org/x/net` to remediate high vulnerabilities.

## v1.12.6 [2025-05-26]
_Whats new_
- Add support to build steampipe standalone FDW for pre-release versions of plugins.

## v1.12.5 [2025-03-31]
_Dependencies_
- Upgrade `containerd` and `golang.org/x/net` to remediate moderate vulnerabilities.

## v1.12.4 [2025-02-11]
_Dependencies_
- Upgrade `pipe-fittings` to v2.1.1 and `go-kit` to v1.0.0.

## v1.12.3 [2025-02-05]
_Dependencies_
- Upgrade `pipe-fittings` to v1.6.8 and `steampipe-plugin-sdk` to v5.11.2.

## v1.12.2 [2025-02-03]
_Dependencies_
- Upgrade `crypto`, `net` and `go-git` packages to remediate critical and high vulnerabilities.

## v1.12.1 [2024-11-20]
_Bug fixes_
* Fix issue where steampipe interactive metacommand `.cache clear` was not clearing the cache. ([#520](https://github.com/turbot/steampipe-postgres-fdw/issues/520))

## v1.12.0 [2024-10-22]
_Whats new_
* Build FDW for Steampipe v1.0.0. ([#515](https://github.com/turbot/steampipe-postgres-fdw/issues/515))

## v1.11.8 [2024-09-05]
_Bug fixes_
* Fix issue where credentials from import foreign schema were lost after restarting session. ([#504](https://github.com/turbot/steampipe-postgres-fdw/issues/504))

## v1.11.7 [2024-08-30]
_Whats new_
* Update steampipe commit reference to include new HCL config to make plugin startup timeout configurable. ([#499](https://github.com/turbot/steampipe-postgres-fdw/issues/499))

## v1.11.6 [2024-08-13]
_Whats new_
* Compiled with Go 1.22. ([#485](https://github.com/turbot/steampipe-postgres-fdw/issues/485))

## v1.11.5 [2024-08-02]
_Bug fixes_
* Fix caching in the standalone plugin FDW extensions. ([#480](https://github.com/turbot/steampipe-postgres-fdw/issues/480))

## v1.11.4 [2024-07-17]
_Bug fixes_
* Fixed the steampipe commit reference for the issue fixed in v1.11.3.

## v1.11.3  [2024-06-06]
_Bug fixes_
* Update CreateConnectionPlugins to do not abort when unrelated connections are not yet loaded. ([#474](https://github.com/turbot/steampipe-postgres-fdw/issues/474))

* ## v1.11.2  [2024-05-17]
_Bug fixes_
* Removed unnecessary NOTICE level log messages. ([#469](https://github.com/turbot/steampipe-postgres-fdw/issues/469))

## v1.11.2  [2024-05-17]
_Bug fixes_
* Removed unnecessary NOTICE level log messages. ([#469](https://github.com/turbot/steampipe-postgres-fdw/issues/469))

## v1.11.1  [2024-05-11]
_Bug fixes_
* Republish to fix bad Linux Arm build.

## v1.11.0  [2024-04-03]
_Whats new_
* Add support for pushing down sort order. ([#447](https://github.com/turbot/steampipe-postgres-fdw/issues/447))
* Update limit pushdown logic to push down the limit if all sort clauses are pushed down. ([#458](https://github.com/turbot/steampipe-postgres-fdw/issues/458))
* Update Steampipe timing output to show all scans for all connections. ([#439](https://github.com/turbot/steampipe-postgres-fdw/issues/439))
* Add support for WHERE column=val1 OR column=val2 OR column=val3...  ([#440](https://github.com/turbot/steampipe-postgres-fdw/issues/440))
* Add support for running plugins in-process. ([#383](https://github.com/turbot/steampipe-postgres-fdw/issues/383))
* Fixes issue where the install script fails if pg_config in not in users path. ([#404](https://github.com/turbot/steampipe-postgres-fdw/issues/404))
* Always build with netgo to fix runtime error `undefined symbol: __res_search` when building on ubuntu20. ([#450](https://github.com/turbot/steampipe-postgres-fdw/issues/450))

* _Bug fixes_
* Add signal handler for signal 16 to avoid FDW crash. ([#457](https://github.com/turbot/steampipe-postgres-fdw/issues/457))

## v1.10.0 [2024-03-04]
  _Whats new_
* If `STEAMPIPE_FDW_PARALLEL_SAFE` env var is set is set, mark FDW as PARALLEL SAFE to improve performance. ([#428](https://github.com/turbot/steampipe-postgres-fdw/issues/428))

## v1.9.3 [2024-02-09]
_Whats new_
*  Allow connecting to a local, insecure OpenTelemetry server when `STEAMPIPE_OTEL_INSECURE` environment variable is set. ([#419](https://github.com/turbot/steampipe-postgres-fdw/issues/419))

## v1.9.2 [2024-02-01]
_Bug fixes_
*  Override client cache setting to false if server cache is disabled. ([#414](https://github.com/turbot/steampipe-postgres-fdw/issues/414))

## v1.9.1 [2023-12-22]
_Whats new_
*  allow using pprof on FDW when STEAMPIPE_FDW_PPROF environment variable is set. ([#368](https://github.com/turbot/steampipe-postgres-fdw/issues/368))

_Bug fixes_
* Row count is incorrect when using aggregator connections.  ([#402](https://github.com/turbot/steampipe-postgres-fdw/issues/402))
* OpenTelemetry metric names must only contain `[A-Za-z0-9_.-]`.  ([#369](https://github.com/turbot/steampipe-postgres-fdw/issues/369))
* Update makefile to install into `STEAMPIPE_INSTALL_DIR` if set.

## v1.9.0 [2023-09-29]
_Whats new_
* Add ability to clear connection cache by inserting into `steampipe_settings` table. ([#360](https://github.com/turbot/steampipe-postgres-fdw/issues/360))

## v1.8.0 [2023-09-27]
_Bug fixes_
* Remove duplicate qual values for duplicate IN clauses. ([#353](https://github.com/turbot/steampipe-postgres-fdw/issues/353))
* Do not print stacks as it might contain sensitive infos. ([#316](https://github.com/turbot/steampipe-postgres-fdw/issues/316))
* Reload connection schema when callng importForeignSchema.  ([#358](https://github.com/turbot/steampipe-postgres-fdw/issues/358))
* If a connection config is not found, reload config. ([#356](https://github.com/turbot/steampipe-postgres-fdw/issues/356))

_ Deprecations_ 
* Removes support for plugins which do not have multi connection ability.([#332](https://github.com/turbot/steampipe-postgres-fdw/issues/332))

## v1.7.2 [2023-05-18]
_Whats new_
* Re-add support for legacy command-schema. ([#313](https://github.com/turbot/steampipe-postgres-fdw/issues/313))

## v1.7.1 [2023-05-18]
Rebuild to avoid Linux Arm build error

## v1.7.0 [2023-05-18]
* Add support for configuring 'cache' and 'cache_ttl' per instance. 
* Remove 'steampipe_command' schema and move settings and scan_metadata foreign tables to `steampipe_internal` schema. ([#310](https://github.com/turbot/steampipe-postgres-fdw/issues/310))

## v1.6.3 [2023-04-27]
_Bug fixes_
* Fix array bounds error when querying with an aggregator with no children. Show useful error instead. Closes #303. ([#303](https://github.com/turbot/steampipe-postgres-fdw/issues/303))

## v1.6.2 [2023-03-08]
_Bug fixes_
* Fix nil reference panic when a scan fails to start - do not add an iterator to `Hub.runningIterators` until scan is started successfully. ([#298](https://github.com/turbot/steampipe-postgres-fdw/issues/298))

## v1.6.1 [2023-03-02]
_Bug fixes_
* Fix build issue which caused failure to install FDW on Linux x86_64 systems. ([#295](https://github.com/turbot/steampipe-postgres-fdw/issues/295))

## v1.6.0 [2023-03-01]
_Whats new_
* Add support for dynamic aggregators. Pass connection name in `ExecuteRequest`, this is used to resolve aggregator config. ([#273](https://github.com/turbot/steampipe-postgres-fdw/issues/273))

_Bug fixes_
* Limit should not be pushed down if there are unconverted restrictions. ([#291](https://github.com/turbot/steampipe-postgres-fdw/issues/291))
  
## v1.5.0 [2022-11-30]
_Whats new_
* Update to work with sdk version 5 and dynamic updating of dynamic schemas. ([#259](https://github.com/turbot/steampipe-postgres-fdw/issues/259))

## v1.4.4 [2022-10-31]
_Bug fixes_
* Update GetPathKeys to treat key columns with `AnyOf` require property with the same precedence as `Required`. ([#254](https://github.com/turbot/steampipe-postgres-fdw/issues/254))

## v1.4.3 [2022-10-20]
* Add logging to import foreign schema.

## v1.4.2 [2022-09-26]
* Republish to fix inconsistently versioned Linux Arm build.

## v1.4.1 [2022-09-16]
_Bug fixes_
* Fix `double` qual values not being passed to plugin. ([#243](https://github.com/turbot/steampipe-postgres-fdw/issues/243))

## v1.4.0 [2022-09-09]
_Bug fixes_
* Do not start scan until the first time `IterateForeignScan` is called. Do not create an iterator in `StartForeignScan` if flag `EXEC_FLAG_EXPLAIN_ONLY` is set. ([#237](https://github.com/turbot/steampipe-postgres-fdw/issues/237))

## v1.3.2 [2022-08-23]
* Update referenced `steampipe-plugin-sdk` and `steampipe` version

## v1.3.1 [2022-08-09]
_Bug fixes_
* Ensure ConnectionPlugins (i.e. plugin GRPC clients) are cached by the hub. ([#230](https://github.com/turbot/steampipe-postgres-fdw/issues/230))

## v1.3.0 [2022-08-05]
_What's new?_
* Add support for a single plugin instance hosting multiple Steampipe connections, rather than an instance per connection. ([#226](https://github.com/turbot/steampipe-postgres-fdw/issues/226))

## v1.2.2 [2022-07-21]
_Bug fixes_
* Fix build issue which causes failure to load FDW on Arm Docker images. ([#219](https://github.com/turbot/steampipe-postgres-fdw/issues/219))

## v1.2.1 [2022-07-03]
_Bug fixes_
* Fix EOF error when joining multiple tables on the JSON qual column. Handle zero value jsonb quaLs. ([#201](https://github.com/turbot/steampipe-postgres-fdw/issues/201))
* Fix EOF error when joining multiple tables with jsonb_array_elements. ([#192](https://github.com/turbot/steampipe-postgres-fdw/issues/192))
* Fix panic when querying with json_array_elements_text. ([#207](https://github.com/turbot/steampipe-postgres-fdw/issues/207))

## v1.2.0 [2022-06-22]
_What's new?_
* Add support for Open Telemetry. ([#195](https://github.com/turbot/steampipe-postgres-fdw/issues/195))
* Update `.timing` output to return additional query metadata such as the number of hydrate functions called and the cache status. 
* Print FDW version in the logs.

_Bug fixes_
* Add recover blocks to all callback functions. ([#199](https://github.com/turbot/steampipe-postgres-fdw/issues/199))

## v1.1.0 [2022-05-20]
_What's new?_
* Add support for JSONB quals. ([#185](https://github.com/turbot/steampipe-postgres-fdw/issues/185))

_Bug fixes_
* Fix EOF errors and other query failures caused by invalid index in `columnFromVar`. ([#187](https://github.com/turbot/steampipe-postgres-fdw/issues/187))

## v1.0.0 [2022-05-09]
_What's new?_
* Add support for Postgres 14. ([#179](https://github.com/turbot/steampipe-postgres-fdw/issues/179))
* Update Go version to 1.18. ([#163](https://github.com/turbot/steampipe-postgres-fdw/issues/163))

_Bug fixes_
* Fix JSON data with \u0000 errors in Postgres with "unsupported Unicode escape sequence". ([#118](https://github.com/turbot/steampipe-postgres-fdw/issues/118))
* Escape quotes in all postgres object names. ([#178](https://github.com/turbot/steampipe-postgres-fdw/issues/178))

## v0.4.0 [2022-03-10]
_What's new?_
* Add support for ltree column type. ([#138](https://github.com/turbot/steampipe-postgres-fdw/issues/138))
* Add support for inet column type. ([#156](https://github.com/turbot/steampipe-postgres-fdw/issues/156))

* _Bug fixes_
* Fix refreshing an aggregate connection causing a plugin crash. ([#152](https://github.com/turbot/steampipe-postgres-fdw/issues/152))
* Fix 'is nil' qual causing a plugin NRE. ([#154](https://github.com/turbot/steampipe-postgres-fdw/issues/154))

## v0.3.5 [2022-08-02]
_Bug fixes_
* Fix FDW crash when failing to start a plugin because of a validation error. ([#146](https://github.com/turbot/steampipe-postgres-fdw/issues/146))

## v0.3.4 [2022-02-01]
_Bug fixes_
* Do not set connection config when creating connection plugin for a GetSchema call - but do set it otherwise

## v0.3.3 [2022-02-01]
_Bug fixes_
* Do not set connection config when creating connection plugin - as it will already have been set by Steampipe CLI. ([#139](https://github.com/turbot/steampipe-postgres-fdw/issues/139))

## v0.3.2 [2021-12-21]
_Bug fixes_
* Fixes issue where FDW log entries were using a different format from Postgres. ([#134](https://github.com/turbot/steampipe-postgres-fdw/issues/134))

## v0.3.1 [2021-12-21]
_What's new?_
* Update PathKeys code to give required key columns a lower cost than optional key columns  ([#116](https://github.com/turbot/steampipe-postgres-fdw/issues/116), [#117](https://github.com/turbot/steampipe-postgres-fdw/issues/117))

## v0.3.0 [2021-11-02]
_What's new?_
* Add support for plugin manager and plugin-level query caching. ([#111](https://github.com/turbot/steampipe-postgres-fdw/issues/111))
* Only create query cache if needed. Do not add data to cache if plugin supports caching. ([#119](https://github.com/turbot/steampipe-postgres-fdw/issues/119))

_Bug fixes_
* Avoid concurrency error when calling execute multiple times in parallel. ([#114](https://github.com/turbot/steampipe-postgres-fdw/issues/114))
* Fix intermittent crash when using boolean qual with ? operator and  jsonb column. ([#122](https://github.com/turbot/steampipe-postgres-fdw/issues/122))

## v0.2.6 [2021-10-18]
_What's new?_
* Update Timestamp columns to use "timestamp with time zone", not "timestamp". ([#94](https://github.com/turbot/steampipe-postgres-fdw/issues/94))

## v0.2.5 [2021-10-07]
_What's new?_
* Update Steampipe reference to fix connection config parsing if there is an options block. ([#993](https://github.com/turbot/steampipe/issues/993))

## v0.2.4 [2021-10-04]
_What's new?_
* Update Steampipe reference to support JSON connection config. ([#105](https://github.com/turbot/steampipe-postgres-fdw/issues/105))

_Bug fixes_
* Fix handling of null unicode chars in JSON fields. ([#102](https://github.com/turbot/steampipe-postgres-fdw/issues/102))
* Fix queries with `like` and `limit` clause not listing correct results. ([#103](https://github.com/turbot/steampipe-postgres-fdw/issues/103))
* Reload connection config from `GetRelSize` to ensure config changes are respected. ([#99](https://github.com/turbot/steampipe-postgres-fdw/issues/99))

## v0.2.3 [2021-09-10]
_Bug fixes_
* Fix null reference exception when evaluating certain null-test quals. ([#97](https://github.com/turbot/steampipe-postgres-fdw/issues/97))
* Add support for CIDROID type when converting Postgres datums to qual values. ([#54](https://github.com/turbot/steampipe-postgres-fdw/issues/54))

## v0.2.2 [2021-09-07]
_Bug fixes_
* Fix JSON data with '\u0000' resulting in Postgres error "unsupported Unicode escape sequence". ([#93](https://github.com/turbot/steampipe-postgres-fdw/issues/93))

## v0.2.1 [2021-08-18]
_Bug fixes_
* Restart a plugin if it has exited unexpectedly. ([#89](https://github.com/turbot/steampipe-postgres-fdw/issues/89))

## v0.2.0 [2021-08-03]
_What's new?_
* Support cache commands sent via SQL queries. ([#86](https://github.com/turbot/steampipe-postgres-fdw/issues/86))

## v0.1.0 [2021-07-22]
_What's new?_
* Add support for aggregator connections. ([#78](https://github.com/turbot/steampipe-postgres-fdw/issues/78))
* Construct cache key based on the columns returned by the plugin, not the columns requested. ([#82](https://github.com/turbot/steampipe-postgres-fdw/issues/82))

## v0.0.43 [2021-07-08]
_Bug fixes_
* Fix cache enabled logic reversed. ([#77](https://github.com/turbot/steampipe-postgres-fdw/issues/77))

## v0.0.42 [2021-07-07]
_What's new?_
* Add support for plugin sdk version 1.3. ([#70](https://github.com/turbot/steampipe-postgres-fdw/issues/70))
* Deparse limit from the query and set in QueryContext. ([#9](https://github.com/turbot/steampipe-postgres-fdw/issues/9))
* Query cache must take limit into account. Do not push down limit if the query refers to more than one table, or uses distinct clause.([#66](https://github.com/turbot/steampipe-postgres-fdw/issues/66))

_Bug fixes_
* Update extractRestrictions to handle BoolExpr so queries like `where column1 is false` work. ([#23](https://github.com/turbot/steampipe-postgres-fdw/issues/23))
* Fix plugin errors (e.g. missing key column qual) causing a freeze. ([#72](https://github.com/turbot/steampipe-postgres-fdw/issues/72))
* Update Timestamp column value conversion code to use RFC 3339 string value directly. ([#76](https://github.com/turbot/steampipe-postgres-fdw/issues/76))

## v0.0.41 [2021-06-21]
_Bug fixes_
*  For plugins using sdk > 0.3.0, `get` quals were not being taken into account when building cache key. ([#60](https://github.com/turbot/steampipe-postgres-fdw/issues/60))

## v0.0.40 [2021-06-17]
_What's new?_
* For plugins using sdk > 0.3.0, only use key columns quals when constructing cache key. ([#58](https://github.com/turbot/steampipe-postgres-fdw/issues/58))

_Bug fixes_
* Increase likelihood of join quals being passed to fdw by adding paths for all columns in combination with requires key columns. ([#47](https://github.com/turbot/steampipe-postgres-fdw/issues/47))
  
## v0.0.39 [2021-05-28]
* Fix issue with release build

## v0.0.38 [2021-05-28]
_What's new?_
* Update logging

## v0.0.37 [2021-05-28]
_Bug fixes_
* Ensure consistent ordering when building cache key from multiple quals. ([#53](https://github.com/turbot/steampipe-postgres-fdw/issues/53))

## v0.0.36 [2021-05-27]
_Bug fixes_
* Fix cache key built incorrectly when more than 1 qual used. ([#53](https://github.com/turbot/steampipe-postgres-fdw/issues/53))

## v0.0.35 [2021-05-25]
_What's new?_
* Add connection loading optimisation and support for active scan cancellation. ([#50](https://github.com/turbot/steampipe-postgres-fdw/issues/50))
* Change Steampipe Postgres FDW license to AGPLv3. ([#488](https://github.com/turbot/steampipe/issues/488))
* Update README formatting and license reference

_Bug fixes_
* Add support for query cancellation. ([#49](https://github.com/turbot/steampipe-postgres-fdw/issues/49))

## v0.0.34 [2021-05-18]
_Bug What_
* Add support for query cancellation. ([#49](https://github.com/turbot/steampipe-postgres-fdw/issues/49))

## v0.0.33 [2021-05-13]
_Bug fixes_
* Fix cache check code incorrectly identifying a cache hit after a count(*) query. ([#44](https://github.com/turbot/steampipe-postgres-fdw/issues/44))

## v0.0.32 [2021-05-06]
* Fix issue with release build

## v0.0.31 [2021-05-06]
_What's new?_
* Update steampipe-postgres-fdw and steampipe version

## v0.0.30 [2021-04-15]
_What's new?_
* Replace call to steampipeconfig.Load() with steampipeconfig.LoadSteampipeConfig("") to be compatible with Steampipe 0.4.0

## v0.0.29 [2021-03-19]
_Bug fixes_
* Fix crash when doing "is (not) null" checks on JSON fields. ([#38](https://github.com/turbot/steampipe-postgres-fdw/issues/38))

## v0.0.28 [2021-03-18]
_Bug fixes_
* Update steampipe reference to fix cache config precedence.

## v0.0.27 [2021-03-18]
_What's new?_
* Support cache configuration via Steampipe config. ([#22](https://github.com/turbot/steampipe-postgres-fdw/issues/22))

_Bug fixes_
* Fix various quals issues. ([#8](https://github.com/turbot/steampipe-postgres-fdw/issues/8))
* Fix timestamp quals. ([#37](https://github.com/turbot/steampipe-postgres-fdw/issues/37))

## v0.0.26 [2021-03-03]
_Bug fixes_
* Fix timestamp quals not working for key columns. ([#24](https://github.com/turbot/steampipe-postgres-fdw/issues/24))

## v0.0.25 [2021-02-22]
_Bug fixes_
* Add connection to cache key - fixes retrieving incorrect data for multi connection queries. ([#20](https://github.com/turbot/steampipe-postgres-fdw/issues/20))

## v0.0.24 [2021-02-17]
_What's new?_
* Disable caching by default. ([#18](https://github.com/turbot/steampipe-postgres-fdw/issues/18))

## v0.0.23 [2021-02-17]
_What's new?_
* Add support for connection config. ([#14](https://github.com/turbot/steampipe-postgres-fdw/issues/14))
* Add caching of query results ([#11](https://github.com/turbot/steampipe-postgres-fdw/issues/11))
* Update environment variables to use STEAMPIPE prefix. ([#13](https://github.com/turbot/steampipe-postgres-fdw/issues/13))

