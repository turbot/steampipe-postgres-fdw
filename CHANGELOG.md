## v0.0.41 [2021-05-29]
_Bug fixes_
*  For plugins using sdk > 0.3.0, `get` quals were not being taken into account when building cache key. ([#60](https://github.com/turbot/steampipe-postgres-fdw/issues/60))

## v0.0.40 [2021-05-28]
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

