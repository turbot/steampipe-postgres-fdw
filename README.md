<p align="center">
    <h1 align="center">Steampipe Postgres Foreign Data Wrapper (FDW)</h1>
</p>

<p align="center">
  <a aria-label="Steampipe logo" href="https://steampipe.io">
    <img src="https://steampipe.io/images/steampipe_logo_wordmark_padding.svg" height="28">
  </a>
  <a href="https://godoc.org/github.com/turbot/steampipe-postgres-fdw"><img src="https://img.shields.io/badge/go-documentation-blue.svg?style=flat-square" alt="Godoc" height=28></a>
  &nbsp;
  <a aria-label="License" href="LICENSE">
    <img alt="" src="https://img.shields.io/static/v1?label=license&message=AGPLv3&style=for-the-badge&labelColor=777777&color=F3F1F0">
  </a>
</p>

## Overview

The Steampipe Postgres Foreign Data Wrapper (FDW) is a PostgreSQL 12.0 extension that is used by Steampipe plugins to interface with Postgres. Similar to [Multicorn](https://github.com/Segfault-Inc/Multicorn) for Python, the Steampipe FDW simplifies writing foreign data wrappers in Go for use in plugins.

Steampipe uses a Postgres Foreign Data Wrapper to present data from external systems and services as database tables. The Steampipe Foreign Data Wrapper (FDW) provides a Postgres extension that allows Postgres to connect to external data in a standardized way. The Steampipe FDW does not directly interface with external systems, but instead relies on plugins to implement the API/provider specific code and return it in a standard format via gRPC. This approach simplifies extending Steampipe as the Postgres-specific logic is encapsulated in the FDW, and API and service specific code resides only in the plugin.

See the [Writing Plugins](https://steampipe.io/docs/develop/writing-plugins) guide to get started writing Steampipe plugins.

## Get involved

### Community

The Steampipe community can be found on [Slack](https://steampipe.io/community/join), where you can ask questions, voice ideas, and share your projects.

Our [Code of Conduct](https://github.com/turbot/steampipe/blob/main/CODE_OF_CONDUCT.md) applies to all Steampipe community channels.

### Contributing

Please see [CONTRIBUTING.md](https://github.com/turbot/steampipe/blob/main/CONTRIBUTING.md).

### Building the FDW

Make sure that you have the following installed in your system:
1. `Postgresql v14` 
1. `go`
1. `gcc` for Linux

> For instructions on how to install PostgreSQL, please visit: https://www.postgresql.org/download/
> 
> For instruction on how to install `golang`, please visit: https://go.dev/dl/

Steps:
1. Clone this repository onto your system
1. Change to the cloned directory
1. Run the following commands:
```
$ make
```

This will install the compiled FDW (`steampipe_postgres_fdw.so`) into the default Steampipe installation directory (`~/.steampipe`) - if it exists.

### License

This open source library is licensed under the [GNU Affero General Public License v3](https://opensource.org/licenses/AGPL-3.0).
