## Overview

The Steampipe Postgres Foreign Data Wrapper (FDW) is a Postgres extension that translates APIs to foreign tables. It does not directly interface with external systems, but instead relies on plugins to implement API- or provider-specific code that returns data in a standard format via gRPC. See the [Writing Plugins](https://steampipe.io/docs/develop/writing-plugins) guide to get started writing Steampipe plugins.

The FDW is part of the [Steampipe project](https://github.com/turbot/steampipe). Bundled with the Steampipe CLI, it works with one or more of the [plugins](https://hub.steampipe.io/plugins) you install in Steampipe. You can also [install](https://steampipe.io/docs/steampipe_postgres/install) one or more plugin-specific extensions in your own instance of Postgres.

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

This will compile the FDW (`steampipe_postgres_fdw.so`) along with the `control` and `sql` file in the `build-$PLATFORM` directory. This will install the compiled FDW into the default Steampipe installation directory (`~/.steampipe`) - if it exists.

### Building the FDW as a standalone extension

To build the FDW for one particular plugin, and run it as a standalone extension in any PostgreSQL database without relying on Steampipe:

Make sure that you have the following installed in your system:
1. `Postgresql v14` 
1. `go`
1. `gcc` for Linux

Steps:
1. Clone this repository onto your system
1. Change to the cloned directory
1. Run the following commands:
```
$ make standalone plugin="<plugin alias>"
```
Replace plugin alias with the alias or short name of your plugin.

This command will compile the FDW specifically for the chosen plugin, and the resulting binary, control file, and SQL files will be generated.

#### Example

Suppose you want to build the FDW for a plugin with an alias `aws` from a GitHub repository located at https://github.com/turbot/steampipe-plugin-aws. You would run the following command:
```
$ make standalone plugin="aws"
```

### License

This open-source library is licensed under the [GNU Affero General Public License v3](https://opensource.org/licenses/AGPL-3.0).

