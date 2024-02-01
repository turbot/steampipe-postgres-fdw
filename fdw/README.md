# Fdw

Fdw is a Postgres Foreign Data Wrapper interface written in Go.
Dynamic Foreign Tables are defined through gRPC plugins, making them
safe, performant and easy to build.


## Loading the fdw server and tables - Option 1

Each provider acts as a separate fdw server.

```
create server
  fdw_aws
foreign data wrapper
  fdw
options (
  wrapper 'aws'
);
```

```
create foreign table
  aws_acm_certificate (
    arn text,
    domain_name text
  )
server
  "fdw_aws"
options (
  table 'aws_acm_certificate'
);
```


## Loading the fdw server and tables - Option 2

A single fdw server loads various providers, and allows tables from in them
to be loaded specifically.

```
create server
  fdw
foreign data wrapper
  fdw
;
```

```
create foreign table
  aws_acm_certificate (
    arn text,
    domain_name text
  )
server
  "fdw"
options (
  table 'aws.aws_acm_certificate'
)
```

## Architecture

Fdw is a postgres foreign data wrapper. It is implemented in Go,
but tightly coupled with the Postgres C code.

Fdw then acts as a gRPC client, allowing tables to be plugged in
as extensions (servers). This interface deliberately hides the Postgres
internals, making it much faster and easier to implement tables. (Heavily
optimized implementations needing more access to Postgres internals should
consider forking Fdw.)

```
                                          +-----------+
                                      +-->| aws_*     |
                                      |   +-----------+
                                      |
+----------+     +-----------+  gRPC  |   +-----------+
| Postgres |=====| Fdw       |--------+-->| google_*  |
+----------+     +-----------+        |   +-----------+
                                      |
                                      |   +-----------+
                                      +-->| ...       |
                                          +-----------+
```


## Phases

1. Wrapper and plugin registration
2. Schema import
3. Query planning
4. Query execution



## Related

* Multicorn offers a python based FDW.
* [oracle_fdw](https://github.com/laurenz/oracle_fdw) is C-based.


* [Query planning in Foreign Data Wrappers](https://www.postgresql.org/docs/14/fdw-planning.html)
* [Parallel safety](https://www.postgresql.org/docs/14/parallel-safety.html)
* [FDW Routines for Parallel Execution](https://www.postgresql.org/docs/14/fdw-callbacks.html#FDW-CALLBACKS-PARALLEL)
