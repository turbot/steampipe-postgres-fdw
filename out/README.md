# Steampipe Postgres FDW AWS

This Postgres Foreign Data Wrapper (FDW) allows you to query AWS data using the [Steampipe Plugin](https://github.com/turbot/steampipe-plugin-aws).

## Prerequisites
Before you can use this Postgres FDW, make sure you have the following prerequisites in place:

- You need to have a Postgres database installed and running. You can download and install PostgreSQL from the [official PostgreSQL website](https://www.postgresql.org/download/).

## Configuration
To set up the configuration for this PostgreSQL FDW, follow these steps:

- Download the SQL and Control extension files from the latest release of this FDW.

- Copy the downloaded SQL and Control files into your Postgres extensions directory and the downloaded binary file into your PostgreSQL lib directory:
```bash
cp steampipe_postgres_fdw_aws--1.0.sql /path/to/your/extensions/directory
cp steampipe_postgres_fdw_aws.control /path/to/your/extensions/directory
cp steampipe_postgres_fdw_aws.so /path/to/your/lib/directory
```

- Run the following SQL commands to create extensions and servers:
```sql
DROP EXTENSION IF EXISTS steampipe_postgres_fdw_aws CASCADE;
CREATE EXTENSION IF NOT EXISTS steampipe_postgres_fdw_aws;
DROP SERVER IF EXISTS steampipe_aws;
CREATE SERVER steampipe_aws FOREIGN DATA WRAPPER steampipe_postgres_fdw_aws;
DROP SCHEMA IF EXISTS aws CASCADE;
CREATE SCHEMA aws;
IMPORT FOREIGN SCHEMA aws FROM SERVER steampipe_aws INTO aws;
```
Once you have completed these steps, your PostgreSQL environment will be configured to work with the FDW. You can then proceed to use the FDW to query AWS data.

## Usage
Please refer to the [Table Documentation](https://hub.steampipe.io/plugins/turbot/aws/tables).
