/* fdw-c/steampipe_postgres_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION fdw" to load this extension. \quit

CREATE FUNCTION steampipe_trivy_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION steampipe_trivy_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER steampipe_postgres_trivy
  HANDLER steampipe_trivy_fdw_handler
  VALIDATOR steampipe_trivy_fdw_validator;
