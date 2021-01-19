/* fdw-c/steampipe_postgres_fdw--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION fdw" to load this extension. \quit

CREATE FUNCTION fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER steampipe_postgres_fdw
  HANDLER fdw_handler
  VALIDATOR fdw_validator;

-- CREATE SERVER "steampipe"
--     FOREIGN DATA WRAPPER steampipe_postgres_fdw;
