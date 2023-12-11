DROP EXTENSION IF EXISTS steampipe_postgres_fdw_aws CASCADE;
CREATE EXTENSION IF NOT EXISTS steampipe_postgres_fdw_aws;
DROP SERVER IF EXISTS steampipe_aws;
CREATE SERVER steampipe_aws FOREIGN DATA WRAPPER steampipe_postgres_fdw_aws
DROP SCHEMA IF EXISTS aws CASCADE;
CREATE SCHEMA aws;
COMMENT ON SCHEMA aws IS 'steampipe aws fdw';
GRANT USAGE ON SCHEMA aws TO steampipe_users;
GRANT SELECT ON ALL TABLES IN SCHEMA aws TO steampipe_users;
IMPORT FOREIGN SCHEMA aws FROM SERVER steampipe_aws INTO aws OPTIONS(config 'profile="morales-aaa"');
