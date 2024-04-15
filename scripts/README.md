# Installation Guide for Steampipe Postgres net FDW

This README provides instructions on how to set up the Steampipe Postgres net Foreign Data Wrapper (FDW) extension.

## Prerequisites

Before proceeding with the installation, ensure that you have:

- Installed PostgreSQL on your system.
- Obtained the necessary permissions to create extensions, servers, and schemas in your PostgreSQL database.

## Installation Steps

1. Run the `install.sh` script to copy the binary files (`.so`, `.control`, and `.sql`) into your PostgreSQL installation directories.

   ```bash
   ./install.sh
   ```

2. Connect to your PostgreSQL database using your preferred method (e.g., psql command line tool).

3. Execute the following SQL commands to set up the extension:

   ```sql
   -- Drop the extension if it already exists
   DROP EXTENSION IF EXISTS steampipe_postgres_net CASCADE;

   -- Create the extension
   CREATE EXTENSION IF NOT EXISTS steampipe_postgres_net;

   -- Drop the server if it already exists
   DROP SERVER IF EXISTS steampipe_net;

   -- Create the foreign server
   -- To pass configuration, set it as an OPTION. eg: CREATE SERVER steampipe_net FOREIGN DATA WRAPPER steampipe_postgres_net OPTIONS (config 'you_config_here');
   CREATE SERVER steampipe_net FOREIGN DATA WRAPPER steampipe_postgres_net;

   -- Drop the schema if it already exists
   DROP SCHEMA IF EXISTS net CASCADE;

   -- Create the schema
   CREATE SCHEMA net;

   -- Add a comment to the schema
   COMMENT ON SCHEMA net IS 'steampipe net fdw';

   -- Import the foreign schema
   IMPORT FOREIGN SCHEMA net FROM SERVER steampipe_net INTO net;
   ```

## Post-Installation

After the installation, you should be able to use the Steampipe Postgres net FDW to query net data directly from your PostgreSQL database.

For more information on using the FDW, refer to the Steampipe Hub documentation https://hub.steampipe.io/plugins/turbot/net.
