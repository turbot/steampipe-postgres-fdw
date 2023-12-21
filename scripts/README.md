# Installation Guide for Steampipe Postgres trivy FDW

This README provides instructions on how to set up the Steampipe Postgres trivy Foreign Data Wrapper (FDW) extension.

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
   DROP EXTENSION IF EXISTS steampipe_postgres_trivy CASCADE;

   -- Create the extension
   CREATE EXTENSION IF NOT EXISTS steampipe_postgres_trivy;

   -- Drop the server if it already exists
   DROP SERVER IF EXISTS steampipe_trivy;

   -- Create the foreign server
   -- To pass configuration, set it as an OPTION. eg: CREATE SERVER steampipe_trivy FOREIGN DATA WRAPPER steampipe_postgres_trivy OPTIONS (config 'you_config_here');
   CREATE SERVER steampipe_trivy FOREIGN DATA WRAPPER steampipe_postgres_trivy;

   -- Drop the schema if it already exists
   DROP SCHEMA IF EXISTS trivy CASCADE;

   -- Create the schema
   CREATE SCHEMA trivy;

   -- Add a comment to the schema
   COMMENT ON SCHEMA trivy IS 'steampipe trivy fdw';

   -- Import the foreign schema
   IMPORT FOREIGN SCHEMA trivy FROM SERVER steampipe_trivy INTO trivy;
   ```

## Post-Installation

After the installation, you should be able to use the Steampipe Postgres trivy FDW to query trivy data directly from your PostgreSQL database.

For more information on using the FDW, refer to the Steampipe Hub documentation https://hub.steampipe.io/plugins/turbot/trivy.
