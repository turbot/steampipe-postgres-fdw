#!/bin/sh

set -e

list=(
"steampipe-plugin-$1"
)

if [[ ! ${MY_PATH} ]];
then
  MY_PATH="`dirname \"$0\"`"              # relative
  MY_PATH="`( cd \"$MY_PATH\" && pwd )`"  # absolutized and normalized
fi

echo $MY_PATH

mkdir $MY_PATH/go-cache || true
mkdir $MY_PATH/go-mod-cache || true

GOCACHE=$(docker run --memory="10g" --memory-swap="16g" --rm --name sp_sqlite_builder steampipe_qlite_builder:latest go env GOCACHE)
MODCACHE=$(docker run --memory="10g" --memory-swap="16g" --rm --name sp_sqlite_builder steampipe_qlite_builder:latest go env GOMODCACHE)

for item in "${list[@]}"; do
  # Extract the plugin name (last word after the hyphen)
  plugin_name=${item##*-}

  echo "Processing plugin: ${plugin_name}"

  # Step 1: Switch to steampipe-sqlite-extension directory
  cd /Users/pskrbasu/turbot-delivery/Steampipe/steampipe-sqlite-extension || exit 1

  # Step 2: Run Docker commands for SQLite Builder
  docker run --memory="10g" --memory-swap="16g" --rm --name sp_sqlite_builder -v "$(pwd)":/tmp/ext steampipe_qlite_builder make build plugin="${plugin_name}"

  # Step 3: Tar the SQLite extension
  tar -czvf steampipe_sqlite_${plugin_name}.linux_arm64.tar.gz steampipe_sqlite_${plugin_name}.so

  # Final Check: If SQLite tar.gz file is created
  if [ ! -f "steampipe_sqlite_${plugin_name}.linux_arm64.tar.gz" ]; then
    echo "Error: Tar file for SQLite not created."
    exit 1
  fi

  echo "Processing completed for plugin: ${plugin_name}"
done

echo "All plugins processed successfully."