#!/bin/sh

set -e

# Desired PostgreSQL version
PG_VERSION="PostgreSQL 14.11 (Homebrew)
"
desired_pg_version=$(echo "$PG_VERSION" | sed 's/[^0-9]*\([0-9]\{1,\}\.[0-9]\{1,\}\).*/\1/' | cut -d'.' -f1)

main() {
  # ANSI escape code variables
  BOLD=$(tput bold)
  NORMAL=$(tput sgr0)

  attempt_count=0
  while true; do
    PG_CONFIG=$(command -v pg_config)

    if [ -z "$PG_CONFIG" ] || [ $attempt_count -gt 0 ]; then
      request_pg_config_path
    fi

    get_postgresql_details

    if [ "$PG_VERSION" != "$desired_pg_version" ]; then
      echo "Warning: Your pg_config points to version $PG_VERSION, but the desired version is $desired_pg_version." >&2
      display_discovered_details

      if [ $attempt_count -ge 1 ]; then
        echo "Error: The downloaded/built Postgres FDW is built for version $desired_pg_version. Your pg_config points to version $PG_VERSION."
        exit 1
      fi

      attempt_count=$((attempt_count + 1))
      continue
    fi

    display_discovered_details
    confirm_and_install
  done
}

request_pg_config_path() {
  echo "Please enter the full path to your PostgreSQL $desired_pg_version installation directory (e.g., /usr/lib/postgresql/$desired_pg_version): "
  read PG_DIR
  PG_CONFIG="${PG_DIR%/}/bin/pg_config"

  if [ ! -x "$PG_CONFIG" ]; then
    echo "Error: 'pg_config' could not be found in the provided directory." >&2
    exit 1
  fi
}

get_postgresql_details() {
  PG_VERSION_FULL=$("$PG_CONFIG" --version)
  PG_VERSION=$(echo "$PG_VERSION_FULL" | sed 's/[^0-9]*\([0-9]\{1,\}\.[0-9]\{1,\}\).*/\1/' | cut -d'.' -f1)
  PG_DIR=$("$PG_CONFIG" --bindir)
  PG_DIR=${PG_DIR%/bin}
}

display_discovered_details() {
  echo ""
  echo "Discovered:"
  echo "- PostgreSQL version:   ${BOLD}$PG_VERSION${NORMAL}"
  echo "- PostgreSQL location:  ${BOLD}$PG_DIR${NORMAL}"
  echo ""
}

confirm_and_install() {
  printf "Install Steampipe PostgreSQL FDW for version $PG_VERSION in $PG_DIR? (Y/n): "
  read REPLY
  echo

  if [ "$REPLY" = "y" ] || [ "$REPLY" = "Y" ] || [ -z "$REPLY" ]; then
    echo "Installing..."

    # Get directories from pg_config
    LIBDIR=$("$PG_DIR/bin/pg_config" --pkglibdir)
    EXTDIR=$("$PG_DIR/bin/pg_config" --sharedir)/extension/

    # Copy the files to the PostgreSQL installation directory
    cp steampipe_postgres_net.so "$LIBDIR"
    cp steampipe_postgres_net.control "$EXTDIR"
    cp steampipe_postgres_net--1.0.sql "$EXTDIR"

    # Check if the files were copied correctly
    if [ $? -eq 0 ]; then
      echo ""
      echo "Successfully installed steampipe_postgres_net extension!"
      echo ""
      echo "Files have been copied to:"
      echo "- Library directory: ${LIBDIR}"
      echo "- Extension directory: ${EXTDIR}"
    else
      echo "Failed to install steampipe_postgres_net extension. Please check permissions and try again."
      exit 1
    fi
    exit 0
  else
    echo ""
  fi
}

# Call the main function
main "$@"