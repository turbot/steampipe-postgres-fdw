#!/bin/sh

set -e

main() {
  # ANSI escape code variables
  BOLD=$(tput bold)
  NORMAL=$(tput sgr0)

  if ! command -v tar >/dev/null; then
    echo "Error: 'tar' is required." 1>&2
    exit 1
  fi

  OS=$(uname -s)
  if [ "$OS" = "Windows_NT" ]; then
    echo "Error: Windows is not supported yet." 1>&2
    exit 1
  else
    UNAME_SM=$(uname -sm)
    case "$UNAME_SM" in
    "Darwin x86_64") target="darwin_amd64.tar.gz" ;;
    "Darwin arm64") target="darwin_arm64.tar.gz" ;;
    "Linux x86_64") target="linux_amd64.tar.gz" ;;
    "Linux aarch64") target="linux_arm64.tar.gz" ;;
    *) echo "Error: '$UNAME_SM' is not supported yet." 1>&2; exit 1 ;;
    esac
  fi

  # Check if plugin is provided as an argument
  if [ $# -eq 0 ] || [ -z "$1" ]; then
    printf "Enter the plugin name: "
    read plugin
  else
    plugin=$1
  fi

  # Check if version is provided as an argument
  if [ $# -lt 2 ] || [ -z "$2" ]; then
    printf "Enter the version (latest): "
    read version
    version=${version:-latest}  # Default to 'latest' if input is empty
  else
    version=$2
  fi

  # Locate the PostgreSQL installation directory and version
  PG_CONFIG=$(command -v pg_config)
  if [ -z "$PG_CONFIG" ]; then
      echo "Warning: 'pg_config' was not found in your PATH."
      printf "Please enter the full path to your PostgreSQL installation directory (e.g., /usr/lib/postgresql/14): "
      read PG_DIR
      PG_CONFIG="${PG_DIR%/}/bin/pg_config"
      
      if [ ! -x "$PG_CONFIG" ]; then
          echo "Error: 'pg_config' could not be found in the provided directory." 1>&2
          exit 1
      fi
  fi

  # Extract the major version number from the PostgreSQL version and PG_DIR
  get_postgresql_details $plugin
  echo ""

  # Prompt the user to confirm the installation of the FDW for the detected version
  echo "Proceed with installing Steampipe PostgreSQL FDW for version $PG_VERSION at $PG_DIR?"
  echo "- Press 'y' to continue with the current version."
  printf -- "- Press 'n' to customize your PostgreSQL installation directory and select a different version. (Y/n): "
  read REPLY

  echo
  if [ "$REPLY" = "n" ] || [ "$REPLY" = "N" ]; then
      echo ""
      printf "Please enter the full path to your PostgreSQL installation directory (e.g., /usr/lib/postgresql/14): "
      read PG_DIR

      # Check if the input is empty
      if [ -z "$PG_DIR" ]; then
        echo ""
        echo "Error: No path entered. Exiting script." 1>&2
        exit 1
      fi

      PG_CONFIG="${PG_DIR%/}/bin/pg_config"

      if [ ! -x "$PG_CONFIG" ]; then
          echo ""
          echo "Error: 'pg_config' could not be found in the provided directory." 1>&2
          exit 1
      fi

      # Extract the major version number from the PostgreSQL version and PG_DIR
      get_postgresql_details $plugin
  fi

  asset_name="steampipe_postgres_${plugin}.pg${PG_VERSION}.${target}"

  # Generate the URI for the FDW
  if [ "$version" = "latest" ]; then
    uri="https://github.com/turbot/steampipe-plugin-${plugin}/releases/latest/download/${asset_name}"
  else
    uri="https://github.com/turbot/steampipe-plugin-${plugin}/releases/download/${version}/${asset_name}"
  fi

  echo ""
  echo "Downloading ${asset_name}..."
  if ! curl --fail --location --progress-bar --output ${asset_name} "$uri"; then
    echo "Could not find version $version"
    exit 1
  fi

  # If the .gz file is expected to contain a tar archive then extract it
  tar -xzvf $asset_name

  # Remove the downloaded tar.gz file
  rm -f $asset_name

  echo ""
  echo "Download and extraction completed."
  echo ""
  echo "Installing steampipe_postgres_${plugin} in ${BOLD}$PG_DIR${NORMAL}..."
  echo ""
  # Get the name of the extracted directory
  ext_dir=$(echo $asset_name | sed 's/\.tar\.gz$//')
  cd $ext_dir

  # Get directories from pg_config
  LIBDIR=$($PG_CONFIG --pkglibdir)
  EXTDIR=$($PG_CONFIG --sharedir)/extension/

  # Copy the files to the PostgreSQL installation directory
  cp steampipe_postgres_${plugin}.so "$LIBDIR"
  cp steampipe_postgres_${plugin}.control "$EXTDIR"
  cp steampipe_postgres_${plugin}--1.0.sql "$EXTDIR"

  # Check if the files were copied correctly
  if [ $? -eq 0 ]; then
    echo "Successfully installed steampipe_postgres_${plugin} extension!"
    echo ""
    echo "Files have been copied to:"
    echo "- Library directory: ${LIBDIR}"
    echo "- Extension directory: ${EXTDIR}"
    cd ../
    rm -rf $ext_dir
  else
    echo "Failed to install steampipe_postgres_${plugin} extension. Please check permissions and try again."
    exit 1
  fi
}

# Extract the PostgreSQL version and directory
get_postgresql_details() {
  PG_VERSION_FULL=$($PG_CONFIG --version)
  PG_VERSION=$(echo $PG_VERSION_FULL | awk '{print $2}' | awk -F '.' '{print $1}')
  PG_DIR=$($PG_CONFIG --bindir)
  PG_DIR=${PG_DIR%/bin}
  PLUGIN=$1

  echo ""
  echo "Discovered:"
  echo "- PostgreSQL version:   ${BOLD}$PG_VERSION${NORMAL}"
  echo "- PostgreSQL location:  ${BOLD}$PG_DIR${NORMAL}"
  echo "- Operating system:     ${BOLD}$(uname -s)${NORMAL}"
  echo "- System architecture:  ${BOLD}$(uname -m)${NORMAL}"
  echo ""
  check_postgresql_version
  echo "Based on the above, ${BOLD}steampipe_postgres_${PLUGIN}.pg${PG_VERSION}.${target}${NORMAL} will be downloaded, extracted and installed at: ${BOLD}$PG_DIR${NORMAL}"
}

check_postgresql_version(){
  # Check if the PostgreSQL version is supported(either 14 or 15)
  if [ "$PG_VERSION" != "14" ] && [ "$PG_VERSION" != "15" ]; then
    echo "Error: PostgreSQL version $PG_VERSION is not supported. Only versions 14 and 15 are supported." >&2
    return 1
  fi
}

# Call the main function to run the script
main "$@"
