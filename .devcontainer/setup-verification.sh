#!/bin/bash

# Steampipe PostgreSQL FDW Dev Container Setup Verification
# This script verifies that all required tools and dependencies are properly installed

echo "🔍 Steampipe PostgreSQL FDW Development Environment Verification"
echo "================================================================="

# Function to check if command exists
check_command() {
    if command -v "$1" >/dev/null 2>&1; then
        echo "✅ $1 is installed"
        return 0
    else
        echo "❌ $1 is NOT installed"
        return 1
    fi
}

# Function to check version
check_version() {
    local cmd="$1"
    local version_cmd="$2"
    echo "📋 $cmd version: $($version_cmd 2>/dev/null || echo 'Unable to determine version')"
}

echo ""
echo "🔧 Checking core development tools..."
check_command "go" && check_version "Go" "go version"
check_command "gcc" && check_version "GCC" "gcc --version | head -n1"
check_command "make" && check_version "Make" "make --version | head -n1"
check_command "pg_config" && check_version "PostgreSQL" "pg_config --version"

echo ""
echo "📦 Checking PostgreSQL development components..."
PG_INCLUDEDIR=$(pg_config --includedir 2>/dev/null)
PG_INCLUDEDIR_SERVER=$(pg_config --includedir-server 2>/dev/null)

if [ -d "$PG_INCLUDEDIR" ]; then
    echo "✅ PostgreSQL include directory exists: $PG_INCLUDEDIR"
else
    echo "❌ PostgreSQL include directory missing: $PG_INCLUDEDIR"
fi

if [ -d "$PG_INCLUDEDIR_SERVER" ]; then
    echo "✅ PostgreSQL server include directory exists: $PG_INCLUDEDIR_SERVER"
else
    echo "❌ PostgreSQL server include directory missing: $PG_INCLUDEDIR_SERVER"
fi

echo ""
echo "🏗️  Checking build environment..."
check_command "rsync"
check_command "gettext"
check_command "git"

echo ""
echo "🔍 Go environment check..."
echo "📋 GOPATH: ${GOPATH:-'Not set'}"
echo "📋 GOROOT: ${GOROOT:-'Not set'}"
echo "📋 PATH includes Go: $(echo $PATH | grep -q go && echo 'Yes' || echo 'No')"

# Check Go tools
# echo ""
# echo "🛠️  Checking Go development tools..."
# check_command "gopls"
# check_command "dlv"
# check_command "staticcheck"

echo ""
echo "📁 Checking workspace permissions..."
if [ -w "/workspace" ]; then
    echo "✅ Workspace is writable"
else
    echo "❌ Workspace is not writable"
fi

echo ""
echo "🧪 Testing basic build preparation..."
cd /workspace

# Test prebuild.go generation
if make prebuild.go >/dev/null 2>&1; then
    echo "✅ prebuild.go generation works"
    rm -f prebuild.go prebuild.go.bak
else
    echo "❌ prebuild.go generation failed"
fi

echo ""
echo "📋 Environment Summary:"
echo "   • Container user: $(whoami)"
echo "   • Working directory: $(pwd)"
echo "   • PostgreSQL version: $(pg_config --version 2>/dev/null | cut -d' ' -f2 || echo 'Unknown')"
echo "   • Go version: $(go version 2>/dev/null | cut -d' ' -f3 || echo 'Unknown')"
echo "   • Platform: $(uname -s)"

echo ""
echo "🚀 Ready to build! Try these commands:"
echo "   make build                    # Build the generic FDW"
echo "   make clean                    # Clean build artifacts"
echo "   fdw-help                      # Show help information"
echo ""
echo "📚 For plugin-specific builds:"
echo "   make standalone plugin=aws plugin_github_url=github.com/turbot/steampipe-plugin-aws"
echo ""
echo "=================================================================" 