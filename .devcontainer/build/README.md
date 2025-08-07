# Steampipe PostgreSQL FDW Dev Container Build

This directory contains the build configuration for the Steampipe PostgreSQL FDW development container image.

## Building

```bash
# Build and push to registry (requires push permissions)
make build VERSION=1.0.0

# Build locally for testing
make build-local VERSION=1.0.0

# Test the built image
make test VERSION=1.0.0
```

## Publishing

The built image is published to `ghcr.io/turbot/steampipe-postgres-fdw-devcontainer:VERSION` and referenced in the main `devcontainer.json`. 