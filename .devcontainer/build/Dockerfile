FROM mcr.microsoft.com/devcontainers/go:1-1.24-bookworm

ARG DEVCONTAINER_VERSION

LABEL org.opencontainers.image.ref.name="steampipe-postgres-fdw-devcontainer" \
  org.opencontainers.image.version="${DEVCONTAINER_VERSION}" \
  org.opencontainers.image.url="https://steampipe.io" \
  org.opencontainers.image.authors="Turbot HQ, Inc" \
  org.opencontainers.image.source="https://github.com/turbot/steampipe-postgres-fdw" \
  org.opencontainers.image.description="Development container for Steampipe PostgreSQL FDW"

# Install PostgreSQL and development dependencies
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
  postgresql-15 \
  postgresql-server-dev-15 \
  libpq-dev \
  build-essential \
  gcc \
  make \
  rsync \
  gettext \
  && echo 'vscode ALL=(ALL) NOPASSWD: ALL' >> /etc/sudoers \
  && systemctl disable postgresql || true \
  && rm -rf /var/lib/apt/lists/*

# Switch to vscode user for Go package installs
USER vscode

# Install common Go tools for FDW development
RUN 
# RUN go install golang.org/x/tools/gopls@latest \
#   && go install github.com/go-delve/delve/cmd/dlv@latest \
#   && go install honnef.co/go/tools/cmd/staticcheck@latest 