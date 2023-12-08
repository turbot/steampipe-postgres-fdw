#####
#
# Dockerfile for cross-compiling for Linux on MacOS
# Build the image with:
#   docker build --pull -f Dockerfile -t steampipe_fdw_builder:15 --build-arg="pg_version=15" .  
#
# Run with:
#   docker run -it --rm --name sp_fdw_builder  -v $(pwd):/tmp/ext steampipe_fdw_builder:15
#
#####

FROM ubuntu:focal

# We know that the FDW does not compile with PG12. 
# Use this so that the build fails if an ARG is not passed in.
# This is useful since we can use the same container definition for the SQLite builder as well
ARG pg_version=12
ARG go_repo="deb http://ppa.launchpad.net/longsleep/golang-backports/ubuntu bionic main"
ARG pg_repo="deb http://apt.postgresql.org/pub/repos/apt/ focal-pgdg main"

ENV PG_VERS=$pg_version
ENV GO_VERS=1.21

## for apt to be noninteractive
ARG DEBIAN_FRONTEND=noninteractive
ARG DEBCONF_NONINTERACTIVE_SEEN=true

RUN apt-get update 
RUN apt-get install -y --no-install-recommends apt-transport-https 
RUN apt-get install -y --no-install-recommends dirmngr
RUN apt-get install -y --no-install-recommends gnupg
RUN apt-get install -y --no-install-recommends curl 
RUN apt-get install -y --no-install-recommends ca-certificates

RUN mkdir -p /etc/apt/sources.list.d \
  && apt-key adv --keyserver keyserver.ubuntu.com --recv 56A3D45E \
  && apt-key adv --keyserver keyserver.ubuntu.com --recv E0C56BD4 \
  && echo $go_repo > /etc/apt/sources.list.d/golang.list \
  && echo $pg_repo > /etc/apt/sources.list.d/pgdb.list \
  && curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -

RUN apt-get update
RUN env DEBIAN_FRONTEND=noninteractive \
  apt-get install -y --no-install-recommends golang-${GO_VERS} \
  postgresql-${PG_VERS} postgresql-server-dev-${PG_VERS} libpq-dev wget build-essential \
  libgcc-7-dev \
  locales \
  tzdata \
  git \
  && rm -rf \
  /var/lib/apt/lists/* \
  /var/cache/debconf \
  /tmp/* \
  && apt-get clean

RUN ln -s /usr/lib/go-${GO_VERS}/bin/go /usr/bin/go
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en
ENV LC_ALL en_US.UTF-8

WORKDIR /tmp/ext
COPY  . /tmp/ext

RUN chown -R postgres:postgres /tmp/ext
