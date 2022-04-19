#!/usr/bin/env bash
cd fdw
make clean
make go
make
make inst
cd -

#pg_ctl -D /usr/local/var/postgres restart
#psql postgres
cp ./build-Darwin/steampipe_postgres_fdw--1.0.sql ~/.steampipe/db/14.2.0/postgres/share/postgresql/extension/
cp ./build-Darwin/steampipe_postgres_fdw.control ~/.steampipe/db/14.2.0/postgres/share/postgresql/extension/
cp ./build-Darwin/steampipe_postgres_fdw.so ~/.steampipe/db/14.2.0/postgres/lib/postgresql/