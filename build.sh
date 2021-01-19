#!/usr/bin/env bash
cd fdw
make clean
make go
make
make inst
cd -

#pg_ctl -D /usr/local/var/postgres restart
#psql postgres
