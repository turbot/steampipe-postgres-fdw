#!/usr/bin/env bash

DRAFT=$(gh api -X GET /repos/{owner}/{repo}/releases -F owner=turbot -F repo=steampipe --jq 'select(.[].draft == true) | .[].draft')
COUNT=$(echo "$DRAFT" | wc -l | tr -d ' ')
if [[ "$COUNT" == "1" ]]; then
  TAG=$(gh api -X GET /repos/{owner}/{repo}/releases -F owner=turbot -F repo=steampipe --jq '.[].tag_name')
  gzip steampipe_postgres_fdw.so
  mv steampipe_postgres_fdw.so.gz steampipe_postgres_fdw.so.darwin_arm64.gz
  gh release upload ${TAG} steampipe_postgres_fdw.so.darwin_arm64.gz
else
  echo "contains more than 1 draft releases"
fi
