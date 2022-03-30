#!/usr/bin/env bash

DRAFT=$(gh api -X GET /repos/{owner}/{repo}/releases -F owner=turbot -F repo=steampipe --jq '.[].draft')
COUNT=$(echo "$DRAFT" | wc -l | tr -d ' ')
if [[ "$COUNT" == "1" ]]; then
  TAG=$(gh api -X GET /repos/{owner}/{repo}/releases -F owner=turbot -F repo=steampipe --jq '.[].tag_name')
  gh release upload ${TAG} ../build-Darwin/steampipe_postgres_fdw.so
else
  echo "contains more than 1 draft releases"
fi
