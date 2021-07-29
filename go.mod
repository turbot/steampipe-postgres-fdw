module github.com/turbot/steampipe-postgres-fdw

go 1.15

require (
	github.com/dgraph-io/ristretto v0.0.3
	github.com/golang/protobuf v1.5.2
	github.com/hashicorp/go-hclog v0.15.0
	github.com/hashicorp/go-version v1.2.1
	github.com/turbot/go-kit v0.2.2-0.20210701162132-c7aef7d72757
	github.com/turbot/steampipe v0.7.1
	github.com/turbot/steampipe-plugin-sdk v1.3.1
	google.golang.org/protobuf v1.27.1
)

replace github.com/c-bata/go-prompt => github.com/turbot/go-prompt v0.2.6-steampipe.0.20210713132659-0208eda137bd
