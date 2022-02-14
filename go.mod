module github.com/turbot/steampipe-postgres-fdw

go 1.16

require (
	github.com/dgraph-io/ristretto v0.1.0
	github.com/golang/protobuf v1.5.2
	github.com/hashicorp/go-hclog v1.1.0
	github.com/hashicorp/go-version v1.4.0
	github.com/turbot/go-kit v0.3.0
	github.com/turbot/steampipe v0.12.3-rc.0
	github.com/turbot/steampipe-plugin-sdk/v3 v3.0.0-rc.2
	google.golang.org/protobuf v1.27.1
)

// sdk_version_3
replace github.com/turbot/steampipe => github.com/turbot/steampipe v1.7.0-rc.0.0.20220214151300-53835e0dcb87
