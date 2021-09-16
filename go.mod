module github.com/turbot/steampipe-postgres-fdw

go 1.15

require (
	github.com/dgraph-io/ristretto v0.0.3
	github.com/golang/protobuf v1.5.2
	github.com/hashicorp/go-hclog v0.15.0
	github.com/hashicorp/go-version v1.3.0
	github.com/turbot/go-kit v0.2.2-0.20210730122803-1ecb35c27e98
	github.com/turbot/steampipe v0.7.1
	github.com/turbot/steampipe-plugin-sdk v1.3.1
	google.golang.org/protobuf v1.27.1
)

replace github.com/c-bata/go-prompt => github.com/turbot/go-prompt v0.2.6-steampipe.0.20210713132659-0208eda137bd

// sp_metadata
replace github.com/turbot/steampipe => github.com/turbot/steampipe v1.7.0-rc.0.0.20210916155011-a3f3a5206dcd

// create_tables
replace github.com/turbot/steampipe-plugin-sdk => github.com/turbot/steampipe-plugin-sdk v1.5.1-0.20210916104843-083998731780
