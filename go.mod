module github.com/turbot/steampipe-postgres-fdw

go 1.16

require (
	github.com/dgraph-io/ristretto v0.0.3
	github.com/golang/protobuf v1.5.2
	github.com/hashicorp/go-hclog v0.15.0
	github.com/hashicorp/go-plugin v1.4.1 // indirect
	github.com/hashicorp/go-version v1.3.0
	github.com/turbot/go-kit v0.3.0
	// main
	github.com/turbot/steampipe v1.7.0-rc.0.0.20211018162653-6c21a742dad2
	github.com/turbot/steampipe-plugin-sdk v1.7.0
	google.golang.org/protobuf v1.27.1
)

replace github.com/c-bata/go-prompt => github.com/turbot/go-prompt v0.2.6-steampipe.0.20210830083819-c872df2bdcc9

// plugin_manager
replace github.com/turbot/steampipe => github.com/turbot/steampipe v1.7.0-rc.0.0.20211027131410-d699e782c422

// plugin_manager
replace github.com/turbot/steampipe-plugin-sdk => github.com/turbot/steampipe-plugin-sdk v1.7.1-0.20211027131410-ccbf69760446
