module github.com/turbot/steampipe-postgres-fdw

go 1.16

require (
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/dgraph-io/ristretto v0.1.0
	github.com/golang/glog v1.0.0 // indirect
	github.com/golang/protobuf v1.5.2
	github.com/hashicorp/go-hclog v0.15.0
	github.com/hashicorp/go-version v1.3.0
	github.com/turbot/go-kit v0.3.0
	// main
	github.com/turbot/steampipe v1.7.0-rc.0.0.20211018162653-6c21a742dad2
	github.com/turbot/steampipe-plugin-sdk v1.8.0
	golang.org/x/sys v0.0.0-20211102061401-a2f17f7b995c // indirect
	google.golang.org/protobuf v1.27.1
)

replace github.com/c-bata/go-prompt => github.com/turbot/go-prompt v0.2.6-steampipe.0.20210830083819-c872df2bdcc9

// main
replace github.com/turbot/steampipe => github.com/turbot/steampipe v0.10.0-dev.2

