module github.com/turbot/steampipe-postgres-fdw

go 1.16

require (
	github.com/dgraph-io/ristretto v0.1.0
	github.com/golang/protobuf v1.5.2
	github.com/hashicorp/go-hclog v0.15.0
	github.com/hashicorp/go-version v1.3.0
	github.com/mxschmitt/golang-combinations v1.1.0
	github.com/turbot/go-kit v0.3.0
	// startup_opt
	github.com/turbot/steampipe v1.7.0-rc.0.0.20211130124439-0985ab2993bd
	github.com/turbot/steampipe-plugin-sdk v1.8.0
	google.golang.org/protobuf v1.27.1
)

replace github.com/turbot/steampipe => /Users/kai/Dev/github/turbot/steampipe
