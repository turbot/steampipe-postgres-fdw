module github.com/turbot/steampipe-postgres-fdw

go 1.15

require (
	github.com/dgraph-io/ristretto v0.0.3
	github.com/golang/protobuf v1.4.3
	github.com/hashicorp/go-hclog v0.15.0
	github.com/hashicorp/go-version v1.2.1
	github.com/turbot/go-kit v0.2.2-0.20210628165333-268ba0a30be3
	github.com/turbot/steampipe v0.6.1-0.20210629133405-2643f0cabf98 // main
	github.com/turbot/steampipe-plugin-sdk v0.3.0-rc.0.0.20210630121447-d3b3374a82fe //key_columns_should_allow_specifying_supported_operators_121
	google.golang.org/protobuf v1.25.0
)

replace github.com/c-bata/go-prompt => github.com/binaek89/go-prompt v0.2.7-multiline-clearscreen

//replace github.com/turbot/steampipe => /Users/kai/Dev/github/turbot/steampipe
