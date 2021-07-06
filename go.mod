module github.com/turbot/steampipe-postgres-fdw

go 1.15

require (
	github.com/dgraph-io/ristretto v0.0.3
	github.com/golang/protobuf v1.4.3
	github.com/hashicorp/go-hclog v0.15.0
	github.com/hashicorp/go-version v1.2.1
	github.com/turbot/go-kit v0.2.2-0.20210701162132-c7aef7d72757
	github.com/turbot/steampipe v0.6.1-0.20210701133655-3ff8eeddc56b // main
	//github.com/turbot/steampipe v0.6.1-0.20210702114446-b8a439dd3ada // add_support_for_connection_groups
	github.com/turbot/steampipe-plugin-sdk v0.3.0-rc.0.0.20210706124136-8e4ec59e90ef // main
	google.golang.org/protobuf v1.25.0
)

replace github.com/c-bata/go-prompt => github.com/binaek89/go-prompt v0.2.7-multiline-clearscreen
