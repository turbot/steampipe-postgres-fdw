module github.com/turbot/steampipe-postgres-fdw

go 1.15

require (
	github.com/dgraph-io/ristretto v0.0.3
	github.com/golang/protobuf v1.4.3
	github.com/hashicorp/go-hclog v0.15.0
	github.com/turbot/go-kit v0.2.0-rc.0
	github.com/turbot/steampipe v0.5.0-rc.0
	github.com/turbot/steampipe-plugin-sdk v0.3.0-rc.0
	google.golang.org/protobuf v1.25.0
)

replace github.com/c-bata/go-prompt => github.com/binaek89/go-prompt v0.2.7-multiline-clearscreen

// main
//replace github.com/turbot/steampipe => github.com/turbot/steampipe v0.4.1-0.20210506162355-af994d6c203f
replace github.com/turbot/steampipe-plugin-sdk => github.com/turbot/steampipe-plugin-sdk v0.3.0-rc.0.0.20210510130246-61e1bddf930a
