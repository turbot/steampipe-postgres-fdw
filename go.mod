module github.com/turbot/steampipe-postgres-fdw

go 1.15

require (
	github.com/golang/protobuf v1.4.3
	github.com/hashicorp/go-hclog v0.14.1
	github.com/turbot/go-kit v0.1.1
	github.com/turbot/steampipe v0.0.0-20210119164829-e85f286e46e0
	github.com/turbot/steampipe-plugin-sdk v0.0.0-20210120214727-bb3e0ba7e84f
	google.golang.org/protobuf v1.25.0
)

replace github.com/c-bata/go-prompt => github.com/binaek89/go-prompt v0.2.7-multiline-clearscreen

// issue-21
replace github.com/turbot/steampipe-plugin-sdk => github.com/turbot/steampipe-plugin-sdk v0.1.2-0.20210216110503-b3467c637d18

// issue-173
replace github.com/turbot/steampipe => github.com/turbot/steampipe v0.1.1-0.20210216110648-5a27457d3ae8
