module github.com/turbot/steampipe-postgres-fdw

go 1.15

require (
	github.com/golang/protobuf v1.4.3
	github.com/hashicorp/go-hclog v0.14.1
	github.com/turbot/go-kit v0.0.0-20210119154454-db924443f736
	github.com/turbot/steampipe v0.0.0-20210119164829-e85f286e46e0
	github.com/turbot/steampipe-plugin-sdk v0.0.0-20210120214727-bb3e0ba7e84f
	google.golang.org/protobuf v1.25.0
)

replace github.com/c-bata/go-prompt => github.com/turbot/go-prompt v0.2.6-steampipe

replace github.com/turbot/steampipe-plugin-sdk => github.com/turbot/steampipe-plugin-sdk v0.0.0-20210209160742-a106c8cef00b

replace github.com/turbot/steampipe => github.com/turbot/steampipe v0.1.1-0.20210209160819-d2e12f7f42ce
