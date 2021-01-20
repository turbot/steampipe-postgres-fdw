module github.com/turbot/steampipe-postgres-fdw

go 1.15

require (
	github.com/go-ole/go-ole v1.2.5 // indirect
	github.com/golang/protobuf v1.4.3
	github.com/hashicorp/go-hclog v0.14.1
	github.com/turbot/go-kit v0.0.0-20210119154454-db924443f736
	github.com/turbot/steampipe v0.0.0-20210119164829-e85f286e46e0
	github.com/turbot/steampipe-plugin-sdk v0.0.0-20210120214727-bb3e0ba7e84f
	google.golang.org/protobuf v1.25.0
)

replace github.com/c-bata/go-prompt => github.com/binaek89/go-prompt v0.2.7-multiline-clearscreen
