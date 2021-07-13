module github.com/turbot/steampipe-postgres-fdw

go 1.15

require (
	github.com/dgraph-io/ristretto v0.0.3
	github.com/golang/protobuf v1.4.3
	github.com/hashicorp/go-hclog v0.15.0
	github.com/hashicorp/go-version v1.2.1
	github.com/turbot/go-kit v0.2.2-0.20210701162132-c7aef7d72757
	github.com/turbot/steampipe v1.7.0-rc.0.0.20210713170830-c98a0f4adcfa // main
	github.com/turbot/steampipe-plugin-sdk v0.3.0-rc.0.0.20210707184714-511c93a3762b // main
	google.golang.org/protobuf v1.25.0
)

replace github.com/c-bata/go-prompt => github.com/binaek89/go-prompt v0.2.7-multiline-clearscreen
