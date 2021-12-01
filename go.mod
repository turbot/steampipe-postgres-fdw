module github.com/turbot/steampipe-postgres-fdw

go 1.16

require (
	github.com/dgraph-io/ristretto v0.1.0
	github.com/golang/protobuf v1.5.2
	github.com/hashicorp/go-hclog v0.15.0
	github.com/hashicorp/go-version v1.3.0
	github.com/turbot/go-kit v0.3.0
	// main
	github.com/turbot/steampipe v1.7.0-rc.0.0.20211124164827-1ed6eae515eb
	github.com/turbot/steampipe-plugin-sdk v1.8.3-0.20211201054607-5d4d53229a45
	go.opentelemetry.io/otel v1.2.0
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.2.0 // indirect
	go.opentelemetry.io/otel/trace v1.2.0
	google.golang.org/protobuf v1.27.1
)

replace github.com/turbot/steampipe-plugin-sdk => /Users/pskrbasu/turbot-delivery/Steampipe/steampipe-plugin-sdk
