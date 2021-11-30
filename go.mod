module github.com/turbot/steampipe-postgres-fdw

go 1.16

require (
	github.com/dgraph-io/ristretto v0.1.0
	github.com/golang/protobuf v1.5.2
	github.com/hashicorp/go-hclog v0.15.0
	github.com/hashicorp/go-version v1.3.0
	github.com/spf13/cobra v1.2.1 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/turbot/go-kit v0.3.0
	// main
	github.com/turbot/steampipe v1.7.0-rc.0.0.20211124164827-1ed6eae515eb
	github.com/turbot/steampipe-plugin-sdk v1.8.0
	go.opentelemetry.io/otel v1.2.0
	go.opentelemetry.io/otel/exporters/jaeger v1.2.0
	go.opentelemetry.io/otel/sdk v1.2.0
	go.opentelemetry.io/otel/trace v1.2.0
	google.golang.org/protobuf v1.27.1
)
