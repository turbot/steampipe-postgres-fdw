package instrument

import (
	"context"

	"github.com/turbot/steampipe/constants"
	"github.com/turbot/steampipe/version"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	TRACER_NAME     = "fdw"
	TRACER_ENDPOINT = "http://localhost:14268/api/traces"
)

// tracerProvider returns an OpenTelemetry TracerProvider configured to use
// the Jaeger exporter that will send spans to the provided url. The returned
// TracerProvider will also use a Resource configured with all the information
// about the application.
func InitTracing() error {
	// Create the Jaeger exporter
	jaegerExporter, _ := jaeger.New(jaeger.WithCollectorEndpoint(jaeger.WithEndpoint(TRACER_ENDPOINT)))
	tp := tracesdk.NewTracerProvider(
		// Always be sure to batch in production.
		tracesdk.WithBatcher(jaegerExporter),
		// Record information about this application in a Resource.
		tracesdk.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("CLI"),
			semconv.ServiceVersionKey.String(version.String()),
		)),
	)

	otel.SetTracerProvider(tp)

	return nil
}

func ShutdownTracing() {
	defer func() {
		// artificially prevent a panic in this fn
		recover()
	}()
	otel.GetTracerProvider().(*tracesdk.TracerProvider).ForceFlush(context.Background())
	otel.GetTracerProvider().(*tracesdk.TracerProvider).Shutdown(context.Background())
}

func FlushTraces() {
	defer func() {
		// artificially prevent a panic in this fn
		recover()
	}()
	otel.GetTracerProvider().(*tracesdk.TracerProvider).ForceFlush(context.Background())
}

func GetTracer() trace.Tracer {
	return otel.GetTracerProvider().Tracer(constants.AppName)
}

func StartRootSpan(id string) (context.Context, trace.Span) {
	tr := GetTracer()
	id = "callId"
	traceContext, span := tr.Start(context.Background(), id)
	span.SetAttributes(attribute.Key(id).String(id))
	// span.SetAttributes(attribute.Key(fmt.Sprintf("flag-%s", f.Name)).String(f.Value.String()))
	// span.SetAttributes(attribute.Key("args").StringSlice(flags.Args()))

	return traceContext, span
}

func StartSpan(baseCtx context.Context, name string) (context.Context, trace.Span) {
	tr := GetTracer()
	return tr.Start(baseCtx, name)
}
