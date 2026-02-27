package hub

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// setupTestTracer sets up a test tracer provider and returns a cleanup function
func setupTestTracer() func() {
	// Create a tracer provider for testing
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	// Set as global tracer provider
	otel.SetTracerProvider(tp)

	// Set global propagator
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Return cleanup function
	return func() {
		_ = tp.Shutdown(context.Background())
	}
}

func TestParseTraceContext(t *testing.T) {
	// Set up test tracer
	cleanup := setupTestTracer()
	defer cleanup()

	// Create a hubBase
	hub := &hubBase{}

	tests := []struct {
		name     string
		input    string
		expected bool // whether valid context should be extracted
	}{
		// Session variable format tests
		{
			name:     "Session variables: Valid traceparent only",
			input:    "traceparent=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
			expected: true,
		},
		{
			name:     "Session variables: Valid traceparent and tracestate",
			input:    "traceparent=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01;tracestate=rojo=00f067aa0ba902b7",
			expected: true,
		},
		// SQLcommenter format tests
		{
			name:     "SQLcommenter: Valid traceparent only with single quotes",
			input:    "traceparent='00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'",
			expected: true,
		},
		{
			name:     "SQLcommenter: Valid traceparent and tracestate with single quotes",
			input:    "traceparent='00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',tracestate='rojo=00f067aa0ba902b7'",
			expected: true,
		},
		{
			name:     "SQLcommenter: Valid traceparent and tracestate with double quotes",
			input:    "traceparent=\"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\",tracestate=\"rojo=00f067aa0ba902b7\"",
			expected: true,
		},
		{
			name:     "SQLcommenter: Mixed with other attributes",
			input:    "application='myapp',traceparent='00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',tracestate='rojo=00f067aa0ba902b7'",
			expected: true,
		},
		{
			name:     "SQLcommenter: No quotes",
			input:    "traceparent=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01,tracestate=rojo=00f067aa0ba902b7",
			expected: true,
		},
		// Error cases
		{
			name:     "Empty string",
			input:    "",
			expected: false,
		},
		{
			name:     "Invalid format",
			input:    "invalid-trace-context",
			expected: false,
		},
		{
			name:     "Missing traceparent (session variable format)",
			input:    "tracestate=rojo=00f067aa0ba902b7",
			expected: false,
		},
		{
			name:     "Missing traceparent (SQLcommenter format)",
			input:    "tracestate='rojo=00f067aa0ba902b7'",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := hub.parseTraceContext(tt.input)
			hasValidSpan := ctx != nil && trace.SpanContextFromContext(ctx).IsValid()

			if hasValidSpan != tt.expected {
				t.Errorf("parseTraceContext() = %v, expected %v for input: %s", hasValidSpan, tt.expected, tt.input)
			}

			if hasValidSpan {
				spanCtx := trace.SpanContextFromContext(ctx)
				t.Logf("Successfully extracted trace context - TraceID: %s, SpanID: %s",
					spanCtx.TraceID().String(), spanCtx.SpanID().String())
			}
		})
	}
}

func TestTraceContextForScanWithSessionVariables(t *testing.T) {
	// Set up test tracer
	cleanup := setupTestTracer()
	defer cleanup()

	// Create a hubBase
	hub := &hubBase{}

	// Test with trace context in options
	opts := map[string]string{
		"connection":    "aws",
		"table":         "aws_s3_bucket",
		"trace_context": "traceparent=00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}

	traceCtx := hub.traceContextForScan("aws_s3_bucket", []string{"name", "region"}, 10, nil, "aws", opts)

	if traceCtx == nil {
		t.Fatal("Expected trace context to be created")
	}

	if traceCtx.Span == nil {
		t.Fatal("Expected span to be created")
	}

	// Verify that the span has a valid context
	spanCtx := traceCtx.Span.SpanContext()
	if !spanCtx.IsValid() {
		t.Fatal("Expected span context to be valid")
	}

	t.Logf("Created span with TraceID: %s, SpanID: %s",
		spanCtx.TraceID().String(), spanCtx.SpanID().String())
}

func TestTraceContextForScanWithoutSessionVariables(t *testing.T) {
	// Set up test tracer
	cleanup := setupTestTracer()
	defer cleanup()

	// Create a hubBase
	hub := &hubBase{}

	// Test without trace context in options
	opts := map[string]string{
		"connection": "aws",
		"table":      "aws_s3_bucket",
	}

	traceCtx := hub.traceContextForScan("aws_s3_bucket", []string{"name", "region"}, 10, nil, "aws", opts)

	if traceCtx == nil {
		t.Fatal("Expected trace context to be created")
	}

	if traceCtx.Span == nil {
		t.Fatal("Expected span to be created")
	}

	// Verify that the span has a valid context (should be a new root span)
	spanCtx := traceCtx.Span.SpanContext()
	if !spanCtx.IsValid() {
		t.Fatal("Expected span context to be valid")
	}

	t.Logf("Created root span with TraceID: %s, SpanID: %s",
		spanCtx.TraceID().String(), spanCtx.SpanID().String())
}

func TestTraceContextForScanWithSQLcommenter(t *testing.T) {
	// Set up test tracer
	cleanup := setupTestTracer()
	defer cleanup()

	// Create a hubBase
	hub := &hubBase{}

	// Test with SQLcommenter format trace context in options
	opts := map[string]string{
		"connection":    "aws",
		"table":         "aws_s3_bucket",
		"trace_context": "traceparent='00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',tracestate='rojo=00f067aa0ba902b7'",
	}

	traceCtx := hub.traceContextForScan("aws_s3_bucket", []string{"name", "region"}, 10, nil, "aws", opts)

	if traceCtx == nil {
		t.Fatal("Expected trace context to be created")
	}

	if traceCtx.Span == nil {
		t.Fatal("Expected span to be created")
	}

	// Verify that the span has a valid context
	spanCtx := traceCtx.Span.SpanContext()
	if !spanCtx.IsValid() {
		t.Fatal("Expected span context to be valid")
	}

	t.Logf("Created span with SQLcommenter format - TraceID: %s, SpanID: %s",
		spanCtx.TraceID().String(), spanCtx.SpanID().String())
}

func TestParseTraceContextSQLcommenterFormats(t *testing.T) {
	// Set up test tracer
	cleanup := setupTestTracer()
	defer cleanup()

	// Create a hubBase
	hub := &hubBase{}

	sqlcommenterTests := []struct {
		name        string
		input       string
		description string
	}{
		{
			name:        "Basic SQLcommenter with single quotes",
			input:       "traceparent='00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01'",
			description: "Basic traceparent only with single quotes",
		},
		{
			name:        "SQLcommenter with both traceparent and tracestate",
			input:       "traceparent='00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',tracestate='rojo=00f067aa0ba902b7'",
			description: "Both traceparent and tracestate with single quotes",
		},
		{
			name:        "SQLcommenter with double quotes",
			input:       "traceparent=\"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\",tracestate=\"rojo=00f067aa0ba902b7\"",
			description: "Both traceparent and tracestate with double quotes",
		},
		{
			name:        "SQLcommenter mixed with other attributes",
			input:       "application='myapp',traceparent='00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',tracestate='rojo=00f067aa0ba902b7',controller='users'",
			description: "Trace context mixed with other SQLcommenter attributes",
		},
	}

	for _, tt := range sqlcommenterTests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := hub.parseTraceContext(tt.input)
			hasValidSpan := ctx != nil && trace.SpanContextFromContext(ctx).IsValid()

			if !hasValidSpan {
				t.Errorf("parseTraceContext() failed to extract valid context from SQLcommenter format: %s", tt.input)
			} else {
				spanCtx := trace.SpanContextFromContext(ctx)
				t.Logf("Successfully extracted SQLcommenter trace context (%s) - TraceID: %s, SpanID: %s",
					tt.description, spanCtx.TraceID().String(), spanCtx.SpanID().String())
			}
		})
	}
}
