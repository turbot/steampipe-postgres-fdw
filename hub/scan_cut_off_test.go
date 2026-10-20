package hub

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v6/telemetry"
	"github.com/turbot/steampipe/v2/pkg/query/queryresult"
	"go.opentelemetry.io/otel/trace"
)

// A scan that stops before the plugin finished must fail, not end as a complete, short result.

func testRow() *proto.ExecuteResponse {
	return &proto.ExecuteResponse{Row: &proto.Row{Columns: map[string]*proto.Column{}}, Connection: "c"}
}

// startTestScan starts an iterator reading from a localStream fed by plugin, as scanIteratorLocal.execute
// and Start do. Only a test that installs a query cancel checker passes watchCancel.
func startTestScan(t *testing.T, watchCancel bool, plugin func(ctx context.Context, s *localStream) error) *scanIteratorBase {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	stream := newLocalStream(ctx)
	go func() { stream.finish(plugin(ctx, stream)) }()

	i := &scanIteratorBase{
		status:          QueryStatusStarted,
		rows:            make(chan *proto.Row, rowBufferSize),
		scanMetadata:    map[string]queryresult.ScanMetadataRow{},
		pluginRowStream: stream,
		table:           "fake_table",
		cancel:          cancel,
		traceCtx:        &telemetry.TraceCtx{Ctx: ctx, Span: trace.SpanFromContext(ctx)},
		queryContext:    proto.NewQueryContext(nil, nil, -1, nil),
		startTime:       time.Now(),
	}
	go i.readThread(ctx)
	if watchCancel {
		go i.watchForCancellation(ctx)
	}
	return i
}

// drain reads rows until the iterator closes its row channel (the plugin finished or the scan stopped).
func drain(i *scanIteratorBase) int {
	n := 0
	for row := range i.rows {
		if row != nil {
			n++
		}
	}
	return n
}

func sendRows(ctx context.Context, s *localStream, n int) error {
	for range n {
		if err := s.Send(testRow()); err != nil {
			return err
		}
	}
	return nil
}

func TestScanCompletesWhenPluginFinishes(t *testing.T) {
	i := startTestScan(t, false, func(ctx context.Context, s *localStream) error { return sendRows(ctx, s, 25) })

	if n := drain(i); n != 25 {
		t.Fatalf("got %d rows, want 25", n)
	}
	if i.status == QueryStatusError {
		t.Fatalf("scan failed: %v", i.err)
	}
}

// The plugin returns an error after the rows of its first pages (a 5xx or throttle once retries run
// out): the error reaches the scan after those rows, instead of the scan ending as complete.
func TestScanFailsWhenPluginErrorsAfterFirstPage(t *testing.T) {
	i := startTestScan(t, false, func(ctx context.Context, s *localStream) error {
		if err := sendRows(ctx, s, 30); err != nil {
			return err
		}
		return errors.New("provider returned HTTP 503 on page 3")
	})

	if n := drain(i); n != 30 {
		t.Fatalf("got %d rows, want the 30 sent before the error", n)
	}
	if i.status != QueryStatusError || i.err == nil || !strings.Contains(i.err.Error(), "HTTP 503") {
		t.Fatalf("status %s, err %v: want the plugin's error", i.status, i.err)
	}
}

// Postgres asks to cancel the query (statement_timeout, pg_cancel_backend) mid-scan, which
// watchForCancellation turns into a cancel of the iterator's context. The scan did not ask for it, so
// it fails; the FDW then raises Postgres' own 57014 for the pending cancel.
func TestScanFailsWhenPostgresCancelsIt(t *testing.T) {
	var cancelPending atomic.Bool
	SetQueryCancelChecker(cancelPending.Load)
	t.Cleanup(func() { SetQueryCancelChecker(nil) })

	sent := make(chan struct{})
	i := startTestScan(t, true, func(ctx context.Context, s *localStream) error {
		if err := sendRows(ctx, s, 30); err != nil {
			return err
		}
		close(sent)
		<-ctx.Done() // the next page never arrives
		return ctx.Err()
	})
	<-sent
	cancelPending.Store(true)

	if n := drain(i); n > 30 {
		t.Fatalf("got %d rows, want at most 30", n)
	}
	if i.status != QueryStatusError || !errors.Is(i.err, context.Canceled) ||
		!strings.Contains(i.err.Error(), "scan of fake_table cancelled before it completed") {
		t.Fatalf("status %s, err %v: want the scan to fail as cancelled", i.status, i.err)
	}
}

// Postgres ends the scan itself once it has the rows it needs (LIMIT): Close cancels the context,
// and that is not a failure.
func TestScanClosedEarlyIsNotAnError(t *testing.T) {
	i := startTestScan(t, false, func(ctx context.Context, s *localStream) error {
		for {
			if err := s.Send(testRow()); err != nil {
				return err
			}
		}
	})
	for range 15 {
		<-i.rows
	}
	i.Close()
	drain(i)

	if i.status != QueryStatusComplete || i.err != nil {
		t.Fatalf("status %s, err %v: want complete with no error", i.status, i.err)
	}
}

func TestLocalStreamReturnsBufferedRowsBeforeTheError(t *testing.T) {
	s := newLocalStream(context.Background())
	for range 3 {
		if err := s.Send(testRow()); err != nil {
			t.Fatal(err)
		}
	}
	s.finish(errors.New("boom"))

	for n := range 3 {
		if r, err := s.Recv(); r == nil || err != nil {
			t.Fatalf("Recv %d: got (%v, %v), want a row", n, r, err)
		}
	}
	if r, err := s.Recv(); r != nil || err == nil || err.Error() != "boom" {
		t.Fatalf("got (%v, %v), want the plugin's error", r, err)
	}
}

func TestLocalStreamEndsWithEOF(t *testing.T) {
	s := newLocalStream(context.Background())
	if err := s.Send(testRow()); err != nil {
		t.Fatal(err)
	}
	s.finish(nil)

	if r, err := s.Recv(); r == nil || err != nil {
		t.Fatalf("got (%v, %v), want a row", r, err)
	}
	if _, err := s.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("got %v, want io.EOF", err)
	}
}
