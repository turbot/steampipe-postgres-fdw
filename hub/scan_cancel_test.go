package hub

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/turbot/steampipe-plugin-sdk/v6/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v6/row_stream"
	"github.com/turbot/steampipe-plugin-sdk/v6/telemetry"
	"go.opentelemetry.io/otel/trace"
)

// How a Postgres query cancel reaches a scan: only the scan Postgres is waiting on is cancelled.

type fakeHub struct{ Hub }

func (fakeHub) RemoveIterator(Iterator) {}

type fakeExecutor struct {
	plugin func(ctx context.Context, s *localStream) error
}

func (e fakeExecutor) execute(*proto.ExecuteRequest) (row_stream.Receiver, context.Context, context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	stream := newLocalStream(ctx)
	go func() { stream.finish(e.plugin(ctx, stream)) }()
	return stream, ctx, cancel, nil
}

// installCancelChecker stands in for Postgres' QueryCancelPending flag, counting how often it is read.
func installCancelChecker(t *testing.T) (pending *atomic.Bool, reads *atomic.Int64) {
	t.Helper()
	pending, reads = &atomic.Bool{}, &atomic.Int64{}
	SetQueryCancelChecker(func() bool {
		reads.Add(1)
		return pending.Load()
	})
	t.Cleanup(func() { SetQueryCancelChecker(nil) })
	return pending, reads
}

func startScan(t *testing.T, plugin func(ctx context.Context, s *localStream) error) *scanIteratorBase {
	t.Helper()
	ctx := context.Background()
	traceCtx := &telemetry.TraceCtx{Ctx: ctx, Span: trace.SpanFromContext(ctx)}
	i := newBaseScanIterator(fakeHub{}, "c", "fake_table", nil, nil, nil, -1, nil, 0, traceCtx)
	if err := i.Start(fakeExecutor{plugin: plugin}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { i.cancel() })
	return &i
}

// nextAll calls Next as IterateForeignScan does until the scan ends, failing the test if it hangs.
func nextAll(t *testing.T, i *scanIteratorBase) (int, error) {
	t.Helper()
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		n := 0
		for {
			row, err := i.Next()
			if err != nil || row == nil {
				done <- result{n, err}
				return
			}
			n++
		}
	}()
	select {
	case r := <-done:
		return r.n, r.err
	case <-time.After(10 * time.Second):
		t.Fatal("Next did not return")
		return 0, nil
	}
}

// statement_timeout or pg_cancel_backend while IterateForeignScan waits on a plugin that never sends
// its next page: the scan stops and fails, so the backend can raise Postgres' 57014.
func TestCancelStopsScanWaitingOnHungPlugin(t *testing.T) {
	pending, _ := installCancelChecker(t)
	i := startScan(t, func(ctx context.Context, s *localStream) error {
		if err := sendRows(ctx, s, 3); err != nil {
			return err
		}
		<-ctx.Done() // the next page never arrives
		return ctx.Err()
	})
	for range 3 {
		if row, err := i.Next(); row == nil || err != nil {
			t.Fatalf("got (%v, %v), want a row", row, err)
		}
	}
	pending.Store(true)

	_, err := nextAll(t, i)
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "scan of fake_table cancelled before it completed") {
		t.Fatalf("err %v: want the scan to fail as cancelled", err)
	}
}

// A cursor's scan sits idle between FETCHes while another statement runs and is cancelled. The cancel
// belongs to that statement: after ROLLBACK TO SAVEPOINT the cursor's next FETCH must still work.
func TestCancelOfAnotherStatementLeavesIdleScanRunning(t *testing.T) {
	pending, _ := installCancelChecker(t)
	morePages := make(chan struct{})
	i := startScan(t, func(ctx context.Context, s *localStream) error {
		if err := sendRows(ctx, s, 5); err != nil {
			return err
		}
		select {
		case <-morePages:
		case <-ctx.Done():
			return ctx.Err()
		}
		return sendRows(ctx, s, 5)
	})

	// another statement is cancelled while this scan is not being read
	pending.Store(true)
	time.Sleep(3 * queryCancelPollInterval)
	pending.Store(false)
	close(morePages)

	n, err := nextAll(t, i)
	if err != nil || n != 10 {
		t.Fatalf("got %d rows, err %v: want all 10 rows and no error", n, err)
	}
}

// A scan Postgres stops reading without ending it (a rescan inside a join, an EXISTS that found its
// row) must not keep calling into Postgres for the rest of the backend's life.
func TestUnreadScanDoesNotPollForCancel(t *testing.T) {
	_, reads := installCancelChecker(t)
	startScan(t, func(ctx context.Context, s *localStream) error {
		<-ctx.Done()
		return ctx.Err()
	})

	time.Sleep(3 * queryCancelPollInterval)
	if n := reads.Load(); n != 0 {
		t.Fatalf("cancel flag read %d times by a scan nobody is reading, want 0", n)
	}
}
