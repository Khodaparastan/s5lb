package balancer

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/khodaparastan/socks5lb/internal/config"
	"github.com/khodaparastan/socks5lb/internal/metrics"
	"github.com/khodaparastan/socks5lb/internal/strategy"
	"github.com/khodaparastan/socks5lb/internal/upstream"
)

func newTestLB(t *testing.T, maxPer int, ups []*upstream.Upstream) *LoadBalancer {
	t.Helper()
	cfg := config.Defaults()
	cfg.MaxPerProxy = maxPer
	cfg.QueueWaitTimeout = 500 * time.Millisecond
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	reg := prometheus.NewRegistry()
	m := metrics.New(reg, "test", "test", "test")
	sel, _ := strategy.New("least-active")
	return New(cfg, "", log, m, noop.NewTracerProvider().Tracer("t"), ups, sel)
}

func TestAcquireSlot_BasicAndRelease(t *testing.T) {
	u := upstream.New("127.0.0.1", 1, "", "", 1, 0)
	lb := newTestLB(t, 1, []*upstream.Upstream{u})
	defer lb.Shutdown()

	got1 := lb.acquireSlot(context.Background(), strategy.SelectCtx{}, lb.log)
	if got1 == nil {
		t.Fatal("first acquire should succeed")
	}
	// Second acquire on same upstream must queue and time out (MaxPerProxy=1).
	start := time.Now()
	got2 := lb.acquireSlot(context.Background(), strategy.SelectCtx{}, lb.log)
	elapsed := time.Since(start)
	if got2 != nil {
		t.Fatal("second acquire should have timed out")
	}
	if elapsed < 400*time.Millisecond {
		t.Fatalf("acquire returned too fast: %v", elapsed)
	}

	// Release and ensure waiter can succeed.
	lb.releaseSlot(got1)
	got3 := lb.acquireSlot(context.Background(), strategy.SelectCtx{}, lb.log)
	if got3 == nil {
		t.Fatal("acquire after release should succeed")
	}
	lb.releaseSlot(got3)
}

func TestDispatch_FIFO(t *testing.T) {
	u := upstream.New("127.0.0.1", 1, "", "", 1, 0)
	lb := newTestLB(t, 1, []*upstream.Upstream{u})
	defer lb.Shutdown()

	// Take the only slot.
	held := lb.acquireSlot(context.Background(), strategy.SelectCtx{}, lb.log)
	if held == nil {
		t.Fatal("failed to take slot")
	}

	var order []int
	var mu sync.Mutex
	var wg sync.WaitGroup
	const N = 3
	wg.Add(N)
	for i := 0; i < N; i++ {
		i := i
		go func() {
			defer wg.Done()
			u := lb.acquireSlot(context.Background(), strategy.SelectCtx{}, lb.log)
			if u == nil {
				return
			}
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			lb.releaseSlot(u)
		}()
		time.Sleep(5 * time.Millisecond) // ensure enqueue order
	}

	lb.releaseSlot(held) // wake first waiter
	wg.Wait()

	if len(order) != N {
		t.Fatalf("want all %d served, got %d", N, len(order))
	}
	for i := 0; i < N; i++ {
		if order[i] != i {
			t.Fatalf("non-FIFO: %v", order)
		}
	}
}
