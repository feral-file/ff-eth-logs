package chain

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/stretchr/testify/require"
)

// TestCallDeadlineBoundsAHangingProvider pins that a provider that never
// answers cannot block a call forever: each attempt is bounded by
// callTimeout, retried inside the (here tiny) budget, and the call then
// fails with the deadline error so ingestion can exit and reconnect.
func TestCallDeadlineBoundsAHangingProvider(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // never answers until the test ends
	}))
	t.Cleanup(func() { close(release); srv.Close() })

	raw, err := ethclient.Dial(srv.URL)
	require.NoError(t, err)
	c := NewRealEthClient(raw, srv.URL)
	c.callTimeout = 50 * time.Millisecond
	c.retry = retryPolicy{initial: 10 * time.Millisecond, max: 20 * time.Millisecond, elapsed: 200 * time.Millisecond}

	start := time.Now()
	_, err = c.HeadByNumber(context.Background(), 1)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded), err)
	require.Contains(t, err.Error(), "HeadByNumber exceeded 50ms")
	require.Less(t, time.Since(start), 2*time.Second, "bounded by the retry budget, not by the provider")

	// A caller cancellation is permanent: no retry, the caller's error.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = c.BlockNumber(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

// TestDialIsBounded pins that a provider whose handshake never completes
// cannot hang startup: Dial fails within the configured timeout.
func TestDialIsBounded(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release // the WebSocket upgrade never completes
	}))
	t.Cleanup(func() { close(release); srv.Close() })
	wsURL := "ws" + srv.URL[len("http"):]

	start := time.Now()
	_, err := Dial(context.Background(), wsURL, 100*time.Millisecond)
	require.Error(t, err)
	require.Contains(t, err.Error(), "dial ws://")
	require.Less(t, time.Since(start), 2*time.Second)
}
