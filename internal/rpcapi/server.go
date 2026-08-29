package rpcapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/ethereum/go-ethereum/rpc"

	"github.com/feral-file/ff-eth-logs/internal/logger"
	"github.com/feral-file/ff-eth-logs/internal/logstore"
)

// ServerConfig is the HTTP listener configuration.
type ServerConfig struct {
	Host         string
	Port         int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
}

// Server is the HTTP server: JSON-RPC at "/" and a JSON health page at
// "/health" (status, head, seconds since the cursor moved) for the compose
// healthcheck and for operators.
type Server struct {
	http *http.Server
	rpc  *rpc.Server
}

// NewServer wires the API into go-ethereum's rpc.Server and an http.Server.
func NewServer(cfg ServerConfig, api *API) (*Server, error) {
	rpcServer := rpc.NewServer()
	if err := rpcServer.RegisterName("eth", api); err != nil {
		return nil, fmt.Errorf("register eth namespace: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle("/", rpcServer)
	mux.HandleFunc("/health", api.health)
	return &Server{
		rpc: rpcServer,
		http: &http.Server{
			Addr:         net.JoinHostPort(cfg.Host, fmt.Sprint(cfg.Port)),
			Handler:      mux,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
			IdleTimeout:  cfg.IdleTimeout,
		},
	}, nil
}

// Run serves until ctx ends, then shuts down gracefully within 5 s.
func (s *Server) Run(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		logger.InfoCtx(ctx, "JSON-RPC server listening", zapAddr(s.http.Addr))
		errCh <- s.http.ListenAndServe()
	}()
	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := s.http.Shutdown(shutdownCtx)
		s.rpc.Stop()
		<-errCh
		if err != nil {
			return err
		}
		return ctx.Err()
	}
}

// health reports the covered interval. It is 200 whenever the database
// answers; lag is for dashboards, not for the healthcheck, because a long
// catch-up is a healthy state.
func (a *API) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := a.bounded(r.Context())
	defer cancel()
	var cov logstore.Coverage
	var ok bool
	err := a.store.Read(ctx, func(v logstore.View) error {
		var err error
		cov, ok, err = v.Coverage(ctx)
		return err
	})
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "error", "error": err.Error()})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok", "head": cov.Head, "coverage_start": cov.Start, "empty": !ok, "chain_id": a.cfg.ChainID})
}
