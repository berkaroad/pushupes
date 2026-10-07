package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// HTTPServer owns the listener lifecycle for the API handler tree.
type HTTPServer struct {
	httpServer *http.Server
	logger     *slog.Logger
}

// NewServer builds an HTTPServer on the given address.
func NewServer(addr string, handler http.Handler, logger *slog.Logger) *HTTPServer {
	return &HTTPServer{
		httpServer: &http.Server{
			Addr:              addr,
			Handler:           handler,
			ReadHeaderTimeout: 10 * time.Second,
		},
		logger: logger,
	}
}

// ListenAndServe blocks serving until Shutdown is called elsewhere.
func (s *HTTPServer) ListenAndServe() error {
	s.logger.Info("pushupes admin server listening", "addr", s.httpServer.Addr)
	err := s.httpServer.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Shutdown gracefully stops the server.
func (s *HTTPServer) Shutdown(ctx context.Context) error {
	s.logger.Info("Shutting down HTTP server")
	return s.httpServer.Shutdown(ctx)
}
