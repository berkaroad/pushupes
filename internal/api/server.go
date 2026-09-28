package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
)

// HTTPServer owns the listener lifecycle for the API handler tree.
type HTTPServer struct {
	httpServer *http.Server
	logger     *logrus.Entry
}

// NewServer builds an HTTPServer on the given address.
func NewServer(addr string, handler http.Handler, logger *logrus.Entry) *HTTPServer {
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
	s.logger.WithField("addr", s.httpServer.Addr).Info("pushupes admin server listening")
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
