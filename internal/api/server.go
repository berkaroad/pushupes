// Copyright 2026 berkaroad
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
