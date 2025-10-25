package http

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

const (
	contentTypeJSON        = "application/json"
	defaultHTTPPort        = "8080"
	defaultShutdownTimeout = time.Second * 5
)

// iStoreAPI is a minimal interface used by RPC handlers. It allows using a fake store in tests.
type iStoreAPI interface {
	PutString(key, value string) error
	GetString(key string) (string, bool, error)
	Delete(key string) error
}

// Server represents the HTTP server with storage
type Server struct {
	store      iStoreAPI
	httpServer *http.Server
	URL        string
	addr       string
}

// NewServer creates a new server instance. Accepts any implementation of iStoreAPI (including *store.Store).
func NewServer(store iStoreAPI, port string) *Server {
	if port == "" {
		port = defaultHTTPPort
	}
	return &Server{
		store: store,
		URL:   "http://localhost:" + port,
		addr:  ":" + port,
	}
}

// Start starts the server
func (s *Server) Start() error {
	if err := s.startHTTPServer(); err != nil {
		return fmt.Errorf("failed to start HTTP server: %w", err)
	}
	return nil
}

// Stop stops the server
func (s *Server) Stop() error {
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
		defer cancel()

		if err := s.httpServer.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown HTTP server: %w", err)
		}
	}
	return nil
}

// createRouter builds chi router
func (s *Server) createRouter() http.Handler {
	r := chi.NewRouter()

	r.Get("/health", s.handleHealth)
	r.Get("/metrics", s.handleMetrics)
	r.Put("/api/string", s.handlePut)
	r.Get("/api/string", s.handleGet)
	r.Delete("/api", s.handleDelete)

	return r
}

func (s *Server) startHTTPServer() error {
	s.httpServer = &http.Server{
		Addr:              s.addr,
		Handler:           s.createRouter(),
		ReadHeaderTimeout: time.Second,
	}

	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "error", err)
		}
	}()

	slog.Info("HTTP server started", "addr", s.URL)
	return nil
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", contentTypeJSON)
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		slog.Warn("Error encoding response", "error", err)
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, NewOKResponse())
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if _, err := w.Write([]byte("# LSMDB Metrics\n")); err != nil {
		slog.Warn("Failed to write metrics response", "error", err)
	}
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s.writeJSON(w, http.StatusBadRequest, NewErrorResponse("Failed to parse form"))
		return
	}

	key := r.FormValue("key")
	value := r.FormValue("value")

	if key == "" || value == "" {
		s.writeJSON(w, http.StatusBadRequest, NewErrorResponse("Missing key or value"))
		return
	}

	if err := s.store.PutString(key, value); err != nil {
		s.writeJSON(w, http.StatusInternalServerError, NewErrorResponse(err.Error()))
		return
	}

	s.writeJSON(w, http.StatusOK, NewSuccessResponse())
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		s.writeJSON(w, http.StatusBadRequest, NewErrorResponse("Missing key"))
		return
	}

	value, found, err := s.store.GetString(key)
	if err != nil {
		s.writeJSON(w, http.StatusInternalServerError, NewErrorResponse(err.Error()))
		return
	}

	if !found {
		s.writeJSON(w, http.StatusNotFound, NewErrorResponse("Key not found"))
		return
	}

	s.writeJSON(w, http.StatusOK, NewValueResponse(value))
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		s.writeJSON(w, http.StatusBadRequest, NewErrorResponse("Missing key"))
		return
	}

	if err := s.store.Delete(key); err != nil {
		s.writeJSON(w, http.StatusInternalServerError, NewErrorResponse(err.Error()))
		return
	}

	s.writeJSON(w, http.StatusOK, NewSuccessResponse())
}
