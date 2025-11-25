package rpc

import (
	"context"
	"fmt"
	"net/http"
)

type KV interface {
	PutString(key, value string) error
	GetString(key string) (string, bool, error)
	Delete(key string) error
}

// Server represents the HTTP server
type Server struct {
	kv         KV
	httpServer *http.Server
	port       string
}

// NewServer creates a new server
func NewServer(kv KV, port string) *Server {
	return &Server{
		kv:   kv,
		port: port,
	}
}

// Start starts the server
func (s *Server) Start() error {
	// Start HTTP server
	if err := s.startHTTPServer(); err != nil {
		return fmt.Errorf("failed to start HTTP server: %w", err)
	}

	return nil
}

// createHTTPHandler creates the HTTP handler for testing
func (s *Server) createHTTPHandler() http.Handler {
	mux := http.NewServeMux()

	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// REST API endpoints
	mux.HandleFunc("/api/put", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse form data
		if err := r.ParseForm(); err != nil {
			http.Error(w, "Failed to parse form", http.StatusBadRequest)
			return
		}

		key := r.FormValue("key")
		value := r.FormValue("value")

		if key == "" || value == "" {
			http.Error(w, "Missing key or value", http.StatusBadRequest)
			return
		}

		err := s.kv.PutString(key, value)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"success"}`))
	})

	mux.HandleFunc("/api/get", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "Missing key", http.StatusBadRequest)
			return
		}

		value, found, err := s.kv.GetString(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if !found {
			http.Error(w, "Key not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"value":"` + value + `"}`))
	})

	mux.HandleFunc("/api/delete", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "DELETE" {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "Missing key", http.StatusBadRequest)
			return
		}

		err := s.kv.Delete(key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"success"}`))
	})

	// Metrics endpoint
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("# LSMDB Metrics\n"))
	})

	return mux
}

// startHTTPServer starts the HTTP server for health checks
func (s *Server) startHTTPServer() error {
	mux := s.createHTTPHandler()

	s.httpServer = &http.Server{
		Addr:    ":" + s.port,
		Handler: mux,
	}

	// Start HTTP server in goroutine
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("HTTP server error: %v\n", err)
		}
	}()

	fmt.Println("HTTP server started on port 8081")
	return nil
}

// Stop stops the server
func (s *Server) Stop() error {
	// Stop HTTP server
	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(context.TODO()); err != nil {
			return fmt.Errorf("failed to shutdown HTTP server: %w", err)
		}
	}

	return nil
}
