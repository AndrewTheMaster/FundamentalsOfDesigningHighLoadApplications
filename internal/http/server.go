package http

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"lsmdb/pkg/cluster"
	"lsmdb/pkg/raftadapter"
	"lsmdb/pkg/store"
	"net/http"
	"net/url"
	"time"

	"github.com/go-chi/chi/v5"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

const (
	contentTypeJSON        = "application/json"
	defaultHTTPPort        = "8080"
	defaultShutdownTimeout = time.Second * 5
)

type iStoreAPI interface {
	GetString(key string) (string, bool, error)
}

type iRaftNode interface {
	IsLeader() bool
	LeaderAddr() string
	Execute(ctx context.Context, cmd raftadapter.Cmd) error
	Handle(ctx context.Context, message raftpb.Message) error

	Run(ctx context.Context) error
	Stop() error
}

// iShardedDB - интерфейс для шардированной БД
type iShardedDB interface {
	Put(ctx context.Context, key, value string) error
	Get(ctx context.Context, key string) (string, bool, error)
	Delete(ctx context.Context, key string) error
}

// Server represents the HTTP server with storage
type Server struct {
	node       iRaftNode
	store      iStoreAPI
	shardedDB  iShardedDB // опциональный шардированный слой
	httpServer *http.Server
	URL        string
	addr       string
}

// NewServer creates a new server instance
func NewServer(node iRaftNode, port string) *Server {
	if port == "" {
		port = defaultHTTPPort
	}
	return &Server{
		node: node,
		URL:  "http://localhost:" + port,
		addr: ":" + port,
	}
}

// NewShardedServer creates a new server with sharding support
func NewShardedServer(shardedDB *cluster.ShardedRaftDB, store iStoreAPI, port string) *Server {
	if port == "" {
		port = defaultHTTPPort
	}
	return &Server{
		shardedDB: shardedDB,
		store:     store,
		URL:       "http://localhost:" + port,
		addr:      ":" + port,
	}
}

func (s *Server) SetStore(store iStoreAPI) {
	s.store = store
}

// Start starts the server
func (s *Server) Start() error {
	if s.node != nil {
		go func() {
			if err := s.node.Run(context.Background()); err != nil {
				slog.Error("Raft node error", "error", err)
			}
		}()
	}
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
		if s.node != nil {
			_ = s.node.Stop()
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

	// Raft endpoint только если есть node
	if s.node != nil {
		r.Post("/api/internal/raft", s.handleRaft)
	}

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

func (s *Server) redirectLeader(w http.ResponseWriter, r *http.Request) (bool, error) {
	// Редирект работает только при прямом использовании Raft (без шардирования)
	if s.node == nil || s.shardedDB != nil {
		return false, nil
	}

	if !s.node.IsLeader() {
		leaderAddr := s.node.LeaderAddr()
		if leaderAddr == "" {
			// leader unknown yet — don't redirect, allow local handling
			return false, nil
		}

		// Avoid redirect loop when leaderAddr equals this server's URL
		if leaderAddr == s.URL {
			return false, nil
		}

		leaderURL, err := url.JoinPath(leaderAddr, r.URL.Path)
		if err != nil {
			s.writeJSON(w, http.StatusInternalServerError, NewErrorResponse("Failed to get leader URL"))
			return false, fmt.Errorf("failed to join leader path: %w", err)
		}

		http.Redirect(w, r, leaderURL, http.StatusTemporaryRedirect)
		return true, nil
	}
	return false, nil
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

	// Если используется шардирование - используем ShardedDB
	if s.shardedDB != nil {
		if err := s.shardedDB.Put(r.Context(), key, value); err != nil {
			s.writeJSON(w, http.StatusInternalServerError, NewErrorResponse(err.Error()))
			return
		}
		s.writeJSON(w, http.StatusOK, NewSuccessResponse())
		return
	}

	// Иначе используем прямой Raft
	if redirected, err := s.redirectLeader(w, r); redirected || err != nil {
		if err != nil {
			slog.Error("Failed to redirect to leader", "error", err)
		}
		return
	}

	cmd := raftadapter.NewCmd(store.InsertOp, []byte(key), []byte(value))
	if err := s.node.Execute(r.Context(), cmd); err != nil {
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

	var value string
	var found bool
	var err error

	// Если используется шардирование - используем ShardedDB
	if s.shardedDB != nil {
		value, found, err = s.shardedDB.Get(r.Context(), key)
	} else {
		value, found, err = s.store.GetString(key)
	}

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

	// Если используется шардирование - используем ShardedDB
	if s.shardedDB != nil {
		if err := s.shardedDB.Delete(r.Context(), key); err != nil {
			s.writeJSON(w, http.StatusInternalServerError, NewErrorResponse(err.Error()))
			return
		}
		s.writeJSON(w, http.StatusOK, NewSuccessResponse())
		return
	}

	// Иначе используем прямой Raft
	if redirected, err := s.redirectLeader(w, r); redirected || err != nil {
		if err != nil {
			slog.Error("Failed to redirect to leader", "error", err)
		}
		return
	}

	cmd := raftadapter.NewCmd(store.DeleteOp, []byte(key), nil)
	if err := s.node.Execute(r.Context(), cmd); err != nil {
		s.writeJSON(w, http.StatusInternalServerError, NewErrorResponse(err.Error()))
		return
	}

	s.writeJSON(w, http.StatusOK, NewSuccessResponse())
}

func (s *Server) handleRaft(w http.ResponseWriter, r *http.Request) {
	if s.node == nil {
		s.writeJSON(w, http.StatusServiceUnavailable, NewErrorResponse("Raft node not available"))
		return
	}

	dec := json.NewDecoder(r.Body)
	var msg raftpb.Message
	if err := dec.Decode(&msg); err != nil {
		s.writeJSON(w, http.StatusBadRequest, NewErrorResponse(err.Error()))
		return
	}
	if err := s.node.Handle(r.Context(), msg); err != nil {
		s.writeJSON(w, http.StatusInternalServerError, NewErrorResponse(err.Error()))
		return
	}

	s.writeJSON(w, http.StatusOK, NewSuccessResponse())
}
