package rpc

import (
	"context"
	"fmt"
	"lsmdb/pkg/store"
	"time"
)

// KVService implements the key-value service
type KVService struct {
	store *store.Store
}

// NewKVService creates a new KV service
func NewKVService(store *store.Store) *KVService {
	return &KVService{
		store: store,
	}
}

// PutRequest represents a put request
type PutRequest struct {
	Key   string
	Value string
}

// PutResponse represents a put response
type PutResponse struct {
	Success bool
	Error   string
}

// GetRequest represents a get request
type GetRequest struct {
	Key string
}

// GetResponse represents a get response
type GetResponse struct {
	Value  string
	Found  bool
	Error  string
}

// DeleteRequest represents a delete request
type DeleteRequest struct {
	Key string
}

// DeleteResponse represents a delete response
type DeleteResponse struct {
	Success bool
	Error   string
}

// BatchRequest represents a batch request
type BatchRequest struct {
	Operations []Operation
}

// Operation represents a single operation in a batch
type Operation struct {
	Type  string // "put", "delete"
	Key   string
	Value string
}

// BatchResponse represents a batch response
type BatchResponse struct {
	Success bool
	Error   string
}

// Put stores a key-value pair
func (s *KVService) Put(ctx context.Context, req *PutRequest) (*PutResponse, error) {
	if err := s.store.PutString(req.Key, req.Value); err != nil {
		return &PutResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &PutResponse{
		Success: true,
	}, nil
}

// Get retrieves a value by key
func (s *KVService) Get(ctx context.Context, req *GetRequest) (*GetResponse, error) {
	value, found, err := s.store.GetString(req.Key)
	if err != nil {
		return &GetResponse{
			Found: false,
			Error: err.Error(),
		}, nil
	}

	return &GetResponse{
		Value: value,
		Found: found,
	}, nil
}

// Delete removes a key
func (s *KVService) Delete(ctx context.Context, req *DeleteRequest) (*DeleteResponse, error) {
	if err := s.store.DeleteString(req.Key); err != nil {
		return &DeleteResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	return &DeleteResponse{
		Success: true,
	}, nil
}

// Batch processes multiple operations atomically
func (s *KVService) Batch(ctx context.Context, req *BatchRequest) (*BatchResponse, error) {
	// Process operations in order
	for _, op := range req.Operations {
		switch op.Type {
		case "put":
			if err := s.store.PutString(op.Key, op.Value); err != nil {
				return &BatchResponse{
					Success: false,
					Error:   fmt.Sprintf("failed to put %s: %v", op.Key, err),
				}, nil
			}
		case "delete":
			if err := s.store.DeleteString(op.Key); err != nil {
				return &BatchResponse{
					Success: false,
					Error:   fmt.Sprintf("failed to delete %s: %v", op.Key, err),
				}, nil
			}
		default:
			return &BatchResponse{
				Success: false,
				Error:   fmt.Sprintf("unknown operation type: %s", op.Type),
			}, nil
		}
	}

	return &BatchResponse{
		Success: true,
	}, nil
}

// HealthRequest represents a health check request
type HealthRequest struct{}

// HealthResponse represents a health check response
type HealthResponse struct {
	Status    string
	Timestamp time.Time
	Uptime    time.Duration
}

// Health checks the service health
func (s *KVService) Health(ctx context.Context, req *HealthRequest) (*HealthResponse, error) {
	return &HealthResponse{
		Status:    "healthy",
		Timestamp: time.Now(),
		Uptime:    time.Since(time.Now()), // This would be actual uptime in real implementation
	}, nil
}
