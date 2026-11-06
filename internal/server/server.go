package server

import (
	"context"
	"sync"

	kvv1 "github.com/dhiliphari/kvstore/gen/kv/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type KVServer struct {
	kvv1.UnimplementedKeyValueServiceServer

	mu   sync.RWMutex
	data map[string]string
}

func New() *KVServer {
	return &KVServer{}
}

func (s *KVServer) Put(ctx context.Context, req *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	s.mu.Lock()
	s.data[req.GetKey()] = req.GetValue()
	s.mu.Unlock()

	return &kvv1.PutResponse{}, nil
}

func (s *KVServer) Get(ctx context.Context, req *kvv1.GetRequest) (*kvv1.GetResponse, error) {
	s.mu.RLock()
	val, ok := s.data[req.GetKey()]
	s.mu.RUnlock()

	if !ok {
		return nil, status.Errorf(codes.NotFound, "key %q not found", req.GetKey())
	}

	return &kvv1.GetResponse{Value: val}, nil
}

func (s *KVServer) Delete(ctx context.Context, req *kvv1.DeleteRequest) (*kvv1.DeleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[req.GetKey()]; !ok {
		return nil, status.Errorf(codes.NotFound, "key %q not found", req.GetKey())
	}
	delete(s.data, req.GetKey())
	return &kvv1.DeleteResponse{}, nil
}
