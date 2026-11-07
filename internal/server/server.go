package server

import (
	"context"
	"sync"

	kvv1 "github.com/dhiliphari/kvstore/gen/kv/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type KVServer struct {
	kvv1.UnimplementedKeyValueServiceServer

	mu       sync.RWMutex
	data     map[string]string
	watchers map[string]map[chan *kvv1.WatchEvent]struct{}
}

func New() *KVServer {
	return &KVServer{
		data:     make(map[string]string),
		watchers: make(map[string]map[chan *kvv1.WatchEvent]struct{}),
	}
}

func (s *KVServer) Put(ctx context.Context, req *kvv1.PutRequest) (*kvv1.PutResponse, error) {
	if req.GetKey() == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	s.mu.Lock()
	s.data[req.GetKey()] = req.GetValue()
	s.notifyChange(&kvv1.WatchEvent{
		Type:  kvv1.WatchEvent_EVENT_TYPE_PUT,
		Key:   req.GetKey(),
		Value: req.GetValue(),
	})
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

	s.notifyChange(&kvv1.WatchEvent{
		Type: kvv1.WatchEvent_EVENT_TYPE_DELETE,
		Key:  req.GetKey(),
	})

	return &kvv1.DeleteResponse{}, nil
}

func (s *KVServer) Watch(req *kvv1.WatchRequest, stream kvv1.KeyValueService_WatchServer) error {
	key := req.GetKey()
	ch := make(chan *kvv1.WatchEvent, 16)

	s.mu.Lock()
	if s.watchers[key] == nil {
		s.watchers[key] = make(map[chan *kvv1.WatchEvent]struct{})
	}
	s.watchers[key][ch] = struct{}{}
	s.mu.Unlock()

	// unregister when the stream ends
	defer func() {
		s.mu.Lock()
		delete(s.watchers[key], ch)
		if len(s.watchers[key]) == 0 {
			delete(s.watchers, key)
		}
		s.mu.Unlock()
	}()

	if err := stream.SendHeader(metadata.MD{}); err != nil {
		return err
	}

	for {
		select {
		case <-stream.Context().Done():
			// client disconnected or cancelled
			return nil
		case ev := <-ch:
			if err := stream.Send(ev); err != nil {
				return err
			}
		}
	}
}

func (s *KVServer) notifyChange(ev *kvv1.WatchEvent) {
	for ch := range s.watchers[ev.GetKey()] {
		select {
		case ch <- ev:
		default:
			// Watcher's buffer is full (slow client); drop the event
			// rather than blocking every writer.
		}
	}
}
