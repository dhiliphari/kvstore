package server

import (
	"context"
	"fmt"
	"sync"
	"testing"

	kvv1 "github.com/dhiliphari/kvstore/gen/kv/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPut_Validation(t *testing.T) {
	tests := []struct {
		name string
		req  *kvv1.PutRequest
		want codes.Code
	}{
		{"valid", &kvv1.PutRequest{Key: "a", Value: "1"}, codes.OK},
		{"invalid", &kvv1.PutRequest{Key: "", Value: "1"}, codes.InvalidArgument},
		{"empty value is allowed", &kvv1.PutRequest{Key: "b"}, codes.OK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			_, err := s.Put(context.Background(), tt.req)
			if got := status.Code(err); got != tt.want {
				t.Errorf("Put() code = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestGetAndDelete(t *testing.T) {
	s := New()
	ctx := context.Background()

	if _, err := s.Get(ctx, &kvv1.GetRequest{Key: "x"}); status.Code(err) != codes.NotFound {
		t.Fatalf("Get on missing key: got %v, want NotFound", err)
	}

	if _, err := s.Put(ctx, &kvv1.PutRequest{Key: "x", Value: "42"}); status.Code(err) != codes.OK {
		t.Fatalf("Put: %v", err)
	}

	resp, err := s.Get(ctx, &kvv1.GetRequest{Key: "x"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp.Value != "42" {
		t.Errorf("Get value = %q, want %q", resp.GetValue(), "42")
	}

	if _, err := s.Delete(ctx, &kvv1.DeleteRequest{Key: "x"}); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Delete(ctx, &kvv1.DeleteRequest{Key: "x"}); status.Code(err) != codes.NotFound {
		t.Errorf("second Delete: got %v, want NotFound", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := New()
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", i%5)
			_, _ = s.Put(ctx, &kvv1.PutRequest{Key: key, Value: "v"})
			_, _ = s.Get(ctx, &kvv1.GetRequest{Key: key})
		}(i)
	}
	wg.Wait()
}
