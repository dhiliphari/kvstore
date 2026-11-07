package interceptors

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestUnaryRecovery(t *testing.T) {
	panicking := func(ctx context.Context, req any) (any, error) {
		panic("boom")
	}
	info := &grpc.UnaryServerInfo{FullMethod: "/test.Service/Panic"}

	_, err := UnaryRecovery(context.Background(), nil, info, panicking)
	if status.Code(err) != codes.Internal {
		t.Fatalf("got %v, want Internal", err)
	}
}

func TestUnaryAuth(t *testing.T) {
	interceptor := UnaryAuth(map[string]string{"good-key": "alice"})
	info := &grpc.UnaryServerInfo{FullMethod: "/kv.v1.KeyValueService/Get"}

	tests := []struct {
		name       string
		md         metadata.MD
		wantCode   codes.Code
		wantCalled bool
		wantName   string
	}{
		{"no metadata", nil, codes.Unauthenticated, false, ""},
		{"missing key", metadata.Pairs("other", "x"), codes.Unauthenticated, false, ""},
		{"wrong key", metadata.Pairs("x-api-key", "bad"), codes.Unauthenticated, false, ""},
		{"valid key", metadata.Pairs("x-api-key", "good-key"), codes.OK, true, "alice"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := metadata.NewIncomingContext(context.Background(), tt.md)

			called := false
			var gotName string
			handler := func(ctx context.Context, req any) (any, error) {
				called = true
				gotName, _ = ClientNameFromContext(ctx)
				return nil, nil
			}

			_, err := interceptor(ctx, nil, info, handler)

			if got := status.Code(err); got != tt.wantCode {
				t.Errorf("code = %s, want %s", got, tt.wantCode)
			}
			if called != tt.wantCalled {
				t.Errorf("handler called = %v, want %v", called, tt.wantCalled)
			}
			if gotName != tt.wantName {
				t.Errorf("client name = %q, want %q", gotName, tt.wantName)
			}
		})
	}
}
