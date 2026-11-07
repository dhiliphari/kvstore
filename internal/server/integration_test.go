package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	kvv1 "github.com/dhiliphari/kvstore/gen/kv/v1"
	"github.com/dhiliphari/kvstore/internal/interceptors"
	"github.com/dhiliphari/kvstore/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const testAPIKey = "test-key"

func newTestServer(t *testing.T) *bufconn.Listener {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	keys := map[string]string{testAPIKey: "test-client"}

	s := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			interceptors.UnaryRecovery,
			interceptors.UnaryLogging,
			interceptors.UnaryAuth(keys),
		),
		grpc.ChainStreamInterceptor(
			interceptors.StreamRecovery,
			interceptors.StreamLogging,
			interceptors.StreamAuth(keys),
		),
	)

	kvv1.RegisterKeyValueServiceServer(s, server.New())

	go func() { _ = s.Serve(lis) }()
	t.Cleanup(s.Stop)

	return lis
}

func newClient(t *testing.T, lis *bufconn.Listener, apiKey string) kvv1.KeyValueServiceClient {
	t.Helper()

	opts := []grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	if apiKey != "" {
		opts = append(opts,
			grpc.WithChainUnaryInterceptor(interceptors.UnaryClientAuth(apiKey)),
			grpc.WithChainStreamInterceptor(interceptors.StreamClientAuth(apiKey)),
		)
	}

	conn, err := grpc.NewClient("passthrough:///bufnet", opts...)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return kvv1.NewKeyValueServiceClient(conn)
}

func TestPutGet(t *testing.T) {
	client := newClient(t, newTestServer(t), testAPIKey)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := client.Put(ctx, &kvv1.PutRequest{Key: "lang", Value: "go"}); err != nil {
		t.Fatalf("Put: %v", err)
	}

	resp, err := client.Get(ctx, &kvv1.GetRequest{Key: "lang"})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if resp.GetValue() != "go" {
		t.Errorf("value = %q, want %q", resp.GetValue(), "go")
	}

	_, err = client.Get(ctx, &kvv1.GetRequest{Key: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("Get missing: got %v, want NotFound", err)
	}
}

func TestAuthentication(t *testing.T) {
	lis := newTestServer(t)

	tests := []struct {
		name   string
		apiKey string
		want   codes.Code
	}{
		{"no key", "", codes.Unauthenticated},
		{"wrong key", "nope", codes.Unauthenticated},
		{"valid key", testAPIKey, codes.OK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newClient(t, lis, tt.apiKey)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			_, err := client.Put(ctx, &kvv1.PutRequest{Key: "k", Value: "v"})
			if got := status.Code(err); got != tt.want {
				t.Errorf("code = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestWatch(t *testing.T) {
	client := newClient(t, newTestServer(t), testAPIKey)

	// One deadline for the whole test: if any Recv hangs, the test fails instead of blocking forever.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stream, err := client.Watch(ctx, &kvv1.WatchRequest{Key: "k"})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}

	// Block until the server confirms the watcher is registered.
	if _, err := stream.Header(); err != nil {
		t.Fatalf("Header: %v", err)
	}

	// A write to a different key must not be delivered to this watcher.
	if _, err := client.Put(ctx, &kvv1.PutRequest{Key: "other", Value: "x"}); err != nil {
		t.Fatalf("Put other: %v", err)
	}
	if _, err := client.Put(ctx, &kvv1.PutRequest{Key: "k", Value: "v1"}); err != nil {
		t.Fatalf("Put k: %v", err)
	}

	ev, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if ev.GetType() != kvv1.WatchEvent_EVENT_TYPE_PUT || ev.GetKey() != "k" || ev.GetValue() != "v1" {
		t.Errorf("got event %v, want PUT k=v1", ev)
	}

	if _, err := client.Delete(ctx, &kvv1.DeleteRequest{Key: "k"}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	ev, err = stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if ev.GetType() != kvv1.WatchEvent_EVENT_TYPE_DELETE || ev.GetKey() != "k" {
		t.Errorf("got event %v, want DELETE k", ev)
	}
}
