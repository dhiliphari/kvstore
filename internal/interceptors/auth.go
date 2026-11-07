package interceptors

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type clientNameKey struct{}

func authenticate(ctx context.Context, keys map[string]string) (context.Context, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing metadata")
	}

	values := md.Get("x-api-key")
	if len(values) == 0 {
		return nil, status.Error(codes.Unauthenticated, "missing API key")
	}

	name, ok := keys[values[0]]
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "invalid API key")
	}

	return context.WithValue(ctx, clientNameKey{}, name), nil
}

func UnaryAuth(keys map[string]string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, err := authenticate(ctx, keys)
		if err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

type wrappedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (ws *wrappedStream) Context() context.Context {
	return ws.ctx
}

func StreamAuth(keys map[string]string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx, err := authenticate(ss.Context(), keys)
		if err != nil {
			return err
		}
		return handler(srv, &wrappedStream{ServerStream: ss, ctx: ctx})
	}
}
