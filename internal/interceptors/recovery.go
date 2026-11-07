package interceptors

import (
	"context"
	"log"
	"runtime/debug"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func UnaryRecovery(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
	defer func() {
		if pv := recover(); pv != nil {
			log.Printf("panic in %s: %v\n%s", info.FullMethod, pv, debug.Stack())
			err = status.Error(codes.Internal, "internal server error")
		}
	}()
	return handler(ctx, req)
}

func StreamRecovery(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) (err error) {
	defer func() {
		if pv := recover(); pv != nil {
			log.Printf("panic in %s: %v\n%s", info.FullMethod, pv, debug.Stack())
			err = status.Error(codes.Internal, "internal server error")
		}
	}()
	return handler(srv, ss)
}
