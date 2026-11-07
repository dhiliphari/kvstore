package interceptors

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

func UnaryLogging(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req)
	log.Printf("unary %s code=%s duration=%s", info.FullMethod, status.Code(err), time.Since(start))
	return resp, err
}

func StreamLogging(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()
	log.Printf("stream %s started", info.FullMethod)
	err := handler(srv, ss)
	log.Printf("stream %s ended code=%s duration=%s", info.FullMethod, status.Code(err), time.Since(start))
	return err
}
