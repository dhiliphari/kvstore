package main

import (
	"log"
	"net"

	kvv1 "github.com/dhiliphari/kvstore/gen/kv/v1"
	"github.com/dhiliphari/kvstore/internal/interceptors"
	"github.com/dhiliphari/kvstore/internal/server"
	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	apiKeys := map[string]string{
		"dev-secret": "dev-client",
	}

	grpcSrv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			interceptors.UnaryRecovery,
			interceptors.UnaryLogging,
			interceptors.UnaryAuth(apiKeys),
		),
		grpc.ChainStreamInterceptor(
			interceptors.StreamRecovery,
			interceptors.StreamLogging,
			interceptors.StreamAuth(apiKeys),
		),
	)
	kvv1.RegisterKeyValueServiceServer(grpcSrv, server.New())

	log.Println("KV Server listening on :50051")
	if err = grpcSrv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
