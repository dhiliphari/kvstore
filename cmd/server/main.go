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

	grpcSrv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			interceptors.UnaryLogging,
		),
		grpc.StreamInterceptor(
			interceptors.StreamLogging,
		),
	)
	kvv1.RegisterKeyValueServiceServer(grpcSrv, server.New())

	log.Println("KV Server listening on :50051")
	if err = grpcSrv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
