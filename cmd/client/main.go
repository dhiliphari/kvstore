package main

import (
	"context"
	"log"
	"time"

	kvv1 "github.com/dhiliphari/kvstore/gen/kv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func main() {
	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	defer conn.Close()

	client := kvv1.NewKeyValueServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if _, err := client.Put(ctx, &kvv1.PutRequest{Key: "lang", Value: "go"}); err != nil {
		log.Fatalf("Put failed: %v", err)
	}

	resp, err := client.Get(ctx, &kvv1.GetRequest{Key: "lang"})
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	log.Printf("lang = %s", resp.GetValue())

	_, err = client.Get(ctx, &kvv1.GetRequest{Key: "nope"})
	if status.Code(err) == codes.NotFound {
		log.Printf("handled not-found gracefully: %s", status.Convert(err).Message())
	}
}
