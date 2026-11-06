package main

import (
	"context"
	"io"
	"log"
	"os"
	"os/signal"

	kvv1 "github.com/dhiliphari/kvstore/gen/kv/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	key := "name"
	if len(os.Args) > 1 {
		key = os.Args[1]
	}

	conn, err := grpc.NewClient("localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	defer conn.Close()

	// Cancel the stream cleanly on Ctrl+C
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	stream, err := kvv1.NewKeyValueServiceClient(conn).Watch(ctx, &kvv1.WatchRequest{Key: key})
	if err != nil {
		log.Fatalf("watch failed: %v", err)
	}

	log.Printf("watching %q (Ctrl+C to stop)", key)
	for {
		ev, err := stream.Recv()
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Printf("stream ended: %v", err)
			return
		}
		log.Printf("%s %s = %q", ev.GetType(), ev.GetKey(), ev.GetValue())
	}
}
