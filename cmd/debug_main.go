package main

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cl, err := kgo.NewClient(kgo.SeedBrokers("localhost:9092"))
	if err != nil { panic(err) }
	defer cl.Close()
	admin := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	topics, err := admin.ListTopics(ctx, "orders")
	fmt.Printf("ListTopics: topics=%v err=%v\n", topics, err)
	res, err := admin.CreateTopics(ctx, 3, 1, nil, "orders")
	fmt.Printf("CreateTopics: res=%+v err=%v\n", res, err)
	topics, err = admin.ListTopics(ctx, "orders")
	fmt.Printf("after: topics=%v err=%v\n", topics, err)
}
