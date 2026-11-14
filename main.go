package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	broker  = "localhost:9092"
	topic   = "harness"
	groupID = "harness-consumer"
	numMsgs = 1000
)

func main() {
	ctx := context.Background()

	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	ensureTopic(ctx, cl)
	produce(ctx, cl)
	consume(ctx)
}

func ensureTopic(ctx context.Context, cl *kgo.Client) {
	adm := kadm.NewClient(cl)
	_, err := adm.CreateTopic(ctx, 1, 1, nil, topic)
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		panic(err)
	}
	fmt.Printf("topic %q ready\n", topic)
}

func produce(ctx context.Context, cl *kgo.Client) {
	start := time.Now()

	acks := make(chan error, numMsgs)
	for i := 0; i < numMsgs; i++ {
		cl.Produce(ctx, &kgo.Record{
			Topic: topic,
			Value: []byte(fmt.Sprintf(`{"i":%d}`, i)),
		}, func(_ *kgo.Record, err error) {
			acks <- err
		})
	}
	for i := 0; i < numMsgs; i++ {
		if err := <-acks; err != nil {
			panic(err)
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("produce: %d msgs in %v (%.0f msgs/s)\n", numMsgs, elapsed, float64(numMsgs)/elapsed.Seconds())
}

func consume(ctx context.Context) {
	start := time.Now()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	recs := make([]*kgo.Record, 0, numMsgs)
	for len(recs) < numMsgs {
		fetches := cl.PollRecords(ctx, numMsgs)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			panic(fmt.Sprintf("consume timed out with %d/%d msgs", len(recs), numMsgs))
		}
		fetches.EachError(func(t string, p int32, err error) {
			panic(err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			recs = append(recs, r)
		})
	}
	if err := cl.CommitRecords(context.Background(), recs...); err != nil {
		panic(err)
	}

	elapsed := time.Since(start)
	fmt.Printf("consume: %d msgs in %v (%.0f msgs/s)\n", numMsgs, elapsed, float64(numMsgs)/elapsed.Seconds())
}
