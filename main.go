package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	mode    = flag.String("mode", "amo", "delivery semantics: amo|alo|eos")
	n       = flag.Int("n", 100_000, "messages to produce")
	port    = flag.Int("port", 0, "broker port (default: amo 9092, alo 9093, eos 9094)")
	txnSize = flag.Int("txn-size", 10_000, "eos: records per transaction")
)

const topic = "harness"

func main() {
	flag.Parse()

	defPort := map[string]int{"amo": 9092, "alo": 9093, "eos": 9094}[*mode]
	if defPort == 0 {
		panic("mode must be amo|alo|eos")
	}
	if *port == 0 {
		*port = defPort
	}
	if *mode == "eos" && *txnSize <= 0 {
		panic("txn-size must be > 0")
	}

	ctx := context.Background()
	cl := newProducer(*mode, *port)
	defer cl.Close()

	ensureTopic(ctx, cl)
	warmup(ctx, cl)
	produce(ctx, cl)
	consume(*mode, *port)
}

func newProducer(mode string, port int) *kgo.Client {
	opts := []kgo.Opt{
		kgo.SeedBrokers(fmt.Sprintf("localhost:%d", port)),
		kgo.ProduceRequestTimeout(500 * time.Millisecond),
		kgo.RequestTimeoutOverhead(500 * time.Millisecond),
	}
	switch mode {
	case "amo":
		opts = append(opts,
			kgo.DisableIdempotentWrite(),
			kgo.RequiredAcks(kgo.NoAck()),
			kgo.RecordRetries(0),
		)
	case "alo":
		opts = append(opts,
			kgo.DisableIdempotentWrite(),
			kgo.RequiredAcks(kgo.AllISRAcks()),
			kgo.RecordRetries(10),
		)
	case "eos":
		opts = append(opts, kgo.TransactionalID("harness-eos"))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	return cl
}

func ensureTopic(ctx context.Context, cl *kgo.Client) {
	deadline := time.Now().Add(60 * time.Second)
	for {
		_, err := kadm.NewClient(cl).CreateTopic(ctx, 1, 1, nil, topic)
		if err == nil || errors.Is(err, kerr.TopicAlreadyExists) {
			fmt.Printf("topic %q ready (port %d, mode %s)\n", topic, *port, *mode)
			return
		}
		if time.Now().After(deadline) {
			panic("broker never settled: " + err.Error())
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func warmup(ctx context.Context, cl *kgo.Client) {
	if *mode != "eos" {
		return
	}
	for {
		if err := cl.BeginTransaction(); err != nil {
			panic(err)
		}
		cl.Produce(ctx, &kgo.Record{Topic: topic, Value: []byte("warmup")}, nil)
		cl.Flush(ctx)
		if err := cl.EndTransaction(ctx, kgo.TryAbort); err == nil {
			fmt.Println("txn coordinator warm")
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func produce(ctx context.Context, cl *kgo.Client) {
	start := time.Now()

	chunk := *n
	if *mode == "eos" {
		chunk = *txnSize
	}

	errs := make(chan error, *n)
	rec := func(_ *kgo.Record, err error) { errs <- err }

	for i := 0; i < *n; {
		if *mode == "eos" {
			if err := cl.BeginTransaction(); err != nil {
				panic(err)
			}
		}
		end := i + chunk
		if end > *n {
			end = *n
		}
		for ; i < end; i++ {
			cl.Produce(ctx, &kgo.Record{
				Topic: topic,
				Value: []byte(fmt.Sprintf(`{"i":%d}`, i)),
			}, rec)
		}
		if *mode == "eos" {
			cl.Flush(ctx)
			if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
				panic(err)
			}
		}
	}

	for i := 0; i < *n; i++ {
		if err := <-errs; err != nil && *mode != "amo" {
			panic(err)
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("produce(%s): %d msgs in %v (%.0f msgs/s)\n", *mode, *n, elapsed, float64(*n)/elapsed.Seconds())
}

func consume(mode string, port int) {
	start := time.Now()

	opts := []kgo.Opt{
		kgo.SeedBrokers(fmt.Sprintf("localhost:%d", port)),
		kgo.ConsumeTopics(topic),
	}
	if mode == "eos" {
		opts = append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var count int64
	for count < int64(*n) {
		fetches := cl.PollRecords(ctx, 10_000)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			panic(fmt.Sprintf("consume timed out with %d/%d msgs", count, *n))
		}
		fetches.EachError(func(t string, p int32, err error) {
			panic(err)
		})
		fetches.EachRecord(func(*kgo.Record) { count++ })
	}

	elapsed := time.Since(start)
	fmt.Printf("consume(%s): %d msgs in %v (%.0f msgs/s)\n", mode, count, elapsed, float64(count)/elapsed.Seconds())
}
