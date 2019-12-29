package snorlax_test

import (
	"context"
	"crypto/tls"
	"log"
	"os"
	"testing"
	"time"

	"github.com/go-snorlax/snorlax"
	pb "github.com/go-snorlax/snorlax/testdata"
)

// docker run -p 5672:5672 rabbitmq:3.8-alpine

const exchange = "testing"

func TestMain(m *testing.M) {
	if err := snorlax.Init(
		snorlax.TLS(&tls.Config{
			InsecureSkipVerify: true,
		}),
		snorlax.DeclareExchange(exchange),
	); err != nil {
		log.Fatalf("error while snorlax wake up: %v", err)
	}

	st := m.Run()

	if err := snorlax.Close(); err != nil {
		log.Fatalf("error while snorlax sleep: %v", err)
	}

	os.Exit(st)
}

func TestPubSub(t *testing.T) {
	p, err := snorlax.NewPublisher(
		snorlax.PublisherExchange(exchange),
		snorlax.PublisherQueue("queue.testing.pub"),
		snorlax.PublisherNotDurable(),
	)

	if err != nil {
		t.Fatal(err)
	}

	if err := p.Publish(
		context.TODO(),
		"testing.new", &pb.Event{
			Body: "test message",
		}); err != nil {
		t.Fatal(err)
	}

	s, err := snorlax.NewSubscriber(
		snorlax.SubscriberExchange(exchange),
		snorlax.SubscriberQueue("queue.testing.sub"),
		snorlax.SubscriberNotDurable(),
	)

	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := s.Subscribe(
		ctx,
		"testing.#",
		handler(t),
	); err != nil {
		t.Fatal(err)
	}
}

func handler(t *testing.T) func(ctx context.Context, msg interface{}) error {
	return func(ctx context.Context, raw interface{}) error {
		msg, ok := raw.(*pb.Event)

		if !ok {
			t.Fatal("wrong msg type")
		}

		want := "test message"

		if msg.Body != want {
			t.Fatalf("wrong body, want: %s, had: %s", want, msg.Body)
		}

		return nil
	}
}
