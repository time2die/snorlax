package snorlax_test

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/go-snorlax/snorlax"
	pb "github.com/go-snorlax/snorlax/testdata"
)

// docker run -p 5672:5672 rabbitmq:3.8-alpine

func TestPubSub(t *testing.T) {
	const exchange = "testing.pubsub"

	s, err := snorlax.New(
		snorlax.DeclareExchange(exchange),
		snorlax.TLS(&tls.Config{}),
	)

	if err != nil {
		t.Fatal(err)
	}

	pub, err := s.NewPublisher(
		snorlax.PublisherExchange(exchange),
		snorlax.PublisherQueue("queue.testing.pub"),
		snorlax.PublisherNotDurable(),
	)

	if err != nil {
		t.Fatal(err)
	}

	if err := pub.Publish(
		context.TODO(),
		"testing.new", &pb.Event{
			Body: "test message",
		}); err != nil {
		t.Fatal(err)
	}

	sub, err := s.NewSubscriber(
		snorlax.SubscriberExchange(exchange),
		snorlax.SubscriberQueue("queue.testing.sub"),
		snorlax.SubscriberNotDurable(),
	)

	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := sub.Subscribe(
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
