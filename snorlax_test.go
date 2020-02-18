package snorlax_test

import (
	"context"
	"crypto/tls"
	"os"
	"testing"

	"github.com/go-snorlax/snorlax"
	pb "github.com/go-snorlax/snorlax/testdata"
	"github.com/gogo/protobuf/proto"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

// docker run -p 5672:5672 rabbitmq:3.8-alpine

func TestMain(m *testing.M) {
	if os.Getenv("AMQP_URL") == "" {
		os.Setenv("AMQP_URL", "amqp://guest:guest@0.0.0.0:5672/")
	}

	os.Exit(m.Run())
}

func TestPubSub(t *testing.T) {
	assert := assert.New(t)

	const exchange = "testing.pubsub"

	s, err := snorlax.New(
		snorlax.DeclareExchange(exchange),
		snorlax.TLS(&tls.Config{}),
	)

	defer s.Close()

	assert.NoError(err)

	sub, err := s.NewSubscriber(
		snorlax.SubscriberExchange(exchange),
		snorlax.SubscriberQueue("queue.testing.sub"),
		snorlax.SubscriberNotDurable(),
		snorlax.SubscriberWrapper(subWrap()),
	)

	assert.NoError(err)

	assert.NoError(sub.Subscribe(
		context.Background(),
		"testing.#",
		handler(),
	))

	t.Run("Snorlax publisher", func(t *testing.T) {
		pub, err := s.NewPublisher(
			snorlax.PublisherExchange(exchange),
			snorlax.PublisherQueue("queue.testing.pub"),
			snorlax.PublisherNotDurable(),
			snorlax.PublisherWrapper(pubWrap()),
			snorlax.PublisherSource("source.test"),
		)

		assert.NoError(err)

		assert.NoError(pub.Publish(
			context.Background(),
			"testing.new", &pb.Event{
				Body: "test message",
			}))
	})

	t.Run("Json publisher", func(t *testing.T) {
		conn, err := amqp.Dial(os.Getenv("AMQP_URL"))
		if err != nil {
			t.Fatal(err)
		}

		ch, err := conn.Channel()
		if err != nil {
			t.Fatal(err)
		}

		if err := ch.Publish(
			exchange,
			"testing.new",
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Headers: map[string]interface{}{
					"Exchange":    "testing.pubsub",
					"Topic":       "testing.new",
					"Source":      "source.test",
					"MessageType": "event.Event",
				},
				Body: []byte(`{"body":"test message"}`),
			},
		); err != nil {
			t.Fatal(err)
		}
	})
}

func handler() func(ctx context.Context, msg proto.Message) error {
	return func(ctx context.Context, raw proto.Message) error {
		return nil
	}
}

func pubWrap() snorlax.PubWrapper {
	return func(fn snorlax.PubFn) snorlax.PubFn {
		return func(ctx context.Context, topic string, msg proto.Message) error {
			return nil
		}
	}
}

func subWrap() snorlax.SubWrapper {
	return func(fn snorlax.Handler) snorlax.Handler {
		return func(ctx context.Context, msg proto.Message) error {
			return nil
		}
	}
}
