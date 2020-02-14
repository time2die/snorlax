package snorlax_test

import (
	"context"
	"crypto/tls"
	"errors"
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
		snorlax.SubscriberWrapper(subWrap(t)),
	)

	assert.NoError(err)

	assert.NoError(sub.Subscribe(
		context.TODO(),
		"testing.#",
		handler(t),
	))

	t.Run("Snorlax publisher", func(t *testing.T) {
		pub, err := s.NewPublisher(
			snorlax.PublisherExchange(exchange),
			snorlax.PublisherQueue("queue.testing.pub"),
			snorlax.PublisherNotDurable(),
			snorlax.PublisherWrapper(pubWrap(t)),
			snorlax.PublisherSource("source.test"),
		)

		assert.NoError(err)

		assert.NoError(pub.Publish(
			context.TODO(),
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

func handler(t *testing.T) func(ctx context.Context, msg proto.Message) error {
	return func(ctx context.Context, raw proto.Message) error {
		assert := assert.New(t)

		msg, ok := raw.(*pb.Event)

		assert.True(ok)
		if !assert.NotNil(raw) {
			return errors.New("empty message")
		}

		assert.Equal("test message", msg.Body)

		assert.Equal(snorlax.Headers{
			"Exchange":    "testing.pubsub",
			"Topic":       "testing.new",
			"MessageType": "event.Event",
			"Source":      "source.test",
		}, snorlax.FromContext(ctx))

		return nil
	}
}

func pubWrap(t *testing.T) snorlax.PubWrapper {
	return func(fn snorlax.PubFn) snorlax.PubFn {
		return func(ctx context.Context, topic string, msg proto.Message) error {
			assert := assert.New(t)

			assert.Equal(snorlax.Headers{
				"Exchange":    "testing.pubsub",
				"Topic":       "testing.new",
				"MessageType": "event.Event",
				"Source":      "source.test",
			}, snorlax.FromContext(ctx))

			err := fn(ctx, topic, msg)

			assert.Equal("testing.new", topic)

			return err
		}
	}
}

func subWrap(t *testing.T) snorlax.SubWrapper {
	return func(fn snorlax.Handler) snorlax.Handler {
		return func(ctx context.Context, msg proto.Message) error {
			assert := assert.New(t)

			assert.Equal(snorlax.Headers{
				"Exchange":    "testing.pubsub",
				"Topic":       "testing.new",
				"MessageType": "event.Event",
				"Source":      "source.test",
			}, snorlax.FromContext(ctx))

			err := fn(ctx, msg)

			return err
		}
	}
}
