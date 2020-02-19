package snorlax_test

import (
	"context"
	"crypto/tls"
	"os"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"github.com/time2die/snorlax"
	pb "github.com/time2die/snorlax/testdata"
)

// docker run -p 5672:5672 rabbitmq:3.8-alpine

func TestPubSub(t *testing.T) {
	assert := assert.New(t)

	const exchange = "testing.pubsub"

	s, err := snorlax.New(
		snorlax.DeclareExchange(exchange),
		snorlax.TLS(&tls.Config{}),
	)

	defer func() {
		_ = s.Close()
	}()

	assert.NoError(err)

	sub, err := s.NewSubscriber(
		snorlax.SubscriberExchange(exchange),
		snorlax.SubscriberQueue("queue.testing.sub"),
		snorlax.SubscriberNotDurable(),
		snorlax.SubscriberWrapper(subWrap(t)),
	)

	assert.NoError(err)

	statc := sub.Subscribe(
		context.TODO(),
		"testing.#",
		handler(t),
	)

	wantStat := snorlax.SubStatus{
		Exchange:    "testing.pubsub",
		Queue:       "queue.testing.sub",
		Topic:       "testing.new",
		MessageType: "event.Event",
	}

	t.Run("Snorlax publisher", func(t *testing.T) {
		pub, err := s.NewPublisher(
			snorlax.PublisherExchange(exchange),
			snorlax.PublisherQueue("queue.testing.pub"),
			snorlax.PublisherNotDurable(),
			snorlax.PublisherWrapper(pubWrap(t)),
		)

		assert.NoError(err)

		assert.NoError(pub.Publish(
			context.TODO(),
			"testing.new", &pb.Event{
				Body: "test message",
			}))

		stat := <-statc

		wantStat.ContentType = "application/protobuf"

		assert.Equal(wantStat, stat)
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
					"MessageType": "event.Event",
				},
				Body: []byte(`{"body":"test message"}`),
			},
		); err != nil {
			t.Fatal(err)
		}

		stat := <-statc

		wantStat.ContentType = "application/json"

		assert.Equal(wantStat, stat)
	})
}

func handler(t *testing.T) func(ctx context.Context, msg interface{}) error {
	return func(ctx context.Context, raw interface{}) error {
		assert := assert.New(t)

		msg, ok := raw.(*pb.Event)

		assert.True(ok)
		assert.Equal("test message", msg.Body)

		return nil
	}
}

func pubWrap(t *testing.T) snorlax.PubWrapper {
	return func(fn snorlax.PubFn) snorlax.PubFn {
		return func(ctx context.Context, topic string, msg proto.Message) error {
			assert := assert.New(t)

			err := fn(ctx, topic, msg)

			assert.Equal("testing.new", topic)

			return err
		}
	}
}

func subWrap(t *testing.T) snorlax.SubWrapper {
	return func(fn snorlax.SubFn) snorlax.SubFn {
		return func(ctx context.Context, topic string, h snorlax.Handler) <-chan snorlax.SubStatus {
			assert := assert.New(t)

			statc := fn(ctx, topic, h)

			assert.Equal("testing.#", topic)

			return statc
		}
	}
}
