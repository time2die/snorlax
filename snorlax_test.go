package snorlax_test

import (
	"context"
	"crypto/tls"
	"testing"

	"github.com/go-snorlax/snorlax"
	pb "github.com/go-snorlax/snorlax/testdata"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
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

	assert.Equal(snorlax.SubStatus{
		Exchange:    "testing.pubsub",
		Queue:       "queue.testing.sub",
		Topic:       "testing.new",
		MessageType: "event.Event",
	}, stat)
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
