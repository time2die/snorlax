// Package snorlax provides tools for message query based microservices:
//     - Configuration through environment;
//     - Logging through standard log;
//     - Use only one transport = amqp through RabbitMQ;
//     - Use only one exchange type for all = topic;
//
// No registry, service discovery and other smart things.
// Let k8s do the rest.
package snorlax

import (
	"context"
	"reflect"

	"github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"
)

var conn *amqp.Connection

// Init Snorlax: create connection to RabbitMQ, declare exchanges if any etc.
func Init(optsf ...Option) error {
	var err error

	opts := newOpts()

	for _, o := range optsf {
		o(&opts)
	}

	conn, err = amqp.DialTLS(
		opts.url,
		opts.tls,
	)

	if err != nil {
		return err
	}

	if len(opts.exchanges) == 0 {
		return nil
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	for _, ex := range opts.exchanges {
		if err := ch.ExchangeDeclare(
			ex.name,
			amqp.ExchangeTopic,
			true,  // durable
			false, // autoDelete
			false, // internal
			false, // noWait
			nil,
		); err != nil {
			return err
		}
	}

	return ch.Close()
}

// Close amqp connection.
func Close() error {
	if conn == nil {
		return nil
	}

	return conn.Close()
}

// Publisher publish proto messages.
type Publisher struct {
	opts pubOpts
	ch   *amqp.Channel
}

// NewPublisher creates channel to RabbitMQ.
func NewPublisher(optsf ...PublisherOption) (*Publisher, error) {
	opts := newDefPubOpts()

	for _, o := range optsf {
		o(&opts)
	}

	ch, err := conn.Channel()

	if err != nil {
		return nil, err
	}

	if _, err := ch.QueueDeclare(
		opts.queue,
		opts.durable,
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,
	); err != nil {
		return nil, err
	}

	return &Publisher{
		opts: opts,
		ch:   ch,
	}, nil
}

// Publish proto message to specific topic. Messages always persistent.
// Message type will be in header in "MessageType" key.
func (p *Publisher) Publish(ctx context.Context, topic string, msg proto.Message) error {
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	return p.ch.Publish(
		p.opts.exchange,
		topic,
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			Headers: map[string]interface{}{
				"MessageType": proto.MessageName(msg),
			},
			ContentType:  "application/protobuf",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
}

// Subscriber subscribes to exchange's topic.
type Subscriber struct {
	opts subOpts
	ch   *amqp.Channel
}

// NewSubscriber creates channel to RabbitMQ
func NewSubscriber(optsf ...SubscriberOption) (*Subscriber, error) {
	opts := newDefSubOpts()

	for _, o := range optsf {
		o(&opts)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	if _, err := ch.QueueDeclare(
		opts.queue,
		opts.durable,
		false, // autoDelete
		false, // exclusive
		false, // noWait
		nil,
	); err != nil {
		return nil, err
	}

	return &Subscriber{
		opts: opts,
		ch:   ch,
	}, nil
}

// Handler handles messages from subscriber. Msg always proto.Message and will be parsed
// to specific one based on "MessageType" header.
type Handler func(ctx context.Context, msg interface{}) error

// Subscribe to topic with handler. Blocking function.
// If message can't be parsed with MessageType header into proto, message will be rejected.
// On error in handler will be rejected and requeued.
func (s *Subscriber) Subscribe(ctx context.Context, topic string, h Handler) error {
	if err := s.ch.QueueBind(
		s.opts.queue,
		topic,
		s.opts.exchange,
		false, // noWait,
		nil,
	); err != nil {
		return err
	}

	chd, err := s.ch.Consume(
		s.opts.queue,
		topic,
		s.opts.autoAck,
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,
	)

	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case d := <-chd:
			t, ok := d.Headers["MessageType"]

			if !ok {
				_ = d.Reject(false)

				continue
			}

			msgt := proto.MessageType(t.(string))

			if msgt == nil {
				_ = d.Reject(false)

				continue
			}

			msg := reflect.New(msgt.Elem()).Interface().(proto.Message)

			if err := proto.Unmarshal(d.Body, msg); err != nil {
				continue
			}

			if err := h(ctx, msg); err != nil {
				_ = d.Reject(true)

				continue
			}

			_ = d.Ack(false)
		}
	}
}
