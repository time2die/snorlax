package snorlax

import (
	"context"
	"reflect"

	"github.com/golang/protobuf/proto"
	"github.com/streadway/amqp"
)

var conn *amqp.Connection

func Init(optsf ...SnorlaxOption) error {
	var err error

	opts := newSnrlxOpts()

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

	if len(opts.exchnages) == 0 {
		return nil
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	for _, ex := range opts.exchnages {
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

func Close() error {
	if conn == nil {
		return nil
	}

	return conn.Close()
}

type Publisher struct {
	opts pubOpts
	ch   *amqp.Channel
}

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
				"messageType": proto.MessageName(msg),
			},
			ContentType:  "application/protobuf",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	)
}

type Subscriber struct {
	opts subOpts
	ch   *amqp.Channel
}

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

type handler func(ctx context.Context, msg interface{}) error

func (s *Subscriber) Subscribe(ctx context.Context, topic string, h handler) error {

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
			t, ok := d.Headers["messageType"]

			if !ok {
				d.Reject(false)

				continue
			}

			msgt := proto.MessageType(t.(string))

			if msgt == nil {
				d.Reject(false)

				continue
			}

			msg := reflect.New(msgt.Elem()).Interface().(proto.Message)

			if err := proto.Unmarshal(d.Body, msg); err != nil {
				continue
			}

			if err := h(ctx, msg); err != nil {
				d.Reject(true)
			} else {
				d.Ack(false)
			}
		}
	}
}
