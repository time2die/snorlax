package snorlax

import (
	"context"
	"crypto/tls"
	"os"

	"github.com/gogo/protobuf/proto"
)

type exchange struct {
	name string
}

type opts struct {
	url       string
	exchanges []exchange
	tls       *tls.Config
}

type Option func(*opts)

func newOpts() opts {
	return opts{
		url: os.Getenv("AMQP_URL"),
		tls: &tls.Config{},
	}
}

// URL to RabbitMQ. Ex: amqp://guest:guest@0.0.0.0:5672/
// AMQP_URL will be used if this option not provided.
func URL(url string) func(*opts) {
	return func(o *opts) {
		o.url = url
	}
}

// TLS config to RabbitMQ connection.
func TLS(t *tls.Config) func(*opts) {
	return func(o *opts) {
		o.tls = t
	}
}

// DeclareExchange on Snorlax initialization.
// Always topic and always durable.
func DeclareExchange(name string) func(*opts) {
	return func(o *opts) {
		o.exchanges = append(o.exchanges, exchange{
			name: name,
		})
	}
}

type PubFn func(context.Context, string, proto.Message) error

type PubWrapper func(PubFn) PubFn

type pubOpts struct {
	exchange string
	queue    string
	durable  bool

	wrappers []PubWrapper
}

type PublisherOption func(*pubOpts)

func newDefPubOpts() pubOpts {
	return pubOpts{
		exchange: "amq.topic",
		durable:  true,
	}
}

// PublisherExchange. Defaults to "amq.topic".
func PublisherExchange(exchange string) func(*pubOpts) {
	return func(o *pubOpts) {
		o.exchange = exchange
	}
}

func PublisherQueue(queue string) func(*pubOpts) {
	return func(o *pubOpts) {
		o.queue = queue
	}
}

func PublisherNotDurable() func(*pubOpts) {
	return func(o *pubOpts) {
		o.durable = false
	}
}

func PublisherWrapper(w PubWrapper) func(*pubOpts) {
	return func(o *pubOpts) {
		o.wrappers = append(o.wrappers, w)
	}
}

type SubFn func(context.Context, string, Handler) <-chan SubStatus

type SubWrapper func(SubFn) SubFn

type subOpts struct {
	exchange string
	queue    string
	durable  bool
	autoAck  bool

	wrappers []SubWrapper
}

type SubscriberOption func(*subOpts)

func newDefSubOpts() subOpts {
	return subOpts{
		exchange: "amq.topic",
		durable:  true,
	}
}

// SubscriberExchange. Defaults to "amq.topic".
func SubscriberExchange(exchange string) func(*subOpts) {
	return func(o *subOpts) {
		o.exchange = exchange
	}
}

func SubscriberNotDurable() func(*subOpts) {
	return func(o *subOpts) {
		o.durable = false
	}
}

func SubscriberQueue(queue string) func(*subOpts) {
	return func(o *subOpts) {
		o.queue = queue
	}
}

func SubscriberAutoAck() func(*subOpts) {
	return func(o *subOpts) {
		o.autoAck = true
	}
}

func SubscriberWrapper(w SubWrapper) func(*subOpts) {
	return func(o *subOpts) {
		o.wrappers = append(o.wrappers, w)
	}
}
