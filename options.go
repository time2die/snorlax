package snorlax

import (
	"crypto/tls"
	"os"
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

func URL(url string) func(*opts) {
	return func(o *opts) {
		o.url = url
	}
}

func TLS(t *tls.Config) func(*opts) {
	return func(o *opts) {
		o.tls = t
	}
}

func DeclareExchange(name string) func(*opts) {
	return func(o *opts) {
		o.exchanges = append(o.exchanges, exchange{
			name: name,
		})
	}
}

type pubOpts struct {
	exchange string
	queue    string
	durable  bool
}

type PublisherOption func(*pubOpts)

func newDefPubOpts() pubOpts {
	return pubOpts{
		exchange: "amq.topic",
		durable:  true,
	}
}

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

type subOpts struct {
	exchange string
	queue    string
	durable  bool
	autoAck  bool
}

type SubscriberOption func(*subOpts)

func newDefSubOpts() subOpts {
	return subOpts{
		exchange: "amq.topic",
		durable:  true,
	}
}

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
