package snorlax

import (
	"crypto/tls"
	"os"
)

type exchange struct {
	name string
}

type snorlaxOpts struct {
	url       string
	exchnages []exchange
	tls       *tls.Config
}

type SnorlaxOption func(*snorlaxOpts)

func newSnrlxOpts() snorlaxOpts {
	return snorlaxOpts{
		url: os.Getenv("AMQP_URL"),
		tls: &tls.Config{},
	}
}

func URL(url string) func(*snorlaxOpts) {
	return func(o *snorlaxOpts) {
		o.url = url
	}
}

func TLS(t *tls.Config) func(*snorlaxOpts) {
	return func(o *snorlaxOpts) {
		o.tls = t
	}
}

func DeclareExchange(name string) func(*snorlaxOpts) {
	return func(o *snorlaxOpts) {
		o.exchnages = append(o.exchnages, exchange{
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
