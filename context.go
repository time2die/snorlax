package snorlax

import (
	"context"
)

var key = &struct{}{}

type Headers map[string]interface{}

func FromContext(ctx context.Context) Headers {
	return ctx.Value(key).(Headers)
}

func ToContext(ctx context.Context, h Headers) context.Context {
	return context.WithValue(ctx, key, h)
}
