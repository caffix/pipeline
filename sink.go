package pipeline

import "context"

// OutputSink is implemented by types that can operate as the tail of a pipeline.
type OutputSink interface {
	// Consume processes a Data instance that has been emitted out of
	// a Pipeline instance.
	Consume(context.Context, Data) error
}

// SinkFunc is an adapter to allow the use of plain functions as OutputSink instances.
type SinkFunc func(context.Context, Data) error

// Consume calls f(ctx, data)
func (f SinkFunc) Consume(ctx context.Context, data Data) error {
	return f(ctx, data)
}
