package pipeline

import "context"

// OutputSink is implemented by types that can operate as the tail of a pipeline.
type OutputSink interface {
	// Consume processes a Data instance that has been emitted out of
	// a Pipeline instance.
	Consume(context.Context, Data) error
}
