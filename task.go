package pipeline

import "context"

// Task is implemented by types that can process Data as part of a pipeline stage.
type Task interface {
	// Process operates on the input data and returns back a new data to be
	// forwarded to the next pipeline stage. Task instances may also opt to
	// prevent the data from reaching the rest of the pipeline by returning
	// a nil data value instead.
	Process(context.Context, Data) (Data, error)
}

// TaskFunc is an adapter to allow the use of plain functions as Task instances.
type TaskFunc func(context.Context, Data) (Data, error)

// Process calls f(ctx, data)
func (f TaskFunc) Process(ctx context.Context, data Data) (Data, error) {
	return f(ctx, data)
}
