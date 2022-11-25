package pipeline

import (
	"context"
)

// TaskParams provides access to pipeline mechanisms needed by a Task.
// The Stage passes a TaskParams instance to the Process method of
// each task.
type TaskParams interface {
	// Pipeline returns the pipeline executing this task.
	Pipeline() *Pipeline

	// Registry returns a map of stage names to stage input channels.
	Registry() StageRegistry
}

// Task is implemented by types that can process Data as part of a pipeline stage.
type Task interface {
	// Process operates on the input data and returns back a new data to be
	// forwarded to the next pipeline stage. Task instances may also opt to
	// prevent the data from reaching the rest of the pipeline by returning
	// a nil data value instead.
	Process(context.Context, Data, TaskParams) (Data, error)
}

// TaskFunc is an adapter to allow the use of plain functions as Task instances.
type TaskFunc func(context.Context, Data, TaskParams) (Data, error)

// Process calls f(ctx, data)
func (f TaskFunc) Process(ctx context.Context, data Data, params TaskParams) (Data, error) {
	return f(ctx, data, params)
}

// SendData marks the provided data as new to the pipeline and sends it to the
// provided named stage.
func SendData(ctx context.Context, stage string, data Data, tp TaskParams) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	if q, found := tp.Registry()[stage]; found {
		_ = tp.Pipeline().IncDataItemCount()
		q.Append(data)
	}
}

type taskParams struct {
	pipeline *Pipeline
	registry StageRegistry
}

func (tp *taskParams) Pipeline() *Pipeline     { return tp.pipeline }
func (tp *taskParams) Registry() StageRegistry { return tp.registry }
