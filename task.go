package pipeline

import "context"

// TaskParams provides access to pipeline mechanisms needed by a Task.
// The Stage passes a TaskParams instance to the Process method of
// each task.
type TaskParams interface {
	// NewData returns the channel for signalling new Data to the pipeline.
	NewData() chan<- Data

	// ProcessedData returns the channel for signalling processed Data to
	// the pipeline.
	ProcessedData() chan<- Data

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

type taskParams struct {
	newdata   chan<- Data
	processed chan<- Data
	registry  StageRegistry
}

func (tp *taskParams) NewData() chan<- Data       { return tp.newdata }
func (tp *taskParams) ProcessedData() chan<- Data { return tp.processed }
func (tp *taskParams) Registry() StageRegistry    { return tp.registry }
