package pipeline

import (
	"context"

	"github.com/caffix/queue"
)

// StageRegistry is a map of stage identifiers to input channels.
type StageRegistry map[string]queue.Queue

// StageParams provides the information needed for executing a pipeline
// Stage. The Pipeline passes a StageParams instance to the Run method
// of each stage.
type StageParams interface {
	// Pipeline returns the pipeline executing this stage.
	Pipeline() *Pipeline

	// Position returns the position of this stage in the pipeline.
	Position() int

	// Input returns the input channel for this stage.
	Input() <-chan Data

	// Output returns the output channel for this stage.
	Output() chan<- Data

	// DataQueue returns the alternative data queue for this stage.
	DataQueue() queue.Queue

	// Error returns the queue that reports errors encountered by the stage.
	Error() queue.Queue

	// Registry returns a map of stage names to stage input channels.
	Registry() StageRegistry
}

// Stage is designed to be executed in sequential order to
// form a multi-stage data pipeline.
type Stage interface {
	// ID returns the optional identifier assigned to this stage.
	ID() string

	// Run executes the processing logic for this stage by reading
	// data from the input channel, processing the data and sending
	// the results to the output channel. Run blocks until the stage
	// input channel is closed, the context expires, or an error occurs.
	Run(context.Context, StageParams)
}

type execTask func(context.Context, Data, StageParams) (Data, error)

func processStageData(ctx context.Context, sp StageParams, task execTask) bool {
	cont := true
	// Processes data from the input channel and data queue
	select {
	case dataIn, ok := <-sp.Input():
		if ok {
			_, _ = task(ctx, dataIn, sp)
		} else if sp.DataQueue().Len() == 0 {
			cont = false
		}
	case <-sp.DataQueue().Signal():
		if d, ok := sp.DataQueue().Next(); ok {
			if data, ok := d.(Data); ok {
				_, _ = task(ctx, data, sp)
			}
		}
	}
	return cont
}
