package pipeline

import (
	"context"

	"github.com/caffix/queue"
)

// StageParams provides the information needed for executing a pipeline
// Stage. The Pipeline passes a StageParams instance to the Run method
// of each stage.
type StageParams interface {
	// Position returns the position of this stage in the pipeline.
	Position() int

	// Input return the input channel for this stage.
	Input() <-chan Data

	// Output returns the output channel for this stage.
	Output() chan<- Data

	// ReportError returns the queue that reports errors encountered by the stage.
	Error() *queue.Queue
}

// Stage is designed to be executed in sequential order to
// form a multi-stage data pipeline.
type Stage interface {
	// Run executes the processing logic for this stage by reading
	// data from the input channel, processing the data and sending
	// the results to the output channel. Run blocks until the stage
	// input channel is closed, the context expires, or an error occurs.
	Run(context.Context, StageParams)
}
