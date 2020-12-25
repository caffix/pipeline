package pipeline

import (
	"context"

	"github.com/caffix/queue"
)

// StageRegistry is a map of stage identifiers to input channels.
type StageRegistry map[string]*queue.Queue

// StageParams provides the information needed for executing a pipeline
// Stage. The Pipeline passes a StageParams instance to the Run method
// of each stage.
type StageParams interface {
	// Position returns the position of this stage in the pipeline.
	Position() int

	// Input returns the input channel for this stage.
	Input() <-chan Data

	// Output returns the output channel for this stage.
	Output() chan<- Data

	// DataQueue returns the alternative data queue for this stage.
	DataQueue() *queue.Queue

	// NewData returns the channel for signalling new Data to the pipeline.
	NewData() chan<- Data

	// ProcessedData returns the channel for signalling processed Data to
	// the pipeline.
	ProcessedData() chan<- Data

	// Error returns the queue that reports errors encountered by the stage.
	Error() *queue.Queue

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
