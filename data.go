package pipeline

// Data is implemented by values that can be sent through a pipeline.
type Data interface {
	// Clone returns a new Data that is a deep-copy of the original.
	Clone() Data

	// MarkAsProcessed is invoked by the pipeline when the Data either
	// reaches the pipeline sink or gets discarded by one of the
	// pipeline stages.
	MarkAsProcessed()
}
