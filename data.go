package pipeline

// Data is implemented by values that can be sent through a pipeline.
type Data interface {
	// Clone returns a new Data that is a deep-copy of the original.
	Clone() Data
}
