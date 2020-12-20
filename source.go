package pipeline

import "context"

// InputSource is implemented by types that generate Data instances which can be
// used as inputs to a Pipeline instance.
type InputSource interface {
	// Next fetches the next data element from the source. If no more items are
	// available or an error occurs, calls to Next return false.
	Next(context.Context) bool

	// Data returns the next data to be processed.
	Data() Data

	// Error return the last error observed by the source.
	Error() error
}
