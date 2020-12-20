package pipeline

import (
	"context"
	"fmt"
)

type fifo struct {
	task Task
}

// FIFO returns a Stage that processes incoming data in a first-in first-out
// fashion. Each input is passed to the specified Task and its output
// is emitted to the next Stage.
func FIFO(task Task) Stage {
	return fifo{task: task}
}

// Run implements Stage.
func (r fifo) Run(ctx context.Context, sp StageParams) {
	for {
		select {
		case <-ctx.Done():
			return
		case dataIn, ok := <-sp.Input():
			if !ok {
				return
			}

			dataOut, err := r.task.Process(ctx, dataIn)
			if err != nil {
				sp.Error().Append(fmt.Errorf("pipeline stage %d: %v", sp.Position(), err))
				return
			}
			// If the task did not output data for the
			// next stage there is nothing we need to do
			if dataOut == nil {
				dataIn.MarkAsProcessed()
				continue
			}
			// Output processed data
			select {
			case <-ctx.Done():
				return
			case sp.Output() <- dataOut:
			}
		}
	}
}
