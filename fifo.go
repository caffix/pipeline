package pipeline

import (
	"context"
	"fmt"
)

type fifo struct {
	id   string
	task Task
}

// FIFO returns a Stage that processes incoming data in a first-in first-out
// fashion. Each input is passed to the specified Task and its output
// is emitted to the next Stage.
func FIFO(id string, task Task) Stage {
	return &fifo{
		id:   id,
		task: task,
	}
}

// ID implements Stage.
func (r *fifo) ID() string {
	return r.id
}

// Run implements Stage.
func (r *fifo) Run(ctx context.Context, sp StageParams) {
	for {
		select {
		case <-ctx.Done():
			return
		case dataIn, ok := <-sp.Input():
			if !ok {
				return
			}
			r.executeTask(ctx, dataIn, sp)
		case <-sp.DataQueue().Signal():
			if d, ok := sp.DataQueue().Next(); ok {
				if data, ok := d.(Data); ok {
					r.executeTask(ctx, data, sp)
				}
			}
		}
	}
}

func (r *fifo) executeTask(ctx context.Context, data Data, sp StageParams) {
	tp := &taskParams{
		newdata:   sp.NewData(),
		processed: sp.ProcessedData(),
		registry:  sp.Registry(),
	}

	dataOut, err := r.task.Process(ctx, data, tp)
	if err != nil {
		sp.Error().Append(fmt.Errorf("pipeline stage %d: %v", sp.Position(), err))
		return
	}
	// If the task did not output data for the
	// next stage there is nothing we need to do
	if dataOut == nil {
		sp.ProcessedData() <- data
		data.MarkAsProcessed()
		return
	}
	// Output processed data
	select {
	case <-ctx.Done():
	case sp.Output() <- dataOut:
	}
}
