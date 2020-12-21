package pipeline

import (
	"context"
	"fmt"
)

type parallel struct {
	tasks []Task
}

// Parallel returns a Stage that passes a copy of each incoming Data
// to all specified tasks, waits for all the tasks to finish before
// sending data to the next stage, and only passes the original Data
// through to the following stage.
func Parallel(tasks ...Task) Stage {
	if len(tasks) == 0 {
		return nil
	}

	return &parallel{tasks: tasks}
}

// Run implements Stage.
func (p *parallel) Run(ctx context.Context, sp StageParams) {
loop:
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-sp.Input():
			if !ok {
				return
			}

			done := make(chan Data, len(p.tasks))
			for i := 0; i < len(p.tasks); i++ {
				go func(idx int, clone Data) {
					d, err := p.tasks[idx].Process(ctx, clone)
					if err != nil {
						sp.Error().Append(fmt.Errorf("pipeline stage %d: %v", sp.Position(), err))
					}
					clone.MarkAsProcessed()
					done <- d
				}(i, data.Clone())
			}

			var failed bool
			for i := 0; i < len(p.tasks); i++ {
				if d := <-done; d == nil {
					failed = true
				}
			}
			if failed {
				data.MarkAsProcessed()
				continue loop
			}

			select {
			case <-ctx.Done():
				return
			case sp.Output() <- data:
			}
		}
	}
}
