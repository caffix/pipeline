package pipeline

import (
	"context"
	"fmt"
	"sync"
)

type parallel struct {
	tasks []Task
}

// Parallel returns a Stage that passes a copy of each incoming data
// to all specified tasks and emits their outputs to the next stage.
func Parallel(tasks ...Task) Stage {
	if len(tasks) == 0 {
		return nil
	}

	return &parallel{tasks: tasks}
}

// Run implements Stage.
func (p *parallel) Run(ctx context.Context, sp StageParams) {
	for {
		var wg sync.WaitGroup

		select {
		case <-ctx.Done():
			return
		case data, ok := <-sp.Input():
			if !ok {
				return
			}

			for i := 0; i < len(p.tasks); i++ {
				wg.Add(1)
				go func(idx int) {
					defer wg.Done()

					d, err := p.tasks[idx].Process(ctx, data.Clone())
					if err != nil {
						sp.Error().Append(fmt.Errorf("pipeline stage %d: %v", sp.Position(), err))
					}
					if d != nil {
						sp.Output() <- d
					}
				}(i)
			}
			wg.Wait()

			select {
			case <-ctx.Done():
				return
			case sp.Output() <- data:
			}
		}
	}
}
