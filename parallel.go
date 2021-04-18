package pipeline

import (
	"context"
	"fmt"
)

type parallel struct {
	id    string
	tasks []Task
}

// Parallel returns a Stage that passes a copy of each incoming Data
// to all specified tasks, waits for all the tasks to finish before
// sending data to the next stage, and only passes the original Data
// through to the following stage.
func Parallel(id string, tasks ...Task) Stage {
	if len(tasks) == 0 {
		return nil
	}

	return &parallel{
		id:    id,
		tasks: tasks,
	}
}

// ID implements Stage.
func (p *parallel) ID() string {
	return p.id
}

// Run implements Stage.
func (p *parallel) Run(ctx context.Context, sp StageParams) {
	for {
		if !processStageData(ctx, sp, p.executeTask) {
			break
		}
	}
}

func (p *parallel) executeTask(ctx context.Context, data Data, sp StageParams) {
	select {
	case <-ctx.Done():
		data.MarkAsProcessed()
		return
	default:
	}

	done := make(chan Data, len(p.tasks))
	for i := 0; i < len(p.tasks); i++ {
		c := data.Clone()

		select {
		case <-ctx.Done():
			return
		default:
		}

		go func(idx int, clone Data) {
			d, err := p.tasks[idx].Process(ctx, clone, &taskParams{registry: sp.Registry()})
			if err != nil {
				sp.Error().Append(fmt.Errorf("pipeline stage %d: %v", sp.Position(), err))
			}

			clone.MarkAsProcessed()
			done <- d
		}(i, c)
	}

	var failed bool
	for i := 0; i < len(p.tasks); i++ {
		if d := <-done; d == nil {
			failed = true
		}
	}
	if failed {
		data.MarkAsProcessed()
		return
	}

	select {
	case <-ctx.Done():
	case sp.Output() <- data:
	}
}
