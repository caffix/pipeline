package pipeline

import (
	"context"
	"fmt"
	"sync"
)

type fixedPool struct {
	id    string
	fifos []Stage
}

// FixedPool returns a Stage that spins up a pool containing numWorkers
// to process incoming data in parallel and emit their outputs to the next stage.
func FixedPool(id string, task Task, num int) Stage {
	if num <= 0 {
		return nil
	}

	fifos := make([]Stage, num)
	for i := 0; i < num; i++ {
		fifos[i] = FIFO("", task)
	}

	return &fixedPool{
		id:    id,
		fifos: fifos,
	}
}

// ID implements Stage.
func (p *fixedPool) ID() string {
	return p.id
}

// Run implements Stage.
func (p *fixedPool) Run(ctx context.Context, params StageParams) {
	var wg sync.WaitGroup

	// Spin up each task in the pool and wait for them to exit
	for i := 0; i < len(p.fifos); i++ {
		wg.Add(1)
		go func(idx int) {
			p.fifos[idx].Run(ctx, params)
			wg.Done()
		}(i)
	}

	wg.Wait()
}

type dynamicPool struct {
	id        string
	task      Task
	tokenPool chan struct{}
}

// DynamicPool returns a Stage that maintains a dynamic pool that can scale
// up to max parallel tasks for processing incoming inputs in parallel and
// emitting their outputs to the next stage.
func DynamicPool(id string, task Task, max int) Stage {
	if max <= 0 {
		return nil
	}

	tokenPool := make(chan struct{}, max)
	for i := 0; i < max; i++ {
		tokenPool <- struct{}{}
	}

	return &dynamicPool{
		id:        id,
		task:      task,
		tokenPool: tokenPool,
	}
}

// ID implements Stage.
func (p *dynamicPool) ID() string {
	return p.id
}

// Run implements Stage.
func (p *dynamicPool) Run(ctx context.Context, sp StageParams) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case dataIn, ok := <-sp.Input():
			if !ok {
				break loop
			}
			p.executeTask(ctx, dataIn, sp)
		case <-sp.DataQueue().Signal():
			if d, ok := sp.DataQueue().Next(); ok {
				if data, ok := d.(Data); ok {
					p.executeTask(ctx, data, sp)
				}
			}
		}
	}

	// Wait for all workers to exit by trying to empty the token pool
	for i := 0; i < cap(p.tokenPool); i++ {
		<-p.tokenPool
	}
}

func (p *dynamicPool) executeTask(ctx context.Context, data Data, sp StageParams) {
	var token struct{}

	select {
	case <-ctx.Done():
		return
	case token = <-p.tokenPool:
	}

	go func(dataIn Data, token struct{}) {
		defer func() { p.tokenPool <- token }()

		tp := &taskParams{
			newdata:   sp.NewData(),
			processed: sp.ProcessedData(),
			registry:  sp.Registry(),
		}

		dataOut, err := p.task.Process(ctx, dataIn, tp)
		if err != nil {
			sp.Error().Append(fmt.Errorf("pipeline stage %d: %v", sp.Position(), err))
			return
		}

		// If the task did not output data for the
		// next stage there is nothing we need to do.
		if dataOut == nil {
			sp.ProcessedData() <- dataIn
			dataIn.MarkAsProcessed()
			return
		}

		// Output processed data
		select {
		case <-ctx.Done():
		case sp.Output() <- dataOut:
		}
	}(data, token)
}
