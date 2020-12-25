package pipeline

import (
	"context"
	"sync"

	"github.com/caffix/queue"
)

type broadcast struct {
	id    string
	fifos []Stage
	inChs []chan Data
}

// Broadcast returns a Stage that passes a copy of each incoming data
// to all specified tasks and emits their outputs to the next stage.
func Broadcast(id string, tasks ...Task) Stage {
	if len(tasks) == 0 {
		return nil
	}

	fifos := make([]Stage, len(tasks))
	for i, t := range tasks {
		fifos[i] = FIFO("", t)
	}

	return &broadcast{
		id:    id,
		fifos: fifos,
		inChs: make([]chan Data, len(fifos)),
	}
}

// ID implements Stage.
func (b *broadcast) ID() string {
	return b.id
}

// Run implements Stage.
func (b *broadcast) Run(ctx context.Context, sp StageParams) {
	var wg sync.WaitGroup

	// Start each FIFO in a goroutine. Each FIFO gets its own dedicated
	// input channel and the shared output channel passed to Run.
	for i := 0; i < len(b.fifos); i++ {
		wg.Add(1)
		b.inChs[i] = make(chan Data)
		go func(idx int) {
			b.fifos[idx].Run(ctx, &params{
				stage:     sp.Position(),
				inCh:      b.inChs[idx],
				outCh:     sp.Output(),
				dataQueue: queue.NewQueue(),
				errQueue:  sp.Error(),
				newdata:   sp.NewData(),
				processed: sp.ProcessedData(),
				registry:  sp.Registry(),
			})
			wg.Done()
		}(i)
	}
loop:
	for {
		// Read incoming data and pass them to each FIFO
		select {
		case <-ctx.Done():
			break loop
		case dataIn, ok := <-sp.Input():
			if !ok {
				break loop
			}
			b.executeTask(ctx, dataIn, sp)
		case <-sp.DataQueue().Signal:
			if d, ok := sp.DataQueue().Next(); ok {
				if data, ok := d.(Data); ok {
					b.executeTask(ctx, data, sp)
				}
			}
		}
	}

	// Close input channels and wait for FIFOs to exit
	for _, ch := range b.inChs {
		close(ch)
	}
	wg.Wait()
}

func (b *broadcast) executeTask(ctx context.Context, data Data, sp StageParams) {
	for i := len(b.fifos) - 1; i >= 0; i-- {
		// As each FIFO might modify the data, to
		// avoid data races we need to make a copy
		// of the data for all FIFOs except the first.
		var fifoData = data
		if i != 0 {
			fifoData = data.Clone()

			select {
			case <-ctx.Done():
				return
			case sp.NewData() <- fifoData:
			}
		}

		select {
		case <-ctx.Done():
			return
		case b.inChs[i] <- fifoData:
			// data sent to i_th FIFO
		}
	}
}
