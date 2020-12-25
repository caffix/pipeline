package pipeline

import (
	"context"
	"fmt"
	"sync"

	"github.com/caffix/queue"
	"github.com/caffix/stringset"
	"github.com/hashicorp/go-multierror"
)

type params struct {
	stage     int
	inCh      <-chan Data
	outCh     chan<- Data
	dataQueue *queue.Queue
	errQueue  *queue.Queue
	newdata   chan<- Data
	processed chan<- Data
	registry  StageRegistry
}

func (p *params) Position() int              { return p.stage }
func (p *params) Input() <-chan Data         { return p.inCh }
func (p *params) Output() chan<- Data        { return p.outCh }
func (p *params) DataQueue() *queue.Queue    { return p.dataQueue }
func (p *params) Error() *queue.Queue        { return p.errQueue }
func (p *params) NewData() chan<- Data       { return p.newdata }
func (p *params) ProcessedData() chan<- Data { return p.processed }
func (p *params) Registry() StageRegistry    { return p.registry }

// Pipeline is an abstract and extendable asynchronous data
// pipeline with concurrent tasks at each stage. Each pipeline
// is constructed from an InputSource, an OutputSink, and zero
// or more Stage instances for processing.
type Pipeline struct {
	stages []Stage
}

// NewPipeline returns a new data pipeline instance where input
// traverse each of the provided Stage instances.
func NewPipeline(stages ...Stage) *Pipeline {
	var count int
	set := stringset.New()

	for _, stage := range stages {
		if id := stage.ID(); id != "" {
			set.Insert(id)
			count++
		}
	}
	// Check that all stage identifiers are unique
	if count != set.Len() {
		return nil
	}
	return &Pipeline{stages: stages}
}

// Execute performs ExecuteBuffered with a bufsize parameter equal to 1.
func (p *Pipeline) Execute(ctx context.Context, src InputSource, sink OutputSink) error {
	return p.ExecuteBuffered(ctx, src, sink, 1)
}

// ExecuteBuffered reads data from the InputSource, sends them through
// each of the Stage instances, and finishes with the OutputSink.
// All errors are returned that occurred during the execution.
// ExecuteBuffered will block until all data from the InputSource has
// been processed, or an error occurs, or the context expires.
func (p *Pipeline) ExecuteBuffered(ctx context.Context, src InputSource, sink OutputSink, bufsize int) error {
	pCtx, cancel := context.WithCancel(ctx)

	var stageQueue []*queue.Queue
	// Create the stage registry
	registry := make(StageRegistry, len(p.stages)+1)
	// Create channels for wiring together the InputSource,
	// the pipeline Stage instances, and the OutputSink
	stageCh := make([]chan Data, len(p.stages)+1)
	for i := 0; i < len(stageCh); i++ {
		stageCh[i] = make(chan Data, bufsize)
		stageQueue = append(stageQueue, queue.NewQueue())

		id := "sink"
		if i != len(stageCh)-1 {
			id = p.stages[i].ID()
		}
		if id != "" {
			registry[id] = stageQueue[i]
		}
	}
	errQueue := queue.NewQueue()

	var wg sync.WaitGroup
	newdata := make(chan Data)
	processed := make(chan Data)
	// Start a goroutine for each Stage
	for i := 0; i < len(p.stages); i++ {
		wg.Add(1)
		go func(idx int) {
			p.stages[idx].Run(pCtx, &params{
				stage:     idx + 1,
				inCh:      stageCh[idx],
				outCh:     stageCh[idx+1],
				dataQueue: stageQueue[idx],
				errQueue:  errQueue,
				newdata:   newdata,
				processed: processed,
				registry:  registry,
			})
			// Tell the next Stage that no more Data is available
			close(stageCh[idx+1])
			wg.Done()
		}(i)
	}
	// Start goroutines for the InputSource and OutputSink
	wg.Add(2)
	finished := make(chan struct{}, 2)
	go func() {
		inputSourceRunner(pCtx, src, stageCh[0], newdata, errQueue)
		// Tell the next Stage that no more Data is available
		finished <- struct{}{}
		wg.Done()
	}()
	go func() {
		outputSinkRunner(pCtx, sink, stageCh[len(stageCh)-1], processed, errQueue)
		wg.Done()
	}()
	// Monitor for completion of the pipeline execution
	go func(f chan struct{}) {
		var count int
		var done bool
	loop:
		for {
			select {
			case <-pCtx.Done():
				break loop
			case <-f:
				done = true
			case <-newdata:
				count++
			case <-processed:
				count--
			}
			if done && count == 0 {
				break loop
			}
		}

		close(stageCh[0])
		wg.Wait()
		cancel()
	}(finished)

	var err error
	// Collect any emitted errors and wrap them in a multi-error
	select {
	case <-pCtx.Done():
	case <-errQueue.Signal:
		errQueue.Process(func(e interface{}) {
			if qErr, ok := e.(error); ok {
				err = multierror.Append(err, qErr)
			}
		})
		cancel()
	}
	return err
}

// inputSourceRunner drives the InputSource to continue providing
// data to the first stage of the pipeline.
func inputSourceRunner(ctx context.Context, src InputSource, outCh chan<- Data, newdata chan<- Data, errQueue *queue.Queue) {
	for src.Next(ctx) {
		data := src.Data()
		newdata <- data

		select {
		case <-ctx.Done():
			return
		case outCh <- data:
		}
	}
	// Check for errors
	if err := src.Error(); err != nil {
		errQueue.Append(fmt.Errorf("pipeline input source: %v", err))
	}
}

func outputSinkRunner(ctx context.Context, sink OutputSink, inCh <-chan Data, processed chan<- Data, errQueue *queue.Queue) {
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-inCh:
			if !ok {
				return
			}
			if err := sink.Consume(ctx, data); err != nil {
				errQueue.Append(fmt.Errorf("pipeline output sink: %v", err))
				return
			}

			processed <- data
			data.MarkAsProcessed()
		}
	}
}
