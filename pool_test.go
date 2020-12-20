package pipeline

import (
	"context"
	"testing"
	"time"
)

func TestFixedWorkerPool(t *testing.T) {
	num := 10
	syncCh := make(chan struct{})
	rendezvousCh := make(chan struct{})

	task := TaskFunc(func(_ context.Context, _ Data) (Data, error) {
		// Signal that we have reached the sync point and wait for the
		// green light to proceed by the test code
		syncCh <- struct{}{}
		<-rendezvousCh
		return nil, nil
	})

	src := &sourceStub{data: stringDataValues(num)}

	p := NewPipeline(FixedPool(task, num))
	doneCh := make(chan struct{})
	go func() {
		if err := p.Execute(context.TODO(), src, nil); err != nil {
			t.Errorf("Error executing the Pipeline: %v", err)
		}
		close(doneCh)
	}()

	// Wait for all workers to reach sync point. This means that each input
	// from the source is currently handled by a worker in parallel
	for i := 0; i < num; i++ {
		select {
		case <-syncCh:
		case <-time.After(10 * time.Second):
			t.Errorf("timed out waiting for worker %d to reach sync point", i)
		}
	}

	// Allow workers to proceed and wait for the pipeline to complete
	close(rendezvousCh)
	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		t.Errorf("timed out waiting for pipeline to complete")
	}
}

func TestDynamicWorkerPool(t *testing.T) {
	num := 5
	syncCh := make(chan struct{}, num)
	rendezvousCh := make(chan struct{})

	task := TaskFunc(func(_ context.Context, _ Data) (Data, error) {
		// Signal that we have reached the sync point and wait for the
		// green light to proceed by the test code
		syncCh <- struct{}{}
		<-rendezvousCh
		return nil, nil
	})

	src := &sourceStub{data: stringDataValues(num * 2)}

	p := NewPipeline(DynamicPool(task, num))
	doneCh := make(chan struct{})
	go func() {
		if err := p.Execute(context.TODO(), src, nil); err != nil {
			t.Errorf("Error executing the Pipeline: %v", err)
		}
		close(doneCh)
	}()

	// Wait for all workers to reach sync point. This means that the pool
	// has scaled up to the max number of workers
	for i := 0; i < num; i++ {
		select {
		case <-syncCh:
		case <-time.After(10 * time.Second):
			t.Errorf("timed out waiting for worker %d to reach sync point", i)
		}
	}

	// Allow workers to proceed and process the next batch of records
	close(rendezvousCh)
	select {
	case <-doneCh:
	case <-time.After(10 * time.Second):
		t.Errorf("timed out waiting for pipeline to complete")
	}

	assertAllProcessed(t, src.data)
}
