package pipeline

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestParallel(t *testing.T) {
	num := 10
	var count int
	var m sync.Mutex
	tasks := make([]Task, num)
	for i := 0; i < num; i++ {
		tasks[i] = TaskFunc(func(_ context.Context, d Data, _ TaskParams) (Data, error) {
			time.Sleep(time.Second)
			m.Lock()
			count++
			m.Unlock()
			return nil, nil
		})
	}

	// Check that all previous tasks have completed
	checker := TaskFunc(func(_ context.Context, d Data, _ TaskParams) (Data, error) {
		var c int

		m.Lock()
		c = count
		m.Unlock()
		if c != num {
			return d, fmt.Errorf("Not all previous tasks have finished.\nWanted: %d\nGot: %d\n", num, c)
		}
		return d, nil
	})

	src := &sourceStub{data: stringDataValues(1)}
	sink := new(sinkStub)

	p := NewPipeline(Parallel("", tasks...), FIFO("", checker))
	if err := p.Execute(context.TODO(), src, sink); err != nil {
		t.Errorf("Error executing the Pipeline: %v", err)
	}
}

func BenchmarkParallel(b *testing.B) {
	for i := 0; i < b.N; i++ {
		p := NewPipeline(Parallel("", makePassthroughTask()))
		src := &sourceStub{data: []Data{&stringData{val: "benchmark"}}}
		_ = p.Execute(context.TODO(), src, new(sinkStub))
	}
}

func BenchmarkParallelDataElements(b *testing.B) {
	sink := new(sinkStub)
	src := &sourceStub{data: stringDataValues(b.N)}
	p := NewPipeline(Parallel("", makePassthroughTask()))

	b.StartTimer()
	_ = p.Execute(context.TODO(), src, sink)
	b.StopTimer()
}
