package pipeline

import (
	"context"
	"reflect"
	"testing"
)

func TestFIFO(t *testing.T) {
	stages := make([]Stage, 10)
	for i := 0; i < len(stages); i++ {
		stages[i] = FIFO("", makePassthroughTask())
	}

	src := &sourceStub{data: stringDataValues(3)}
	sink := new(sinkStub)

	p := NewPipeline(stages...)
	if err := p.Execute(context.TODO(), src, sink); err != nil {
		t.Errorf("Error executing the Pipeline: %v", err)
	}
	if !reflect.DeepEqual(sink.data, src.data) {
		t.Errorf("Data does not match.\nWanted:%v\nGot:%v\n", src.data, sink.data)
	}
}

func BenchmarkFIFO(b *testing.B) {
	for i := 0; i < b.N; i++ {
		p := NewPipeline(FIFO("", makePassthroughTask()))
		src := &sourceStub{data: []Data{&stringData{val: "benchmark"}}}
		_ = p.Execute(context.TODO(), src, new(sinkStub))
	}
}

func BenchmarkFIFODataElements(b *testing.B) {
	sink := new(sinkStub)
	src := &sourceStub{data: stringDataValues(b.N)}
	p := NewPipeline(FIFO("", makePassthroughTask()))

	b.StartTimer()
	_ = p.Execute(context.TODO(), src, sink)
	b.StopTimer()
}

func makePassthroughTask() Task {
	return TaskFunc(func(_ context.Context, data Data, _ TaskParams) (Data, error) {
		return data, nil
	})
}
