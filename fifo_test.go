package pipeline

import (
	"context"
	"reflect"
	"testing"
)

func TestFIFO(t *testing.T) {
	stages := make([]Stage, 10)
	for i := 0; i < len(stages); i++ {
		stages[i] = FIFO(makePassthroughTask())
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

	assertAllProcessed(t, src.data)
}

func makePassthroughTask() Task {
	return TaskFunc(func(_ context.Context, data Data) (Data, error) {
		return data, nil
	})
}
