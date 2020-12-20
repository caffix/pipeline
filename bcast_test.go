package pipeline

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
)

func TestBroadcast(t *testing.T) {
	num := 3
	tasks := make([]Task, num)
	for i := 0; i < num; i++ {
		tasks[i] = makeMutatingTask(i)
	}

	src := &sourceStub{data: stringDataValues(1)}
	sink := new(sinkStub)

	p := NewPipeline(Broadcast(tasks...))
	if err := p.Execute(context.TODO(), src, sink); err != nil {
		t.Errorf("Error executing the Pipeline: %v", err)
	}

	data := []Data{
		&stringData{val: "0_0", processed: true},
		&stringData{val: "0_1", processed: true},
		&stringData{val: "0_2", processed: true},
	}
	assertAllProcessed(t, src.data)

	// Tasks run as goroutines so outputs will be shuffled. We need
	// to sort them first so we can check for equality.
	sort.Slice(data, func(i, j int) bool {
		return data[i].(*stringData).val < data[j].(*stringData).val
	})
	sort.Slice(sink.data, func(i, j int) bool {
		return sink.data[i].(*stringData).val < sink.data[j].(*stringData).val
	})
	if !reflect.DeepEqual(sink.data, data) {
		t.Errorf("Data does not match.\nWanted:%v\nGot:%v\n", data, sink.data)
	}
}

func makeMutatingTask(index int) Task {
	return TaskFunc(func(_ context.Context, d Data) (Data, error) {
		// Mutate data to check that each task got a copy
		sd := d.(*stringData)
		sd.val = fmt.Sprintf("%s_%d", sd.val, index)
		return d, nil
	})
}
