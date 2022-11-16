package pipeline

import (
	"context"
	"testing"
)

func TestStageRegistry(t *testing.T) {
	src := &sourceStub{data: stringDataValues(1)}
	sink := &sinkStub{}

	max := 3
	var count int
	task := TaskFunc(func(ctx context.Context, data Data, tp TaskParams) (Data, error) {
		count++
		if count > max {
			return data, nil
		}

		// Send data to an unnamed stage
		if count == 1 {
			SendData(ctx, "fake", data.Clone(), tp)
		}

		SendData(ctx, "counter", data.Clone(), tp)
		return data, nil
	})

	p := NewPipeline(FIFO("counter", task))
	if err := p.Execute(context.TODO(), src, sink); err != nil {
		t.Errorf("Error executing the Pipeline: %v", err)
	}
	if c := count - 1; c != max {
		t.Errorf("Retry count does not match expected value.\nWanted:%v\nGot:%v\n", max, c)
	}
}
