package pipeline

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"regexp"
	"testing"
)

func TestDataFlow(t *testing.T) {
	stages := make([]Stage, 10)
	for i := 0; i < len(stages); i++ {
		stages[i] = testStage{t: t}
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

func TestTaskErrorHandling(t *testing.T) {
	err := errors.New("task error")
	stages := make([]Stage, 10)
	for i := 0; i < len(stages); i++ {
		var sErr error
		if i == 5 {
			sErr = err
		}

		stages[i] = testStage{
			t:   t,
			err: sErr,
		}
	}

	src := &sourceStub{data: stringDataValues(3)}
	sink := new(sinkStub)

	p := NewPipeline(stages...)
	re := regexp.MustCompile("(?s).*task error.*")
	if err := p.Execute(context.TODO(), src, sink); err == nil || !re.MatchString(err.Error()) {
		t.Errorf("Error did not match the expectation: %v", err)
	}
}

func TestDataItemCount(t *testing.T) {
	src := &sourceStub{data: stringDataValues(1000)}
	sink := new(sinkStub)

	p := NewPipeline(&randomStage{
		id: "first",
		t:  t,
	})
	if err := p.Execute(context.TODO(), src, sink); err != nil {
		t.Errorf("Error executing the Pipeline: %v", err)
	}
	if num := p.DataItemCount(); num != 0 {
		t.Errorf("Pipeline execution finished with %d pending data items", num)
	}
}

type randomStage struct {
	id  string
	t   *testing.T
	err error
}

func (s randomStage) ID() string {
	return s.id
}

func (s randomStage) Run(ctx context.Context, sp StageParams) {
	// add data items to the stage queue
	for i := 0; i < 1000; i++ {
		SendData(ctx, "first", &stringData{val: fmt.Sprint(i)}, &taskParams{
			pipeline: sp.Pipeline(),
			registry: sp.Registry(),
		})
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-sp.DataQueue().Signal():
			if d, ok := sp.DataQueue().Next(); ok {
				if data, ok := d.(Data); ok {
					s.processData(ctx, data, sp)
				}
			}
		case d, ok := <-sp.Input():
			if !ok {
				return
			}
			s.processData(ctx, d, sp)
		}
	}
}

func (s randomStage) processData(ctx context.Context, d Data, sp StageParams) {
	if s.err != nil {
		s.t.Logf("[stage %d] emit error: %v", sp.Position(), s.err)
		sp.Error().Append(s.err)
		return
	}

	if num := rand.Intn(2); num == 0 {
		return
	}

	select {
	case <-ctx.Done():
	case sp.Output() <- d:
	}
}

type testStage struct {
	id       string
	t        *testing.T
	dropData bool
	err      error
}

func (s testStage) ID() string {
	return s.id
}

func (s testStage) Run(ctx context.Context, sp StageParams) {
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-sp.Input():
			if !ok {
				return
			}
			if s.err != nil {
				sp.Error().Append(s.err)
				return
			}

			if s.dropData {
				continue
			}

			select {
			case <-ctx.Done():
				return
			case sp.Output() <- d:
			}
		}
	}
}

type stringData struct {
	val string
}

func (s *stringData) Clone() Data    { return &stringData{val: s.val} }
func (s *stringData) String() string { return s.val }

func stringDataValues(num int) []Data {
	out := make([]Data, num)

	for i := 0; i < len(out); i++ {
		out[i] = &stringData{val: fmt.Sprint(i)}
	}
	return out
}
