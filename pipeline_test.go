package pipeline

import (
	"context"
	"errors"
	"fmt"
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

	assertAllProcessed(t, src.data)
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

func TestSourceErrorHandling(t *testing.T) {
	src := &sourceStub{
		data: stringDataValues(3),
		err:  errors.New("source error"),
	}
	sink := new(sinkStub)

	p := NewPipeline(testStage{t: t})
	re := regexp.MustCompile("(?s).*pipeline input source: source error.*")
	if err := p.Execute(context.TODO(), src, sink); err == nil || !re.MatchString(err.Error()) {
		t.Errorf("Error did not match the expectation: %v", err)
	}
}

func TestSinkErrorHandling(t *testing.T) {
	src := &sourceStub{data: stringDataValues(3)}
	sink := &sinkStub{err: errors.New("sink error")}

	p := NewPipeline(testStage{t: t})
	re := regexp.MustCompile("(?s).*pipeline output sink: sink error.*")
	if err := p.Execute(context.TODO(), src, sink); err == nil || !re.MatchString(err.Error()) {
		t.Errorf("Error did not match the expectation: %v", err)
	}
}

func TestDataDiscarding(t *testing.T) {
	src := &sourceStub{data: stringDataValues(3)}
	sink := &sinkStub{}

	p := NewPipeline(&testStage{
		t:        t,
		dropData: true,
	})
	if err := p.Execute(context.TODO(), src, sink); err != nil {
		t.Errorf("Error executing the Pipeline: %v", err)
	}
	if len(sink.data) != 0 {
		t.Errorf("Expected all data to be discarded by stage task")
	}

	assertAllProcessed(t, src.data)
}

func TestStageRegistry(t *testing.T) {
	src := &sourceStub{data: stringDataValues(1)}
	sink := &sinkStub{}

	max := 3
	var count int
	task := TaskFunc(func(_ context.Context, data Data, tp TaskParams) (Data, error) {
		count++
		if count > max {
			return data, nil
		}

		c := data.Clone()
		tp.NewData() <- c
		go func(d Data) {
			r := tp.Registry()
			r["counter"] <- d
		}(c)

		return data, nil
	})

	p := NewPipeline(FIFO("counter", task))
	if err := p.Execute(context.TODO(), src, sink); err != nil {
		t.Errorf("Error executing the Pipeline: %v", err)
	}
	if c := count - 1; c != max {
		t.Errorf("Retry count does not match expected value.\nWanted:%v\nGot:%v\n", max, c)
	}

	assertAllProcessed(t, src.data)
}

func assertAllProcessed(t *testing.T, data []Data) {
	for i, d := range data {
		if data := d.(*stringData); data.processed != true {
			t.Errorf("Data %d not processed", i)
		}
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
	defer func() {
		s.t.Logf("[stage %d] exiting", sp.Position())
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-sp.Input():
			if !ok {
				return
			}
			s.t.Logf("[stage %d] received data: %v", sp.Position(), d)
			if s.err != nil {
				s.t.Logf("[stage %d] emit error: %v", sp.Position(), s.err)
				sp.Error().Append(s.err)
				return
			}

			if s.dropData {
				s.t.Logf("[stage %d] dropping data: %v", sp.Position(), d)
				sp.ProcessedData() <- d
				d.MarkAsProcessed()
				continue
			}

			s.t.Logf("[stage %d] emitting data: %v", sp.Position(), d)
			select {
			case <-ctx.Done():
				return
			case sp.Output() <- d:
			}
		}
	}
}

type sourceStub struct {
	index int
	data  []Data
	err   error
}

func (s *sourceStub) Next(context.Context) bool {
	if s.err != nil || s.index == len(s.data) {
		return false
	}
	s.index++
	return true
}
func (s *sourceStub) Error() error { return s.err }
func (s *sourceStub) Data() Data   { return s.data[s.index-1] }

type sinkStub struct {
	data []Data
	err  error
}

func (s *sinkStub) Consume(_ context.Context, d Data) error {
	s.data = append(s.data, d)
	return s.err
}

type stringData struct {
	processed bool
	val       string
}

func (s *stringData) Clone() Data      { return &stringData{val: s.val} }
func (s *stringData) MarkAsProcessed() { s.processed = true }
func (s *stringData) String() string   { return s.val }

func stringDataValues(num int) []Data {
	out := make([]Data, num)

	for i := 0; i < len(out); i++ {
		out[i] = &stringData{val: fmt.Sprint(i)}
	}
	return out
}
