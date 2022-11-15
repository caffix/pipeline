package pipeline

import (
	"context"
	"errors"
	"regexp"
	"testing"
)

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
