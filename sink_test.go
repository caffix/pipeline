package pipeline

import (
	"context"
	"errors"
	"regexp"
	"testing"
)

func TestSinkErrorHandling(t *testing.T) {
	src := &sourceStub{data: stringDataValues(3)}
	sink := &sinkStub{err: errors.New("sink error")}

	p := NewPipeline(testStage{t: t})
	re := regexp.MustCompile("(?s).*pipeline output sink: sink error.*")
	if err := p.Execute(context.TODO(), src, sink); err == nil || !re.MatchString(err.Error()) {
		t.Errorf("Error did not match the expectation: %v", err)
	}
}
