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
