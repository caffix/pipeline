# Data Pipeline

[![GoDoc](https://img.shields.io/static/v1?label=godoc&message=reference&color=blue)](https://pkg.go.dev/github.com/caffix/pipeline?tab=overview)
[![License](https://img.shields.io/github/license/caffix/pipeline)](https://www.apache.org/licenses/LICENSE-2.0)
[![Go Report](https://goreportcard.com/badge/github.com/caffix/pipeline)](https://goreportcard.com/report/github.com/caffix/pipeline)
[![CodeFactor](https://www.codefactor.io/repository/github/caffix/pipeline/badge)](https://www.codefactor.io/repository/github/caffix/pipeline)
[![Codecov](https://codecov.io/gh/caffix/pipeline/branch/master/graph/badge.svg)](https://codecov.io/gh/caffix/pipeline)
[![Follow on Twitter](https://img.shields.io/twitter/follow/jeff_foley.svg?logo=twitter)](https://twitter.com/jeff_foley)

Simple asynchronous data pipeline written in Go with support for concurrent tasks at each stage.

## Installation [![Go Version](https://img.shields.io/github/go-mod/go-version/caffix/pipeline)](https://golang.org/dl/)

```bash
go get -v -u github.com/caffix/pipeline
```

## Usage

The pipeline processes data provided by the input source through multiple stages and finally consumed by the output sink. All steps of the pipeline can be executing concurrently to maximize throughput. The pipeline can also be executed with buffering in-between each step in an attempt to minimize the impact of one stage taking longer than the others. Any error returned from a task being executed will terminate the pipeline. If a task returns `nil` data, the data is marked as processed and will not continue to the following stage.

### The Pipeline Data

The pipeline `Data` implements the `Clone` and `MarkAsProcessed` methods that performs a deep copy and marks the data to prevent further movement down the pipeline, respectively. Below is a simple pipeline `Data` implementation:

```golang
type stringData struct {
	processed bool
	val       string
}

// Clone implements the pipeline Data interface.
func (s *stringData) Clone() pipeline.Data { return &stringData{val: s.val} }

// Clone implements the pipeline Data interface.
func (s *stringData) MarkAsProcessed() { s.processed = true }

// String implements the Stringer interface.
func (s *stringData) String() string   { return s.val }
```

### The Input Source

The `InputSource` is an iterator that feeds the pipeline with data. Once the `Next` method returns `false`, the pipeline prevents the following stage from receiving data and begins an avalanche affect stopping each stage and eventually terminating the pipeline. Below is a simple input source:

```golang
type stringSource []pipeline.Data

var source stringSource = []*stringData{
    &stringData{val: "one"},
    &stringData{val: "two"},
    &stringData{val: "three"},
}

// Next implements the pipeline InputSource interface.
func (s stringSource) Next(context.Context) bool { return len(s) > 0 }

// Data implements the pipeline InputSource interface.
func (s stringSource) Data() pipeline.Data {
    defer func() { s = s[1:] }
    return s[0]
}

// Error implements the pipeline InputSource interface.
func (s stringSource) Error() error { return nil }
```

### The Output Sink

The `OutputSink` serves as a final landing spot for the data after successfully traversing the entire pipeline. All data reaching the output sink is automatically marked as processed. Below is a simple output sink:

```golang
type stringSink []string

// Consume implements the pipeline OutputSink interface.
func (s stringSink) Consume(ctx context.Context, data pipeline.Data) error {
    sd := data.(*stringData)

    s = append(s, sd.String())
    return nil
}
```

### The Stages

The pipeline steps are executed in sequential order by instances of `Stage`. The execution strategies implemented are `FIFO`, `FixedPool`, `DynamicPool`, `Broadcast`, and `Parallel`:

* `FIFO` - Executes the single Task
* `FixedPool` - Executes a fixed number of instances of the one specified Task
* `DynamicPool` - Executes a dynamic number of instances of the one specified Task
* `Broadcast` - Executes several unique Task instances concurrently moving Data ASAP
* `Parallel` - Executes several unique Task instances concurrently and passing through the original Data only once all the tasks complete successfully

The stage execution strategies can be combined to form desired pipelines. A Stage requires at least one Task to be executed at the step it represents in the pipeline. Each Task returns `Data` and an `error`. If the data returned is nil, it will not be sent to the following Stage. If the error is non-nil, the entire pipeline will be terminated. This allows users of the pipeline to have complete control over how failures impact the overall pipeline execution. A Task implements the `Process` method.

```golang
// TaskFunc is defined as a function with a Process method that calls the function
task := pipeline.TaskFunc(func(ctx context.Context, data pipeline.Data) (pipeline.Data, error) {
    var val int
    s := data.(*stringData)

	switch s {
    case "one":
        val = 1
    case "two":
        var = 2
    case "three":
        var = 3
    }

    data.val = fmt.Sprintf("%s - %d", s, val)
	return data, nil
})

stage := pipeline.FIFO(task)
```

### Executing the Pipeline

The Pipeline continues executing until all the Data from the input source is processed, an error takes place, or the provided Context expires. At a minimum, the pipeline requires an input source, a pass through stage, and the output sink.

```golang
p := NewPipeline(stage)

if err := p.Execute(context.TODO(), source, sink); err != nil {
    fmt.Printf("Error executing the pipeline: %v\n", err)
}
```

## Future Features

Some additional features would bring value to this data pipeline implementation.

### Logging

No logging is built into this pipeline implementation and this could be quite useful to have.

### Metrics and Monitoring

It would be helpful to have the ability to monitor stage and task performance such as how long each is taking to execute, the number of Data instances processes, the number of successes and failures, etc.

### Stage and Task Retries

It could be useful to have optional retries for stages and tasks.

### Messaging to Stages and Tasks

It could be helpful to support event-driven communication between tasks. This could allow Data to be sent to specified stages.

### Task Implementations for Common Use Cases

This pipeline implementation is very abstract, which allows users to perform nearly any set of steps. Currently, users must implement their own tasks. Some tasks are very common and the project could build support for such activities. For example, executing a script pulled from a Git repo.

### Support for Configuration Files

As the implementation becomes from complex, it could be helpful to support the use of configuration files and reduce the level of effort necessary to build a pipeline. For example, the configuration file could specify when tasks should be output to alternative stages.

### Develop Additional Stage Execute Strategies

While the current execution strategies work for many use cases, there could be opportunities to develop additional stage types that ease pipeline development.

## Licensing [![License](https://img.shields.io/github/license/caffix/pipeline)](https://www.apache.org/licenses/LICENSE-2.0)

This program is free software: you can redistribute it and/or modify it under the terms of the [Apache license](LICENSE).
