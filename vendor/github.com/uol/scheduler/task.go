package scheduler

import (
	"sync/atomic"
	"time"
)

//
// A single task to run a repetitive task
// author: rnojiri
//

// Job - a job to be executed
type job interface {
	Execute()
}

// Task - a scheduled task
type Task struct {
	ID       string
	Duration time.Duration
	Job      job
	running  uint32
}

// NewTask - creates a new task
func NewTask(id string, duration time.Duration, job job) *Task {

	return &Task{
		ID:       id,
		Duration: duration,
		Job:      job,
		running:  0,
	}
}

// Start - starts to run this task
func (t *Task) Start() {

	if atomic.LoadUint32(&t.running) == 1 {
		return
	}

	go func() {
		for {
			<-time.After(t.Duration)

			if atomic.LoadUint32(&t.running) == 0 {
				return
			}

			t.Job.Execute()
		}
	}()

	atomic.StoreUint32(&t.running, 1)
}

// Stop - stops the task
func (t *Task) Stop() {

	atomic.StoreUint32(&t.running, 0)
}
