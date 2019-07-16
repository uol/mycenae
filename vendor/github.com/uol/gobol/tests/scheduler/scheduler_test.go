package scheduler_test

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/uol/gobol/scheduler"
)

//
// Tests for the scheduler package
// author: rnojiri
//

// IncJob - a job to increment it's counter
type IncJob struct {
	counter int
}

// Execute - increments the counter
func (ij *IncJob) Execute() {
	ij.counter++
}

// createScheduler - creates a new scheduler using 100 millis ticks
func createScheduler(taskId string, autoStart bool) (*IncJob, *scheduler.Manager) {

	job := &IncJob{}

	manager := scheduler.New()
	err := manager.AddTask(scheduler.NewTask(taskId, 100*time.Millisecond, job), autoStart)
	if err != nil {
		panic(err)
	}

	return job, manager
}

// TestNoAutoStartTask - tests the scheduler with autostart feature disabled
func TestNoAutoStartTask(t *testing.T) {

	job, _ := createScheduler("x", false)

	time.Sleep(201 * time.Millisecond)

	assert.Equal(t, 0, job.counter, "expected no increment")
}

// TestAutoStartTask - tests the scheduler with autostart feature enabled
func TestAutoStartTask(t *testing.T) {

	job, _ := createScheduler("x", true)

	time.Sleep(101 * time.Millisecond)

	assert.Equal(t, 1, job.counter, "expected one increment")
}

// TestManualStartTask - tests the scheduler with manual start task
func TestManualStartTask(t *testing.T) {

	job, manager := createScheduler("x", false)

	time.Sleep(101 * time.Millisecond)

	assert.Equal(t, 0, job.counter, "expected no increment")

	if !assert.NoError(t, manager.StartTask("x"), "expected no error") {
		return
	}

	time.Sleep(101 * time.Millisecond)

	assert.Equal(t, 1, job.counter, "expected one increment")
}

// TestStopTask - test the scheduler stop function
func TestStopTask(t *testing.T) {

	job, manager := createScheduler("x", true)

	time.Sleep(201 * time.Millisecond)

	if !assert.NoError(t, manager.StopTask("x"), "expected no error") {
		return
	}

	time.Sleep(201 * time.Millisecond)

	assert.Equal(t, 2, job.counter, "expected two increments")

	if assert.Error(t, manager.StopTask("y"), "expected an error, task not exists") {
		return
	}
}

// TestRemoveTask - test the task removal function
func TestRemoveTask(t *testing.T) {

	job, manager := createScheduler("x", true)

	assert.Equal(t, 1, manager.GetNumTasks(), "expected only one task")

	time.Sleep(301 * time.Millisecond)

	assert.True(t, manager.RemoveTask("x"), "expected true")

	assert.Equal(t, 0, manager.GetNumTasks(), "expected no tasks")

	time.Sleep(301 * time.Millisecond)

	assert.Equal(t, 3, job.counter, "expected three increments")
}

// TestRemoveAllTasks - test the removal of all tasks function
func TestRemoveAllTasks(t *testing.T) {

	job1, manager := createScheduler("x", true)

	job2 := &IncJob{}
	if !assert.NoError(t, manager.AddTask(scheduler.NewTask("y", 100*time.Millisecond, job2), true), "error was not expected") {
		return
	}

	job3 := &IncJob{}
	if !assert.NoError(t, manager.AddTask(scheduler.NewTask("z", 100*time.Millisecond, job3), true), "error was not expected") {
		return
	}

	assert.Equal(t, 3, manager.GetNumTasks(), "expected only three tasks")

	time.Sleep(301 * time.Millisecond)

	manager.RemoveAllTasks()

	assert.Equal(t, 0, manager.GetNumTasks(), "expected no tasks")

	time.Sleep(301 * time.Millisecond)

	assert.Equal(t, 3, job1.counter, "expected three increments")
	assert.Equal(t, 3, job2.counter, "expected three increments")
	assert.Equal(t, 3, job3.counter, "expected three increments")
}

// TestSimultaneousTasks - test multiple running tasks
func TestSimultaneousTasks(t *testing.T) {

	job1, manager := createScheduler("1", true)

	job2 := &IncJob{}
	if !assert.NoError(t, manager.AddTask(scheduler.NewTask("2", 50*time.Millisecond, job2), true), "error was not expected") {
		return
	}

	job3 := &IncJob{}
	if !assert.NoError(t, manager.AddTask(scheduler.NewTask("3", 200*time.Millisecond, job3), true), "error was not expected") {
		return
	}

	time.Sleep(410 * time.Millisecond)

	assert.Equal(t, 3, manager.GetNumTasks(), "expected three tasks")

	assert.Equal(t, 4, job1.counter, "expected four increments")
	assert.Equal(t, 8, job2.counter, "expected eigth increments")
	assert.Equal(t, 2, job3.counter, "expected two increments")

	assert.True(t, manager.RemoveTask("2"), "expected true")

	assert.Equal(t, 2, manager.GetNumTasks(), "expected three tasks")

	time.Sleep(410 * time.Millisecond)

	assert.Equal(t, 8, job2.counter, "expected eigth increments")
}

// TestRestartTask - test restarting a task after a while
func TestRestartTask(t *testing.T) {

	job, manager := createScheduler("x", true)

	time.Sleep(201 * time.Millisecond)

	assert.Equal(t, 2, job.counter, "expected two increments")

	if !assert.NoError(t, manager.StopTask("x"), "expected no error when stopping a task") {
		return
	}

	time.Sleep(201 * time.Millisecond)

	assert.Equal(t, 2, job.counter, "still expecting two increments")

	if !assert.NoError(t, manager.StartTask("x"), "expected no error when starting a task") {
		return
	}

	time.Sleep(201 * time.Millisecond)

	assert.Equal(t, 4, job.counter, "now expecting four increments")
}

// TestInexistentTask - test restarting a task after a while
func TestInexistentTask(t *testing.T) {

	_, manager := createScheduler("x", false)

	if !assert.False(t, manager.RemoveTask("y"), "expected 'false' when removing a non existing task") {
		return
	}

	if !assert.Error(t, manager.StartTask("y"), "expected 'error' when removing a non existing task") {
		return
	}
}

// TestIfTaskExists - tests if a task already exists
func TestIfTaskExists(t *testing.T) {

	_, manager := createScheduler("x", false)

	if !assert.False(t, manager.Exists("y"), "expected 'false'") {
		return
	}

	assert.True(t, manager.Exists("x"), "expected 'true'")
}

// TestDuplicatedTask - tests when the same task is added twice
func TestDuplicatedTask(t *testing.T) {

	job1, manager := createScheduler("1", true)

	time.Sleep(200 * time.Millisecond)

	job2 := &IncJob{}
	if !assert.Error(t, manager.AddTask(scheduler.NewTask("1", 100*time.Millisecond, job2), true), "expected a error") {
		return
	}

	time.Sleep(201 * time.Millisecond)

	assert.Equal(t, 4, job1.counter, "expected four increments and no interruption")
}

// TestTaskList - test reading the task list
func TestTaskList(t *testing.T) {

	tasksNames := []string{"1", "2", "3", "4", "5"}

	_, manager := createScheduler(tasksNames[0], true)

	for i := 1; i < len(tasksNames); i++ {
		job := &IncJob{}
		if !assert.NoError(t, manager.AddTask(scheduler.NewTask(tasksNames[i], 50*time.Millisecond, job), true), "error was not expected") {
			return
		}
	}

	assert.Equal(t, len(tasksNames), manager.GetNumTasks(), fmt.Sprintf("expected %d tasks", len(tasksNames)))
	assert.Len(t, manager.GetTasks(), len(tasksNames), fmt.Sprintf("expected %d tasks", len(tasksNames)))

	tasks := manager.GetTasksIDs()
	taskMap := map[string]bool{}
	for _, name := range tasks {
		taskMap[name] = true
	}

	for _, name := range tasksNames {
		if !assert.True(t, taskMap[name], "expected task named: "+name) {
			return
		}
	}
}

// TestIfTaskIsRunning - tests if a task is running
func TestIfTaskIsRunning(t *testing.T) {

	job, manager := createScheduler("x", false)

	if !assert.False(t, manager.IsRunning("x"), "the task should be stopped") {
		return
	}

	err := manager.StartTask("x")
	if err != nil {
		panic(err)
	}

	time.Sleep(201 * time.Millisecond)

	assert.Equal(t, 2, job.counter, "expected 2 increments and no interruption")

	if !assert.True(t, manager.IsRunning("x"), "the task should be running") {
		return
	}

	time.Sleep(201 * time.Millisecond)

	assert.Equal(t, 4, job.counter, "expected 2 increments and no interruption")

	err = manager.StopTask("x")
	if err != nil {
		panic(err)
	}

	time.Sleep(201 * time.Millisecond)

	assert.Equal(t, 4, job.counter, "expected 2 increments and no interruption")

	if !assert.False(t, manager.IsRunning("x"), "the task should be stopped") {
		return
	}
}

// TestAccessors - tests some common accessors
func TestAccessors(t *testing.T) {

	job1, manager := createScheduler("x", false)

	job2 := &IncJob{}
	if !assert.NoError(t, manager.AddTask(scheduler.NewTask("y", 50*time.Millisecond, job2), true), "error was not expected") {
		return
	}

	job3 := &IncJob{}
	if !assert.NoError(t, manager.AddTask(scheduler.NewTask("z", 50*time.Millisecond, job3), true), "error was not expected") {
		return
	}

	if !assert.True(t, reflect.DeepEqual(job1, manager.GetTask("x").(*scheduler.Task).Job), "expected the same job1") {
		return
	}

	if !assert.True(t, reflect.DeepEqual(job2, manager.GetTask("y").(*scheduler.Task).Job), "expected the same job2") {
		return
	}

	if !assert.True(t, reflect.DeepEqual(job3, manager.GetTask("z").(*scheduler.Task).Job), "expected the same job3") {
		return
	}

	jobMap := map[string]*IncJob{
		"x": job1,
		"y": job2,
		"z": job3,
	}

	tasksInterface := manager.GetTasks()
	for _, taskInterface := range tasksInterface {
		curTask := taskInterface.(*scheduler.Task)
		expectedJob, ok := jobMap[curTask.ID]
		if !assert.True(t, ok, fmt.Sprintf("expected the task '%s'", curTask.ID)) {
			return
		}
		if !assert.True(t, reflect.DeepEqual(curTask.Job.(*IncJob), expectedJob), "expected the same job") {
			return
		}
	}

}
