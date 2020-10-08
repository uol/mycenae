package scheduler

import (
	"fmt"
	"sync"
	"sync/atomic"
)

//
// Manages tasks to be executed repeatedly
// author: rnojiri
//

// Manager - schedules all expression executions
type Manager struct {
	taskMap sync.Map
}

// New - creates a new scheduler
func New() *Manager {

	return &Manager{
		taskMap: sync.Map{},
	}
}

// AddTask - adds a new task
func (m *Manager) AddTask(task *Task, autoStart bool) error {

	if _, exists := m.taskMap.Load(task.ID); exists {

		return fmt.Errorf("task id %s already exists", task.ID)
	}

	m.taskMap.Store(task.ID, task)

	if autoStart {

		if atomic.LoadUint32(&task.running) == 1 {
			return fmt.Errorf("task id %s already is running", task.ID)
		}

		task.Start()
	}

	return nil
}

// Exists - checks if a task exists
func (m *Manager) Exists(id string) bool {

	_, exists := m.taskMap.Load(id)

	return exists
}

// IsRunning - checks if a task is running
func (m *Manager) IsRunning(id string) bool {

	task, exists := m.taskMap.Load(id)

	if exists {

		return atomic.LoadUint32(&task.(*Task).running) == 1
	}

	return false
}

// RemoveTask - removes a task
func (m *Manager) RemoveTask(id string) bool {

	if task, exists := m.taskMap.Load(id); exists {

		task.(*Task).Stop()

		m.taskMap.Delete(id)

		return true
	}

	return false
}

// RemoveAllTasks - removes all tasks
func (m *Manager) RemoveAllTasks() {

	m.taskMap.Range(func(k, v interface{}) bool {

		v.(*Task).Stop()

		m.taskMap.Delete(k)

		return true
	})
}

// StopTask - stops a task
func (m *Manager) StopTask(id string) error {

	if task, exists := m.taskMap.Load(id); exists {

		if atomic.LoadUint32(&task.(*Task).running) == 1 {
			task.(*Task).Stop()
		} else {
			return fmt.Errorf("task id %s was not running (stop)", id)
		}

		return nil
	}

	return fmt.Errorf("task id %s does not exists (stop)", id)
}

// StartTask - starts a task
func (m *Manager) StartTask(id string) error {

	if task, exists := m.taskMap.Load(id); exists {

		if atomic.LoadUint32(&task.(*Task).running) == 0 {
			task.(*Task).Start()
		} else {
			return fmt.Errorf("task id %s is already running (start)", id)
		}

		return nil
	}

	return fmt.Errorf("task id %s does not exists (start)", id)
}

// GetNumTasks - returns the number of tasks
func (m *Manager) GetNumTasks() int {

	var length int

	m.taskMap.Range(func(_, _ interface{}) bool {
		length++
		return true
	})

	return length
}

// GetTasksIDs - returns a list of task IDs
func (m *Manager) GetTasksIDs() []string {

	tasks := []string{}

	m.taskMap.Range(func(k, _ interface{}) bool {
		tasks = append(tasks, k.(string))
		return true
	})

	return tasks
}

// GetTasks - returns a list of tasks
func (m *Manager) GetTasks() []interface{} {

	tasks := []interface{}{}

	m.taskMap.Range(func(_, v interface{}) bool {
		tasks = append(tasks, v)
		return true
	})

	return tasks
}

// GetTask - returns a task by it's ID
func (m *Manager) GetTask(id string) interface{} {

	if t, ok := m.taskMap.Load(id); ok {

		return t
	}

	return nil
}
