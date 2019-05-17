package scheduler

import "fmt"

//
// Manages tasks to be executed repeatedly
// author: rnojiri
//

// Manager - schedules all expression executions
type Manager struct {
	taskMap map[string]*Task
}

// New - creates a new scheduler
func New() *Manager {

	return &Manager{
		taskMap: map[string]*Task{},
	}
}

// AddTask - adds a new task
func (m *Manager) AddTask(task *Task, autoStart bool) error {

	if _, exists := m.taskMap[task.ID]; exists {

		return fmt.Errorf("task id %s already exists", task.ID)
	}

	m.taskMap[task.ID] = task

	if autoStart {

		if task.running {
			return fmt.Errorf("task id %s already is running", task.ID)
		}

		m.taskMap[task.ID].Start()
	}

	return nil
}

// Exists - checks if a task exists
func (m *Manager) Exists(id string) bool {

	_, exists := m.taskMap[id]

	return exists
}

// IsRunning - checks if a task is running
func (m *Manager) IsRunning(id string) bool {

	task, exists := m.taskMap[id]

	if exists {

		return task.running
	}

	return false
}

// RemoveTask - removes a task
func (m *Manager) RemoveTask(id string) bool {

	if task, exists := m.taskMap[id]; exists {

		task.Stop()

		delete(m.taskMap, id)

		return true
	}

	return false
}

// StopTask - stops a task
func (m *Manager) StopTask(id string) error {

	if task, exists := m.taskMap[id]; exists {

		if task.running {
			task.Stop()
		} else {
			return fmt.Errorf("task id %s was not running (stop)", id)
		}

		return nil
	}

	return fmt.Errorf("task id %s does not exists (stop)", id)
}

// StartTask - starts a task
func (m *Manager) StartTask(id string) error {

	if task, exists := m.taskMap[id]; exists {

		if !task.running {
			task.Start()
		} else {
			return fmt.Errorf("task id %s is already running (start)", id)
		}

		return nil
	}

	return fmt.Errorf("task id %s does not exists (start)", id)
}

// GetNumTasks - returns the number of tasks
func (m *Manager) GetNumTasks() int {

	return len(m.taskMap)
}

// GetTasksIDs - returns a list of task IDs
func (m *Manager) GetTasksIDs() []string {

	tasks := make([]string, len(m.taskMap))
	i := 0
	for k := range m.taskMap {
		tasks[i] = k
		i++
	}

	return tasks
}

// GetTasks - returns a list of tasks
func (m *Manager) GetTasks() []interface{} {

	tasks := make([]interface{}, len(m.taskMap))
	i := 0
	for _, v := range m.taskMap {
		tasks[i] = v
		i++
	}

	return tasks
}

// GetTask - returns a task by it's ID
func (m *Manager) GetTask(id string) interface{} {

	if t, ok := m.taskMap[id]; ok {

		return t
	}

	return nil
}
