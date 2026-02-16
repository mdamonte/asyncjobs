package models

import "time"

type TaskStatus string

const (
	StatusPending    TaskStatus = "PENDING"
	StatusProcessing TaskStatus = "PROCESSING"
	StatusSucceeded  TaskStatus = "SUCCEEDED"
	StatusFailed     TaskStatus = "FAILED"
	StatusCancelled  TaskStatus = "CANCELLED"
)

type Task struct {
	TaskID    string     `json:"taskId"`
	Type      string     `json:"type"`
	Status    TaskStatus `json:"status"`
	Payload   string     `json:"payload"` // JSON string
	Result    string     `json:"result,omitempty"`
	Error     string     `json:"error,omitempty"`
	Attempts  int        `json:"attempts"`
	CreatedAt time.Time  `json:"createdAt"`
	UpdatedAt time.Time  `json:"updatedAt"`
}
