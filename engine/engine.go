package engine

import "time"

// Interface represents behaviors of a delayed queue engine.
type Interface interface {
	// Push an element to specified queue with given delay duration
	Push(name []byte, element []byte, delay time.Duration) error
	// Pop ready elements from a queue, with count 'limit'
	Pop(name []byte, limit int) ([][]byte, error)
	// Get length of a queue
	Len(name []byte) (int, error)

	Queues() ([][]byte, error)

	Close() error

	Del(name []byte) (int, error)
}
