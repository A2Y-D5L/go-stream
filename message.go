package stream

import "time"

type Message struct {
	Topic   Topic
	Data    []byte
	Headers map[string]string
	ID      string
	Time    time.Time
}
