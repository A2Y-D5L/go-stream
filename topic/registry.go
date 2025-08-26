package topic

import (
	"errors"
	"sync"
	"time"
)

// ErrTopicNotRegistered indicates the topic is not registered in the registry
var ErrTopicNotRegistered = errors.New("topic not registered")

// Registry manages topic registrations and metadata
type Registry struct {
	mu     sync.RWMutex
	topics map[Topic]*TopicInfo
}

// TopicInfo contains metadata about a registered topic
type TopicInfo struct {
	Topic       Topic  `json:"topic"`
	Description string `json:"description"`
	Mode        Mode   `json:"mode"`
	Schema      string `json:"schema,omitempty"`
	CreatedAt   int64  `json:"created_at"`
}

// NewRegistry creates a new topic registry
func NewRegistry() *Registry {
	return &Registry{
		topics: make(map[Topic]*TopicInfo),
	}
}

// Register registers a topic with the given information
func (r *Registry) Register(topic Topic, info *TopicInfo) error {
	if err := Validate(topic); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.topics[topic]; exists {
		return ErrAlreadyExists
	}

	if info == nil {
		info = &TopicInfo{
			Topic:     topic,
			Mode:      ModeCore,
			CreatedAt: currentTimeMillis(),
		}
	} else {
		info.Topic = topic
		if info.CreatedAt == 0 {
			info.CreatedAt = currentTimeMillis()
		}
	}

	r.topics[topic] = info
	return nil
}

// Unregister removes a topic from the registry
func (r *Registry) Unregister(topic Topic) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.topics[topic]; !exists {
		return ErrTopicNotRegistered
	}

	delete(r.topics, topic)
	return nil
}

// Get retrieves topic information
func (r *Registry) Get(topic Topic) (*TopicInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	info, exists := r.topics[topic]
	if !exists {
		return nil, ErrTopicNotRegistered
	}

	// Return a copy to prevent external modification
	return &TopicInfo{
		Topic:       info.Topic,
		Description: info.Description,
		Mode:        info.Mode,
		Schema:      info.Schema,
		CreatedAt:   info.CreatedAt,
	}, nil
}

// List returns all registered topics
func (r *Registry) List() []*TopicInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	topics := make([]*TopicInfo, 0, len(r.topics))
	for _, info := range r.topics {
		// Return copies to prevent external modification
		topics = append(topics, &TopicInfo{
			Topic:       info.Topic,
			Description: info.Description,
			Mode:        info.Mode,
			Schema:      info.Schema,
			CreatedAt:   info.CreatedAt,
		})
	}

	return topics
}

// Exists checks if a topic is registered
func (r *Registry) Exists(topic Topic) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	_, exists := r.topics[topic]
	return exists
}

// Count returns the number of registered topics
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.topics)
}

// Clear removes all topics from the registry
func (r *Registry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.topics = make(map[Topic]*TopicInfo)
}

// GetByMode returns all topics with the specified mode
func (r *Registry) GetByMode(mode Mode) []*TopicInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var topics []*TopicInfo
	for _, info := range r.topics {
		if info.Mode == mode {
			topics = append(topics, &TopicInfo{
				Topic:       info.Topic,
				Description: info.Description,
				Mode:        info.Mode,
				Schema:      info.Schema,
				CreatedAt:   info.CreatedAt,
			})
		}
	}

	return topics
}

// currentTimeMillis returns the current time in milliseconds
func currentTimeMillis() int64 {
	return time.Now().UnixMilli()
}
