package topic

import (
	"testing"
)

func TestRegistry_Register(t *testing.T) {
	registry := NewRegistry()

	topic := Topic("test.topic")
	info := &TopicInfo{
		Description: "Test topic",
		Mode:        ModeCore,
	}

	err := registry.Register(topic, info)
	if err != nil {
		t.Fatalf("Failed to register topic: %v", err)
	}

	// Test duplicate registration
	err = registry.Register(topic, info)
	if err != ErrAlreadyExists {
		t.Fatalf("Expected ErrTopicExists, got: %v", err)
	}
}

func TestRegistry_Get(t *testing.T) {
	registry := NewRegistry()

	topic := Topic("test.topic")
	originalInfo := &TopicInfo{
		Description: "Test topic",
		Mode:        ModeCore,
	}

	err := registry.Register(topic, originalInfo)
	if err != nil {
		t.Fatalf("Failed to register topic: %v", err)
	}

	retrievedInfo, err := registry.Get(topic)
	if err != nil {
		t.Fatalf("Failed to get topic: %v", err)
	}

	if retrievedInfo.Topic != topic {
		t.Errorf("Expected topic %s, got %s", topic, retrievedInfo.Topic)
	}

	if retrievedInfo.Description != originalInfo.Description {
		t.Errorf("Expected description %s, got %s", originalInfo.Description, retrievedInfo.Description)
	}

	if retrievedInfo.Mode != originalInfo.Mode {
		t.Errorf("Expected mode %v, got %v", originalInfo.Mode, retrievedInfo.Mode)
	}
}

func TestRegistry_Unregister(t *testing.T) {
	registry := NewRegistry()

	topic := Topic("test.topic")
	info := &TopicInfo{
		Description: "Test topic",
		Mode:        ModeCore,
	}

	err := registry.Register(topic, info)
	if err != nil {
		t.Fatalf("Failed to register topic: %v", err)
	}

	err = registry.Unregister(topic)
	if err != nil {
		t.Fatalf("Failed to unregister topic: %v", err)
	}

	// Test getting unregistered topic
	_, err = registry.Get(topic)
	if err != ErrTopicNotRegistered {
		t.Fatalf("Expected ErrTopicNotRegistered, got: %v", err)
	}

	// Test unregistering non-existent topic
	err = registry.Unregister(topic)
	if err != ErrTopicNotRegistered {
		t.Fatalf("Expected ErrTopicNotRegistered, got: %v", err)
	}
}

func TestRegistry_List(t *testing.T) {
	registry := NewRegistry()

	topics := []Topic{"topic1", "topic2", "topic3"}
	for _, topic := range topics {
		err := registry.Register(topic, nil)
		if err != nil {
			t.Fatalf("Failed to register topic %s: %v", topic, err)
		}
	}

	list := registry.List()
	if len(list) != len(topics) {
		t.Fatalf("Expected %d topics, got %d", len(topics), len(list))
	}

	// Verify all topics are in the list
	topicMap := make(map[Topic]bool)
	for _, info := range list {
		topicMap[info.Topic] = true
	}

	for _, topic := range topics {
		if !topicMap[topic] {
			t.Errorf("Topic %s not found in list", topic)
		}
	}
}

func TestRegistry_Exists(t *testing.T) {
	registry := NewRegistry()

	topic := Topic("test.topic")
	
	if registry.Exists(topic) {
		t.Error("Topic should not exist before registration")
	}

	err := registry.Register(topic, nil)
	if err != nil {
		t.Fatalf("Failed to register topic: %v", err)
	}

	if !registry.Exists(topic) {
		t.Error("Topic should exist after registration")
	}
}

func TestRegistry_Count(t *testing.T) {
	registry := NewRegistry()

	if registry.Count() != 0 {
		t.Error("Registry should start empty")
	}

	topics := []Topic{"topic1", "topic2", "topic3"}
	for i, topic := range topics {
		err := registry.Register(topic, nil)
		if err != nil {
			t.Fatalf("Failed to register topic %s: %v", topic, err)
		}

		expectedCount := i + 1
		if registry.Count() != expectedCount {
			t.Errorf("Expected count %d, got %d", expectedCount, registry.Count())
		}
	}
}

func TestRegistry_GetByMode(t *testing.T) {
	registry := NewRegistry()

	// Register topics with different modes
	coreTopics := []Topic{"core1", "core2"}
	jsTopics := []Topic{"js1", "js2"}

	for _, topic := range coreTopics {
		err := registry.Register(topic, &TopicInfo{Mode: ModeCore})
		if err != nil {
			t.Fatalf("Failed to register core topic %s: %v", topic, err)
		}
	}

	for _, topic := range jsTopics {
		err := registry.Register(topic, &TopicInfo{Mode: ModeJetStream})
		if err != nil {
			t.Fatalf("Failed to register JS topic %s: %v", topic, err)
		}
	}

	// Test getting core topics
	coreList := registry.GetByMode(ModeCore)
	if len(coreList) != len(coreTopics) {
		t.Errorf("Expected %d core topics, got %d", len(coreTopics), len(coreList))
	}

	// Test getting JS topics
	jsList := registry.GetByMode(ModeJetStream)
	if len(jsList) != len(jsTopics) {
		t.Errorf("Expected %d JS topics, got %d", len(jsTopics), len(jsList))
	}
}

func TestRegistry_Clear(t *testing.T) {
	registry := NewRegistry()

	// Register some topics
	topics := []Topic{"topic1", "topic2", "topic3"}
	for _, topic := range topics {
		err := registry.Register(topic, nil)
		if err != nil {
			t.Fatalf("Failed to register topic %s: %v", topic, err)
		}
	}

	if registry.Count() == 0 {
		t.Error("Registry should not be empty after registrations")
	}

	registry.Clear()

	if registry.Count() != 0 {
		t.Error("Registry should be empty after clear")
	}
}
