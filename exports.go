package stream

// Re-export core types from subpackages for backward compatibility
import (
	"github.com/a2y-d5l/go-stream/client"
	"github.com/a2y-d5l/go-stream/message"
	"github.com/a2y-d5l/go-stream/pub"
	"github.com/a2y-d5l/go-stream/sub"
	"github.com/a2y-d5l/go-stream/topic"
)

// Core types
type Stream = client.Stream
type Message = message.Message
type Topic = topic.Topic

// Constructor
var New = client.New
var NewTopic = topic.New

// Subscriber types
type Subscriber = sub.Subscriber
type SubscriberFunc = sub.SubscriberFunc
type Subscription = sub.Subscription

// Publisher types  
type Publisher = pub.Publisher

// Option types
type StreamOption = client.Option
type PublishOption = pub.Option
type SubscribeOption = sub.Option