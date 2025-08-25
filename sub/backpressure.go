package sub

// BackpressurePolicy defines how to handle message overflow.
type BackpressurePolicy int

const (
	BackpressureBlock BackpressurePolicy = iota // default
	BackpressureDropNewest
	BackpressureDropOldest
)

func (p BackpressurePolicy) String() string {
	switch p {
	case BackpressureBlock:
		return "block"
	case BackpressureDropNewest:
		return "drop-newest"
	case BackpressureDropOldest:
		return "drop-oldest"
	default:
		return "unknown"
	}
}
