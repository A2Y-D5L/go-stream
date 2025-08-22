package stream

import "errors"

// multiErr is a simple error accumulator.
type multiErr []error

func (m *multiErr) add(err error) { *m = append(*m, err) }

func (m multiErr) Error() string {
	if len(m) == 0 {
		return ""
	}
	if len(m) == 1 {
		return m[0].Error()
	}
	msg := "multiple errors:"
	for _, e := range m {
		msg += "\n - " + e.Error()
	}
	return msg
}

var (
	// ErrCodecUnsupported indicates the codec is not supported.
	ErrCodecUnsupported = errors.New("unsupported codec")
	// ErrTimeout indicates the operation timed out.
	ErrTimeout = errors.New("operation timeout")
)
