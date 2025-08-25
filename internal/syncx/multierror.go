package syncx

import (
	"fmt"
	"strings"
	"sync"
)

// MultiError collects multiple errors and implements the error interface
type MultiError struct {
	errors []error
	mu     sync.Mutex
}

// NewMultiError creates a new MultiError instance
func NewMultiError() *MultiError {
	return &MultiError{
		errors: make([]error, 0),
	}
}

// Add adds an error to the collection (thread-safe)
func (me *MultiError) Add(err error) {
	if err == nil {
		return
	}
	
	me.mu.Lock()
	me.errors = append(me.errors, err)
	me.mu.Unlock()
}

// Errors returns a copy of all collected errors
func (me *MultiError) Errors() []error {
	me.mu.Lock()
	defer me.mu.Unlock()
	
	result := make([]error, len(me.errors))
	copy(result, me.errors)
	return result
}

// Error implements the error interface
func (me *MultiError) Error() string {
	me.mu.Lock()
	defer me.mu.Unlock()
	
	if len(me.errors) == 0 {
		return ""
	}
	
	if len(me.errors) == 1 {
		return me.errors[0].Error()
	}
	
	var messages []string
	for i, err := range me.errors {
		messages = append(messages, fmt.Sprintf("[%d] %s", i+1, err.Error()))
	}
	
	return fmt.Sprintf("multiple errors occurred:\n%s", strings.Join(messages, "\n"))
}

// HasErrors returns true if any errors have been collected
func (me *MultiError) HasErrors() bool {
	me.mu.Lock()
	defer me.mu.Unlock()
	return len(me.errors) > 0
}

// Count returns the number of errors collected
func (me *MultiError) Count() int {
	me.mu.Lock()
	defer me.mu.Unlock()
	return len(me.errors)
}

// ToError returns nil if no errors, otherwise returns the MultiError
func (me *MultiError) ToError() error {
	if !me.HasErrors() {
		return nil
	}
	return me
}

// Clear removes all collected errors
func (me *MultiError) Clear() {
	me.mu.Lock()
	me.errors = me.errors[:0]
	me.mu.Unlock()
}

// First returns the first error or nil if no errors
func (me *MultiError) First() error {
	me.mu.Lock()
	defer me.mu.Unlock()
	
	if len(me.errors) == 0 {
		return nil
	}
	return me.errors[0]
}

// Last returns the last error or nil if no errors
func (me *MultiError) Last() error {
	me.mu.Lock()
	defer me.mu.Unlock()
	
	if len(me.errors) == 0 {
		return nil
	}
	return me.errors[len(me.errors)-1]
}
