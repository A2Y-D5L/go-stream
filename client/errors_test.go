package client

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMultiErr_Creation(t *testing.T) {
	t.Run("empty multi error", func(t *testing.T) {
		var merr multiErr
		
		assert.Empty(t, merr)
		assert.Equal(t, "", merr.Error())
	})

	t.Run("single error", func(t *testing.T) {
		var merr multiErr
		testErr := errors.New("test error")
		
		merr.add(testErr)
		
		assert.Len(t, merr, 1)
		assert.Equal(t, "test error", merr.Error())
	})

	t.Run("multiple errors", func(t *testing.T) {
		var merr multiErr
		err1 := errors.New("first error")
		err2 := errors.New("second error")
		err3 := errors.New("third error")
		
		merr.add(err1)
		merr.add(err2)
		merr.add(err3)
		
		assert.Len(t, merr, 3)
		
		errMsg := merr.Error()
		assert.Contains(t, errMsg, "multiple errors:")
		assert.Contains(t, errMsg, "first error")
		assert.Contains(t, errMsg, "second error")
		assert.Contains(t, errMsg, "third error")
	})
}

func TestMultiErr_Add(t *testing.T) {
	t.Run("add nil error", func(t *testing.T) {
		var merr multiErr
		
		merr.add(nil)
		
		assert.Len(t, merr, 1)
		assert.Nil(t, merr[0])
	})

	t.Run("add various error types", func(t *testing.T) {
		var merr multiErr
		
		standardErr := errors.New("standard error")
		wrappedErr := errors.New("wrapped: original error")
		
		merr.add(standardErr)
		merr.add(wrappedErr)
		
		assert.Len(t, merr, 2)
		assert.Equal(t, standardErr, merr[0])
		assert.Equal(t, wrappedErr, merr[1])
	})
}

func TestMultiErr_Error(t *testing.T) {
	t.Run("error message format", func(t *testing.T) {
		var merr multiErr
		
		merr.add(errors.New("network error"))
		merr.add(errors.New("timeout error"))
		
		errMsg := merr.Error()
		
		assert.Contains(t, errMsg, "multiple errors:")
		assert.Contains(t, errMsg, " - network error")
		assert.Contains(t, errMsg, " - timeout error")
	})

	t.Run("single error format", func(t *testing.T) {
		var merr multiErr
		
		merr.add(errors.New("single error"))
		
		assert.Equal(t, "single error", merr.Error())
	})

	t.Run("empty error format", func(t *testing.T) {
		var merr multiErr
		
		assert.Equal(t, "", merr.Error())
	})
}

func TestSentinelErrors(t *testing.T) {
	t.Run("custom error types", func(t *testing.T) {
		// Test creating custom errors similar to what might be used in the codebase
		closedErr := errors.New("stream is closed")
		healthErr := errors.New("stream is not healthy")
		
		assert.NotNil(t, closedErr)
		assert.NotNil(t, healthErr)
		
		assert.Contains(t, closedErr.Error(), "closed")
		assert.Contains(t, healthErr.Error(), "healthy")
	})

	t.Run("error identity", func(t *testing.T) {
		// Test that different error instances are not equal even with same message
		err1 := errors.New("test error")
		err2 := errors.New("test error")
		
		// Different instances with same message are not equal
		assert.False(t, errors.Is(err1, err2))
		
		// But if we use the same error variable, they should be equal
		err3 := err1
		assert.Equal(t, err1, err3)
		assert.True(t, errors.Is(err1, err3))
	})

	t.Run("error comparison", func(t *testing.T) {
		closedErr := errors.New("closed")
		healthErr := errors.New("not healthy")
		
		assert.NotEqual(t, closedErr, healthErr)
		assert.False(t, errors.Is(closedErr, healthErr))
		assert.False(t, errors.Is(healthErr, closedErr))
	})
}

func TestErrorWrapping(t *testing.T) {
	t.Run("wrapped errors in multiErr", func(t *testing.T) {
		var merr multiErr
		
		originalErr := errors.New("original error")
		wrappedErr := errors.New("wrapped: " + originalErr.Error())
		
		merr.add(wrappedErr)
		
		assert.Len(t, merr, 1)
		assert.Contains(t, merr.Error(), "original error")
		assert.Contains(t, merr.Error(), "wrapped:")
	})

	t.Run("error chain preservation", func(t *testing.T) {
		var merr multiErr
		
		baseErr := errors.New("base error")
		wrappedErr := errors.New("level 1: " + baseErr.Error())
		doubleWrappedErr := errors.New("level 2: " + wrappedErr.Error())
		
		merr.add(doubleWrappedErr)
		
		errMsg := merr.Error()
		assert.Contains(t, errMsg, "level 2:")
		assert.Contains(t, errMsg, "level 1:")
		assert.Contains(t, errMsg, "base error")
	})
}

func TestErrorEdgeCases(t *testing.T) {
	t.Run("very long error messages", func(t *testing.T) {
		var merr multiErr
		
		longMsg := string(make([]byte, 1000))
		for i := range longMsg {
			longMsg = string(append([]byte(longMsg[:i]), 'A'))
		}
		
		longErr := errors.New(longMsg)
		merr.add(longErr)
		
		assert.Contains(t, merr.Error(), longMsg)
	})

	t.Run("unicode in error messages", func(t *testing.T) {
		var merr multiErr
		
		unicodeErr := errors.New("错误: unicode error message 🚫")
		merr.add(unicodeErr)
		
		assert.Contains(t, merr.Error(), "错误")
		assert.Contains(t, merr.Error(), "🚫")
	})

	t.Run("empty error message", func(t *testing.T) {
		var merr multiErr
		
		emptyErr := errors.New("")
		merr.add(emptyErr)
		
		assert.Equal(t, "", merr.Error())
	})
}

func TestErrorConcurrency(t *testing.T) {
	t.Run("concurrent error addition", func(t *testing.T) {
		var merr multiErr
		
		const numGoroutines = 10
		errChan := make(chan error, numGoroutines)
		
		// Add errors concurrently
		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				err := errors.New("concurrent error")
				merr.add(err)
				errChan <- err
			}(i)
		}
		
		// Wait for all goroutines to complete
		for i := 0; i < numGoroutines; i++ {
			<-errChan
		}
		
		// Note: This test doesn't verify thread safety as multiErr is not thread-safe
		// It's just testing that concurrent access doesn't panic
		// The actual length may vary due to race conditions
		assert.GreaterOrEqual(t, len(merr), 1)
		assert.LessOrEqual(t, len(merr), numGoroutines)
	})
}

func TestErrorCompatibility(t *testing.T) {
	t.Run("multiErr implements error interface", func(t *testing.T) {
		var merr multiErr
		merr.add(errors.New("test"))
		
		var err error = merr
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "test")
	})

	t.Run("multiErr with errors.Is", func(t *testing.T) {
		var merr multiErr
		testErr := errors.New("specific error")
		merr.add(testErr)
		
		// Note: multiErr doesn't implement Is/As interfaces
		// This test documents current behavior
		assert.False(t, errors.Is(merr, testErr))
	})

	t.Run("multiErr type assertion", func(t *testing.T) {
		var merr multiErr
		merr.add(errors.New("test"))
		
		var err error = merr
		
		// Should be able to type assert back to multiErr
		if multiError, ok := err.(multiErr); ok {
			assert.Len(t, multiError, 1)
		} else {
			t.Error("Failed to type assert back to multiErr")
		}
	})
}

func TestErrorUsagePatterns(t *testing.T) {
	t.Run("accumulating errors from operations", func(t *testing.T) {
		var merr multiErr
		
		// Simulate multiple operations that might fail
		operations := []func() error{
			func() error { return errors.New("operation 1 failed") },
			func() error { return nil }, // Success
			func() error { return errors.New("operation 3 failed") },
			func() error { return nil }, // Success
			func() error { return errors.New("operation 5 failed") },
		}
		
		for _, op := range operations {
			if err := op(); err != nil {
				merr.add(err)
			}
		}
		
		assert.Len(t, merr, 3) // 3 operations failed
		
		errMsg := merr.Error()
		assert.Contains(t, errMsg, "operation 1 failed")
		assert.Contains(t, errMsg, "operation 3 failed")
		assert.Contains(t, errMsg, "operation 5 failed")
	})

	t.Run("conditional error return", func(t *testing.T) {
		var merr multiErr
		
		// Add some errors
		merr.add(errors.New("error 1"))
		merr.add(errors.New("error 2"))
		
		// Function that returns multiErr only if it has errors
		var result error
		if len(merr) > 0 {
			result = merr
		}
		
		assert.NotNil(t, result)
		assert.Contains(t, result.Error(), "multiple errors:")
	})
}
