package message

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test data structures for codec testing
type CodecTestUser struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

type CodecTestOrder struct {
	OrderID   string    `json:"order_id"`
	UserID    int       `json:"user_id"`
	Amount    float64   `json:"amount"`
	CreatedAt time.Time `json:"created_at"`
}

type CodecTestComplexData struct {
	Metadata   map[string]any       `json:"metadata"`
	Tags       []string             `json:"tags"`
	Nested     *CodecTestNestedData `json:"nested"`
	OptionalID *int                 `json:"optional_id,omitempty"`
}

type CodecTestNestedData struct {
	Level int            `json:"level"`
	Items map[string]any `json:"items"`
}

// Mock codec for testing codec resolution
type mockCodec struct {
	name        string
	contentType string
	encodeErr   error
	decodeErr   error
	encoded     []byte
}

func (m *mockCodec) Encode(v any) ([]byte, error) {
	if m.encodeErr != nil {
		return nil, m.encodeErr
	}
	if m.encoded != nil {
		return m.encoded, nil
	}
	return []byte(fmt.Sprintf("%s-encoded-%v", m.name, v)), nil
}

func (m *mockCodec) Decode(data []byte, v any) error {
	if m.decodeErr != nil {
		return m.decodeErr
	}
	// Simple mock decode - just set a string representation
	if ptr, ok := v.(*string); ok {
		*ptr = fmt.Sprintf("%s-decoded-%s", m.name, string(data))
	}
	return nil
}

func (m *mockCodec) ContentType() string {
	return m.contentType
}

// --------------------- JSON Codec Tests ---------------------

func TestJSONCodec_BasicTypes(t *testing.T) {
	codec := JSONCodec

	tests := []struct {
		name  string
		input any
		want  any
	}{
		{"string", "hello world", "hello world"},
		{"int", 42, float64(42)}, // JSON unmarshals numbers as float64
		{"float64", 3.14159, 3.14159},
		{"bool", true, true},
		{"nil", nil, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			data, err := codec.Encode(tt.input)
			require.NoError(t, err)
			assert.NotEmpty(t, data)

			// Decode back to same type
			var decoded any
			err = codec.Decode(data, &decoded)
			require.NoError(t, err)
			assert.Equal(t, tt.want, decoded)
		})
	}
}

func TestJSONCodec_Structs(t *testing.T) {
	codec := JSONCodec

	tests := []struct {
		name   string
		input  any
		target any
	}{
		{
			name:   "simple struct",
			input:  CodecTestUser{ID: 123, Name: "John Doe", Email: "john@example.com"},
			target: &CodecTestUser{},
		},
		{
			name: "struct with time",
			input: CodecTestOrder{
				OrderID:   "ORD-001",
				UserID:    123,
				Amount:    99.99,
				CreatedAt: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
			},
			target: &CodecTestOrder{},
		},
		{
			name: "complex nested struct",
			input: CodecTestComplexData{
				Metadata: map[string]any{"version": "1.0", "type": "test"},
				Tags:     []string{"tag1", "tag2", "tag3"},
				Nested: &CodecTestNestedData{
					Level: 2,
					Items: map[string]any{"key1": "value1", "key2": float64(42)}, // Use float64 for JSON compatibility
				},
				OptionalID: func() *int { i := 999; return &i }(),
			},
			target: &CodecTestComplexData{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			data, err := codec.Encode(tt.input)
			require.NoError(t, err)
			assert.NotEmpty(t, data)

			// Verify it's valid JSON
			assert.True(t, json.Valid(data))

			// Decode
			err = codec.Decode(data, tt.target)
			require.NoError(t, err)
			assert.Equal(t, tt.input, getDeref(tt.target))
		})
	}
}

func TestJSONCodec_Arrays(t *testing.T) {
	codec := JSONCodec

	tests := []struct {
		name   string
		input  any
		target any
	}{
		{"string array", []string{"a", "b", "c"}, &[]string{}},
		{"int array", []int{1, 2, 3, 4, 5}, &[]int{}},
		{"struct array", []CodecTestUser{
			{ID: 1, Name: "User1", Email: "user1@test.com"},
			{ID: 2, Name: "User2", Email: "user2@test.com"},
		}, &[]CodecTestUser{}},
		{"empty array", []string{}, &[]string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := codec.Encode(tt.input)
			require.NoError(t, err)

			err = codec.Decode(data, tt.target)
			require.NoError(t, err)
			assert.Equal(t, tt.input, getDeref(tt.target))
		})
	}
}

func TestJSONCodec_Maps(t *testing.T) {
	codec := JSONCodec

	tests := []struct {
		name   string
		input  any
		target any
	}{
		{
			"string map",
			map[string]string{"key1": "value1", "key2": "value2"},
			&map[string]string{},
		},
		{
			"interface map",
			map[string]any{"string": "value", "number": float64(42), "bool": true}, // Use float64 for JSON
			&map[string]any{},
		},
		{
			"nested map",
			map[string]map[string]int{"group1": {"a": 1, "b": 2}, "group2": {"c": 3}},
			&map[string]map[string]int{},
		},
		{"empty map", map[string]string{}, &map[string]string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := codec.Encode(tt.input)
			require.NoError(t, err)

			err = codec.Decode(data, tt.target)
			require.NoError(t, err)
			assert.Equal(t, tt.input, getDeref(tt.target))
		})
	}
}

func TestJSONCodec_ContentType(t *testing.T) {
	codec := JSONCodec
	assert.Equal(t, "application/json", codec.ContentType())
}

func TestJSONCodec_EdgeCases(t *testing.T) {
	codec := JSONCodec

	t.Run("encode nil", func(t *testing.T) {
		data, err := codec.Encode(nil)
		require.NoError(t, err)
		assert.Equal(t, []byte("null"), data)
	})

	t.Run("decode into nil pointer", func(t *testing.T) {
		data := []byte(`"test"`)
		err := codec.Decode(data, nil)
		assert.Error(t, err)
	})

	t.Run("decode invalid JSON", func(t *testing.T) {
		invalidJSON := []byte(`{"incomplete": `)
		var result map[string]any
		err := codec.Decode(invalidJSON, &result)
		assert.Error(t, err)
	})

	t.Run("encode channel (unsupported)", func(t *testing.T) {
		ch := make(chan int)
		defer close(ch)
		_, err := codec.Encode(ch)
		assert.Error(t, err)
	})

	t.Run("decode type mismatch", func(t *testing.T) {
		data := []byte(`"string value"`)
		var num int
		err := codec.Decode(data, &num)
		assert.Error(t, err)
	})
}

// --------------------- Codec Resolution Tests ---------------------

func TestCodecResolution_PublishOption(t *testing.T) {
	// Note: This test demonstrates the intended codec resolution behavior
	// The actual implementation would need to be extended to support custom codecs

	t.Run("custom codec via publish option", func(t *testing.T) {
		// This would test WithCodec(customCodec) taking precedence
		t.Skip("Codec resolution not yet implemented - placeholder for future feature")
	})
}

func TestCodecResolution_TopicDefault(t *testing.T) {
	t.Run("topic-level codec default", func(t *testing.T) {
		// This would test topic-specific codec configuration
		t.Skip("Topic-level codec configuration not yet implemented - placeholder for future feature")
	})
}

func TestCodecResolution_StreamDefault(t *testing.T) {
	t.Run("stream-level codec default", func(t *testing.T) {
		// This would test stream-wide codec configuration
		t.Skip("Stream-level codec configuration not yet implemented - placeholder for future feature")
	})
}

func TestCodecResolution_FallbackToJSON(t *testing.T) {
	t.Run("fallback to JSON when no codec specified", func(t *testing.T) {
		// Current behavior - always uses JSON
		codec := JSONCodec

		user := CodecTestUser{ID: 1, Name: "Test", Email: "test@example.com"}
		data, err := codec.Encode(user)
		require.NoError(t, err)

		var decoded CodecTestUser
		err = codec.Decode(data, &decoded)
		require.NoError(t, err)
		assert.Equal(t, user, decoded)
	})
}

// --------------------- Custom Codec Tests ---------------------

func TestCustomCodec_Implementation(t *testing.T) {
	t.Run("mock codec basic functionality", func(t *testing.T) {
		codec := &mockCodec{
			name:        "test",
			contentType: "application/test",
		}

		// Test encode
		data, err := codec.Encode("test data")
		require.NoError(t, err)
		assert.Equal(t, "test-encoded-test data", string(data))

		// Test decode
		var result string
		err = codec.Decode([]byte("some data"), &result)
		require.NoError(t, err)
		assert.Equal(t, "test-decoded-some data", result)

		// Test content type
		assert.Equal(t, "application/test", codec.ContentType())
	})

	t.Run("mock codec with errors", func(t *testing.T) {
		encodeErr := errors.New("encode error")
		decodeErr := errors.New("decode error")

		codec := &mockCodec{
			name:        "failing",
			contentType: "application/fail",
			encodeErr:   encodeErr,
			decodeErr:   decodeErr,
		}

		// Test encode error
		_, err := codec.Encode("data")
		assert.Equal(t, encodeErr, err)

		// Test decode error
		var result string
		err = codec.Decode([]byte("data"), &result)
		assert.Equal(t, decodeErr, err)
	})
}

// --------------------- Performance Tests ---------------------

func BenchmarkJSONCodec_EncodeSimpleStruct(b *testing.B) {
	codec := &jsonCodec{}
	user := CodecTestUser{ID: 123, Name: "John Doe", Email: "john@example.com"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := codec.Encode(user)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONCodec_DecodeSimpleStruct(b *testing.B) {
	codec := &jsonCodec{}
	user := CodecTestUser{ID: 123, Name: "John Doe", Email: "john@example.com"}
	data, _ := codec.Encode(user)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var decoded CodecTestUser
		err := codec.Decode(data, &decoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONCodec_EncodeComplexStruct(b *testing.B) {
	codec := &jsonCodec{}
	complex := CodecTestComplexData{
		Metadata: map[string]any{"version": "1.0", "type": "test", "count": float64(100)},
		Tags:     []string{"tag1", "tag2", "tag3", "tag4", "tag5"},
		Nested: &CodecTestNestedData{
			Level: 3,
			Items: map[string]any{
				"item1": "value1",
				"item2": float64(42),
				"item3": []string{"a", "b", "c"},
				"item4": map[string]int{"x": 1, "y": 2},
			},
		},
		OptionalID: func() *int { i := 999; return &i }(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := codec.Encode(complex)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONCodec_DecodeComplexStruct(b *testing.B) {
	codec := &jsonCodec{}
	complex := CodecTestComplexData{
		Metadata: map[string]any{"version": "1.0", "type": "test", "count": float64(100)},
		Tags:     []string{"tag1", "tag2", "tag3", "tag4", "tag5"},
		Nested: &CodecTestNestedData{
			Level: 3,
			Items: map[string]any{
				"item1": "value1",
				"item2": float64(42),
				"item3": []string{"a", "b", "c"},
				"item4": map[string]int{"x": 1, "y": 2},
			},
		},
		OptionalID: func() *int { i := 999; return &i }(),
	}
	data, _ := codec.Encode(complex)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var decoded CodecTestComplexData
		err := codec.Decode(data, &decoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --------------------- Helper Functions ---------------------

// getDeref safely dereferences a pointer to get the underlying value
func getDeref(v any) any {
	switch ptr := v.(type) {
	case *string:
		return *ptr
	case *int:
		return *ptr
	case *float64:
		return *ptr
	case *bool:
		return *ptr
	case *CodecTestUser:
		return *ptr
	case *CodecTestOrder:
		return *ptr
	case *CodecTestComplexData:
		return *ptr
	case *[]string:
		return *ptr
	case *[]int:
		return *ptr
	case *[]CodecTestUser:
		return *ptr
	case *map[string]string:
		return *ptr
	case *map[string]any:
		return *ptr
	case *map[string]map[string]int:
		return *ptr
	default:
		return v
	}
}
