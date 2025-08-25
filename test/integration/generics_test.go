package stream

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test types for generic testing
type GenericTestUser struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Email    string `json:"email"`
	Active   bool   `json:"active"`
	JoinedAt time.Time `json:"joined_at"`
}

type GenericTestOrder struct {
	OrderID   string    `json:"order_id"`
	UserID    int       `json:"user_id"`
	Amount    float64   `json:"amount"`
	Items     []string  `json:"items"`
	CreatedAt time.Time `json:"created_at"`
}

type GenericTestProduct struct {
	ID          int                    `json:"id"`
	Name        string                 `json:"name"`
	Price       float64                `json:"price"`
	Categories  []string               `json:"categories"`
	Metadata    map[string]any `json:"metadata"`
	Available   bool                   `json:"available"`
}

// Complex nested type for advanced generic testing
type GenericTestComplex struct {
	Header   GenericTestHeader            `json:"header"`
	Payload  any                  `json:"payload"`
	Metadata map[string]string            `json:"metadata"`
	Items    []GenericTestItem            `json:"items"`
	Optional *GenericTestOptional         `json:"optional,omitempty"`
}

type GenericTestHeader struct {
	Version   string    `json:"version"`
	Timestamp time.Time `json:"timestamp"`
	Source    string    `json:"source"`
}

type GenericTestItem struct {
	ID    string `json:"id"`
	Value int    `json:"value"`
}

type GenericTestOptional struct {
	Flag        bool   `json:"flag"`
	Description string `json:"description"`
}

// --------------------- Generic Type Safety Tests ---------------------

func TestGenerics_TypeSafety(t *testing.T) {
	t.Run("simple struct type safety", func(t *testing.T) {
		user := GenericTestUser{
			ID:       123,
			Name:     "John Doe",
			Email:    "john@example.com",
			Active:   true,
			JoinedAt: time.Now(),
		}

		// Test that the type is preserved
		assert.IsType(t, GenericTestUser{}, user)
		assert.Equal(t, 123, user.ID)
		assert.Equal(t, "John Doe", user.Name)
		assert.True(t, user.Active)
	})

	t.Run("slice type safety", func(t *testing.T) {
		users := []GenericTestUser{
			{ID: 1, Name: "User1", Email: "user1@test.com", Active: true},
			{ID: 2, Name: "User2", Email: "user2@test.com", Active: false},
		}

		assert.IsType(t, []GenericTestUser{}, users)
		assert.Len(t, users, 2)
		assert.Equal(t, 1, users[0].ID)
		assert.Equal(t, 2, users[1].ID)
	})

	t.Run("map type safety", func(t *testing.T) {
		userMap := map[string]GenericTestUser{
			"user1": {ID: 1, Name: "User1", Email: "user1@test.com"},
			"user2": {ID: 2, Name: "User2", Email: "user2@test.com"},
		}

		assert.IsType(t, map[string]GenericTestUser{}, userMap)
		assert.Len(t, userMap, 2)
		assert.Equal(t, 1, userMap["user1"].ID)
		assert.Equal(t, 2, userMap["user2"].ID)
	})

	t.Run("pointer type safety", func(t *testing.T) {
		user := &GenericTestUser{
			ID:    456,
			Name:  "Jane Doe",
			Email: "jane@example.com",
		}

		assert.IsType(t, (*GenericTestUser)(nil), user)
		assert.NotNil(t, user)
		assert.Equal(t, 456, user.ID)
		assert.Equal(t, "Jane Doe", user.Name)
	})
}

// --------------------- Generic JSON Codec Tests ---------------------

func TestGenerics_JSONCodec(t *testing.T) {
	t.Run("encode decode user", func(t *testing.T) {
		original := GenericTestUser{
			ID:       789,
			Name:     "Generic User",
			Email:    "generic@test.com",
			Active:   true,
			JoinedAt: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
		}

		// Encode
		data, err := json.Marshal(original)
		require.NoError(t, err)
		assert.True(t, json.Valid(data))

		// Decode
		var decoded GenericTestUser
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		assert.Equal(t, original.ID, decoded.ID)
		assert.Equal(t, original.Name, decoded.Name)
		assert.Equal(t, original.Email, decoded.Email)
		assert.Equal(t, original.Active, decoded.Active)
		assert.True(t, original.JoinedAt.Equal(decoded.JoinedAt))
	})

	t.Run("encode decode order", func(t *testing.T) {
		original := GenericTestOrder{
			OrderID:   "ORD-123",
			UserID:    456,
			Amount:    99.99,
			Items:     []string{"item1", "item2", "item3"},
			CreatedAt: time.Now(),
		}

		data, err := json.Marshal(original)
		require.NoError(t, err)

		var decoded GenericTestOrder
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		assert.Equal(t, original.OrderID, decoded.OrderID)
		assert.Equal(t, original.UserID, decoded.UserID)
		assert.Equal(t, original.Amount, decoded.Amount)
		assert.Equal(t, original.Items, decoded.Items)
	})

	t.Run("encode decode complex type", func(t *testing.T) {
		original := GenericTestComplex{
			Header: GenericTestHeader{
				Version:   "v1.0",
				Timestamp: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC),
				Source:    "test-source",
			},
			Payload: map[string]any{
				"type":  "test",
				"value": 42,
				"flag":  true,
			},
			Metadata: map[string]string{
				"env":    "test",
				"region": "us-west-2",
			},
			Items: []GenericTestItem{
				{ID: "item1", Value: 100},
				{ID: "item2", Value: 200},
			},
			Optional: &GenericTestOptional{
				Flag:        true,
				Description: "optional data",
			},
		}

		data, err := json.Marshal(original)
		require.NoError(t, err)

		var decoded GenericTestComplex
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		assert.Equal(t, original.Header.Version, decoded.Header.Version)
		assert.Equal(t, original.Header.Source, decoded.Header.Source)
		assert.Equal(t, original.Metadata, decoded.Metadata)
		assert.Len(t, decoded.Items, 2)
		assert.NotNil(t, decoded.Optional)
		assert.Equal(t, original.Optional.Flag, decoded.Optional.Flag)
	})
}

// --------------------- Generic Array and Slice Tests ---------------------

func TestGenerics_ArraysAndSlices(t *testing.T) {
	t.Run("slice of users", func(t *testing.T) {
		users := []GenericTestUser{
			{ID: 1, Name: "User1", Email: "user1@test.com", Active: true},
			{ID: 2, Name: "User2", Email: "user2@test.com", Active: false},
			{ID: 3, Name: "User3", Email: "user3@test.com", Active: true},
		}

		data, err := json.Marshal(users)
		require.NoError(t, err)

		var decoded []GenericTestUser
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		assert.Len(t, decoded, 3)
		for i, user := range users {
			assert.Equal(t, user.ID, decoded[i].ID)
			assert.Equal(t, user.Name, decoded[i].Name)
			assert.Equal(t, user.Active, decoded[i].Active)
		}
	})

	t.Run("empty slice", func(t *testing.T) {
		var users []GenericTestUser

		data, err := json.Marshal(users)
		require.NoError(t, err)
		assert.Equal(t, []byte("null"), data)

		var decoded []GenericTestUser
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)
		assert.Nil(t, decoded)
	})

	t.Run("slice with nil elements", func(t *testing.T) {
		userPtrs := []*GenericTestUser{
			{ID: 1, Name: "User1", Email: "user1@test.com"},
			nil,
			{ID: 3, Name: "User3", Email: "user3@test.com"},
		}

		data, err := json.Marshal(userPtrs)
		require.NoError(t, err)

		var decoded []*GenericTestUser
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		assert.Len(t, decoded, 3)
		assert.NotNil(t, decoded[0])
		assert.Nil(t, decoded[1])
		assert.NotNil(t, decoded[2])
		assert.Equal(t, 1, decoded[0].ID)
		assert.Equal(t, 3, decoded[2].ID)
	})

	t.Run("large slice performance", func(t *testing.T) {
		// Create large slice for performance testing
		users := make([]GenericTestUser, 1000)
		for i := range users {
			users[i] = GenericTestUser{
				ID:     i,
				Name:   "User" + string(rune(i)),
				Email:  "user" + string(rune(i)) + "@test.com",
				Active: i%2 == 0,
			}
		}

		start := time.Now()
		data, err := json.Marshal(users)
		encodeTime := time.Since(start)
		require.NoError(t, err)

		start = time.Now()
		var decoded []GenericTestUser
		err = json.Unmarshal(data, &decoded)
		decodeTime := time.Since(start)
		require.NoError(t, err)

		assert.Len(t, decoded, 1000)
		t.Logf("Encode time: %v, Decode time: %v", encodeTime, decodeTime)
	})
}

// --------------------- Generic Map Tests ---------------------

func TestGenerics_Maps(t *testing.T) {
	t.Run("map of users", func(t *testing.T) {
		userMap := map[string]GenericTestUser{
			"user1": {ID: 1, Name: "User1", Email: "user1@test.com"},
			"user2": {ID: 2, Name: "User2", Email: "user2@test.com"},
			"user3": {ID: 3, Name: "User3", Email: "user3@test.com"},
		}

		data, err := json.Marshal(userMap)
		require.NoError(t, err)

		var decoded map[string]GenericTestUser
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		assert.Len(t, decoded, 3)
		for key, expectedUser := range userMap {
			decodedUser, exists := decoded[key]
			assert.True(t, exists)
			assert.Equal(t, expectedUser.ID, decodedUser.ID)
			assert.Equal(t, expectedUser.Name, decodedUser.Name)
		}
	})

	t.Run("nested maps", func(t *testing.T) {
		nestedMap := map[string]map[string]GenericTestProduct{
			"electronics": {
				"laptop": {ID: 1, Name: "Laptop", Price: 999.99, Available: true},
				"phone":  {ID: 2, Name: "Phone", Price: 599.99, Available: true},
			},
			"books": {
				"novel":    {ID: 3, Name: "Novel", Price: 19.99, Available: true},
				"textbook": {ID: 4, Name: "Textbook", Price: 199.99, Available: false},
			},
		}

		data, err := json.Marshal(nestedMap)
		require.NoError(t, err)

		var decoded map[string]map[string]GenericTestProduct
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		assert.Len(t, decoded, 2)
		assert.Len(t, decoded["electronics"], 2)
		assert.Len(t, decoded["books"], 2)
		assert.Equal(t, "Laptop", decoded["electronics"]["laptop"].Name)
		assert.Equal(t, 999.99, decoded["electronics"]["laptop"].Price)
	})

	t.Run("map with any values", func(t *testing.T) {
		mixedMap := map[string]any{
			"user":    GenericTestUser{ID: 1, Name: "User1", Email: "user1@test.com"},
			"order":   GenericTestOrder{OrderID: "ORD-123", UserID: 1, Amount: 99.99},
			"string":  "just a string",
			"number":  42,
			"boolean": true,
		}

		data, err := json.Marshal(mixedMap)
		require.NoError(t, err)

		var decoded map[string]any
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		assert.Len(t, decoded, 5)
		assert.Equal(t, "just a string", decoded["string"])
		assert.Equal(t, float64(42), decoded["number"]) // JSON unmarshals numbers as float64
		assert.Equal(t, true, decoded["boolean"])
	})
}

// --------------------- Generic Pointer Tests ---------------------

func TestGenerics_Pointers(t *testing.T) {
	t.Run("pointer to struct", func(t *testing.T) {
		original := &GenericTestUser{
			ID:    999,
			Name:  "Pointer User",
			Email: "pointer@test.com",
		}

		data, err := json.Marshal(original)
		require.NoError(t, err)

		var decoded *GenericTestUser
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		assert.NotNil(t, decoded)
		assert.Equal(t, original.ID, decoded.ID)
		assert.Equal(t, original.Name, decoded.Name)
		assert.Equal(t, original.Email, decoded.Email)
	})

	t.Run("nil pointer", func(t *testing.T) {
		var original *GenericTestUser

		data, err := json.Marshal(original)
		require.NoError(t, err)
		assert.Equal(t, []byte("null"), data)

		var decoded *GenericTestUser
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)
		assert.Nil(t, decoded)
	})

	t.Run("optional fields with pointers", func(t *testing.T) {
		type OptionalFields struct {
			Required string  `json:"required"`
			Optional *string `json:"optional,omitempty"`
			Number   *int    `json:"number,omitempty"`
		}

		// With optional fields
		value := "optional value"
		number := 42
		withOptional := OptionalFields{
			Required: "required value",
			Optional: &value,
			Number:   &number,
		}

		data, err := json.Marshal(withOptional)
		require.NoError(t, err)

		var decoded OptionalFields
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		assert.Equal(t, "required value", decoded.Required)
		assert.NotNil(t, decoded.Optional)
		assert.Equal(t, "optional value", *decoded.Optional)
		assert.NotNil(t, decoded.Number)
		assert.Equal(t, 42, *decoded.Number)

		// Without optional fields
		withoutOptional := OptionalFields{
			Required: "required only",
		}

		data, err = json.Marshal(withoutOptional)
		require.NoError(t, err)

		var decoded2 OptionalFields
		err = json.Unmarshal(data, &decoded2)
		require.NoError(t, err)

		assert.Equal(t, "required only", decoded2.Required)
		assert.Nil(t, decoded2.Optional)
		assert.Nil(t, decoded2.Number)
	})
}

// --------------------- Generic Interface Tests ---------------------

func TestGenerics_Interfaces(t *testing.T) {
	t.Run("any handling", func(t *testing.T) {
		testCases := []struct {
			name  string
			value any
		}{
			{"string", "test string"},
			{"int", 42},
			{"float", 3.14159},
			{"bool", true},
			{"struct", GenericTestUser{ID: 1, Name: "Test"}},
			{"slice", []string{"a", "b", "c"}},
			{"map", map[string]int{"key": 123}},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				data, err := json.Marshal(tc.value)
				require.NoError(t, err)

				var decoded any
				err = json.Unmarshal(data, &decoded)
				require.NoError(t, err)

				// Note: JSON unmarshaling may change types (int -> float64)
				assert.NotNil(t, decoded)
			})
		}
	})

	t.Run("type assertion after unmarshaling", func(t *testing.T) {
		original := GenericTestUser{ID: 123, Name: "Test User", Email: "test@example.com"}

		data, err := json.Marshal(original)
		require.NoError(t, err)

		var decoded any
		err = json.Unmarshal(data, &decoded)
		require.NoError(t, err)

		// Should be a map[string]any after JSON unmarshaling
		if userMap, ok := decoded.(map[string]any); ok {
			assert.Equal(t, float64(123), userMap["id"]) // JSON numbers become float64
			assert.Equal(t, "Test User", userMap["name"])
			assert.Equal(t, "test@example.com", userMap["email"])
		} else {
			t.Fatal("Expected map[string]any")
		}
	})
}

// --------------------- Generic Error Handling Tests ---------------------

func TestGenerics_ErrorHandling(t *testing.T) {
	t.Run("decode into wrong type", func(t *testing.T) {
		// Encode a user
		user := GenericTestUser{ID: 123, Name: "Test User"}
		data, err := json.Marshal(user)
		require.NoError(t, err)

		// Try to decode into wrong type
		var order GenericTestOrder
		err = json.Unmarshal(data, &order)
		// This might not error but will have incorrect values
		assert.NoError(t, err) // JSON is flexible
		assert.Empty(t, order.OrderID) // Will be empty/zero value
	})

	t.Run("malformed JSON", func(t *testing.T) {
		malformedJSON := []byte(`{"id": 123, "name": "Test", "incomplete":`)

		var user GenericTestUser
		err := json.Unmarshal(malformedJSON, &user)
		assert.Error(t, err)
	})

	t.Run("incompatible types", func(t *testing.T) {
		// JSON with string where number expected
		jsonData := []byte(`{"id": "not-a-number", "name": "Test"}`)

		var user GenericTestUser
		err := json.Unmarshal(jsonData, &user)
		assert.Error(t, err)
	})

	t.Run("missing required fields", func(t *testing.T) {
		// JSON missing some fields
		jsonData := []byte(`{"name": "Test User"}`)

		var user GenericTestUser
		err := json.Unmarshal(jsonData, &user)
		require.NoError(t, err) // JSON unmarshaling is permissive

		// Missing fields get zero values
		assert.Equal(t, 0, user.ID)
		assert.Equal(t, "Test User", user.Name)
		assert.Empty(t, user.Email)
		assert.False(t, user.Active)
	})
}

// --------------------- Performance Benchmarks ---------------------

func BenchmarkGenerics_EncodeUser(b *testing.B) {
	user := GenericTestUser{
		ID:     123,
		Name:   "Benchmark User",
		Email:  "benchmark@test.com",
		Active: true,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(user)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGenerics_DecodeUser(b *testing.B) {
	user := GenericTestUser{
		ID:     123,
		Name:   "Benchmark User",
		Email:  "benchmark@test.com",
		Active: true,
	}
	data, _ := json.Marshal(user)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var decoded GenericTestUser
		err := json.Unmarshal(data, &decoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGenerics_EncodeSlice(b *testing.B) {
	users := make([]GenericTestUser, 100)
	for i := range users {
		users[i] = GenericTestUser{
			ID:     i,
			Name:   "User" + string(rune(i)),
			Email:  "user" + string(rune(i)) + "@test.com",
			Active: i%2 == 0,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(users)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGenerics_DecodeSlice(b *testing.B) {
	users := make([]GenericTestUser, 100)
	for i := range users {
		users[i] = GenericTestUser{
			ID:     i,
			Name:   "User" + string(rune(i)),
			Email:  "user" + string(rune(i)) + "@test.com",
			Active: i%2 == 0,
		}
	}
	data, _ := json.Marshal(users)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var decoded []GenericTestUser
		err := json.Unmarshal(data, &decoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGenerics_EncodeComplex(b *testing.B) {
	complex := GenericTestComplex{
		Header: GenericTestHeader{
			Version:   "v1.0",
			Timestamp: time.Now(),
			Source:    "benchmark",
		},
		Payload: map[string]any{
			"type":  "benchmark",
			"value": 42,
			"data":  []string{"a", "b", "c"},
		},
		Metadata: map[string]string{
			"env":    "test",
			"region": "us-west-2",
		},
		Items: []GenericTestItem{
			{ID: "item1", Value: 100},
			{ID: "item2", Value: 200},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(complex)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGenerics_DecodeComplex(b *testing.B) {
	complex := GenericTestComplex{
		Header: GenericTestHeader{
			Version:   "v1.0",
			Timestamp: time.Now(),
			Source:    "benchmark",
		},
		Payload: map[string]any{
			"type":  "benchmark",
			"value": 42,
			"data":  []string{"a", "b", "c"},
		},
		Metadata: map[string]string{
			"env":    "test",
			"region": "us-west-2",
		},
		Items: []GenericTestItem{
			{ID: "item1", Value: 100},
			{ID: "item2", Value: 200},
		},
	}
	data, _ := json.Marshal(complex)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var decoded GenericTestComplex
		err := json.Unmarshal(data, &decoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}
