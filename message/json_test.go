package message

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test types for JSON edge case testing
type JSONTestStruct struct {
	String   string         `json:"string"`
	Number   float64        `json:"number"`
	Integer  int            `json:"integer"`
	Boolean  bool           `json:"boolean"`
	Null     *string        `json:"null"`
	Array    []any          `json:"array"`
	Object   map[string]any `json:"object"`
	Optional *int           `json:"optional,omitempty"`
}

type JSONTestUnicode struct {
	English string `json:"english"`
	Chinese string `json:"chinese"`
	Arabic  string `json:"arabic"`
	Emoji   string `json:"emoji"`
	Mixed   string `json:"mixed"`
}

type JSONTestNumbers struct {
	Zero        float64 `json:"zero"`
	Positive    float64 `json:"positive"`
	Negative    float64 `json:"negative"`
	Large       float64 `json:"large"`
	Small       float64 `json:"small"`
	Infinity    float64 `json:"infinity"`
	NegInfinity float64 `json:"neg_infinity"`
	NaN         float64 `json:"nan"`
}

// --------------------- JSON Basic Edge Cases ---------------------

func TestJSON_BasicEdgeCases(t *testing.T) {
	t.Run("empty JSON object", func(t *testing.T) {
		jsonData := []byte(`{}`)

		var result map[string]any
		err := json.Unmarshal(jsonData, &result)
		require.NoError(t, err)
		assert.Empty(t, result)
		assert.NotNil(t, result)
	})

	t.Run("empty JSON array", func(t *testing.T) {
		jsonData := []byte(`[]`)

		var result []any
		err := json.Unmarshal(jsonData, &result)
		require.NoError(t, err)
		assert.Empty(t, result)
		assert.NotNil(t, result)
	})

	t.Run("null values", func(t *testing.T) {
		jsonData := []byte(`{"null_value": null, "string": "test"}`)

		var result map[string]*string
		err := json.Unmarshal(jsonData, &result)
		require.NoError(t, err)
		assert.Nil(t, result["null_value"])
		assert.NotNil(t, result["string"])
		assert.Equal(t, "test", *result["string"])
	})

	t.Run("nested empty structures", func(t *testing.T) {
		jsonData := []byte(`{"empty_object": {}, "empty_array": [], "nested": {"deep": {}}}`)

		var result map[string]any
		err := json.Unmarshal(jsonData, &result)
		require.NoError(t, err)

		emptyObj := result["empty_object"].(map[string]any)
		assert.Empty(t, emptyObj)

		emptyArr := result["empty_array"].([]any)
		assert.Empty(t, emptyArr)

		nested := result["nested"].(map[string]any)
		deep := nested["deep"].(map[string]any)
		assert.Empty(t, deep)
	})
}

// --------------------- JSON String Edge Cases ---------------------

func TestJSON_StringEdgeCases(t *testing.T) {
	t.Run("empty string", func(t *testing.T) {
		data := map[string]string{"empty": ""}

		jsonData, err := json.Marshal(data)
		require.NoError(t, err)

		var decoded map[string]string
		err = json.Unmarshal(jsonData, &decoded)
		require.NoError(t, err)
		assert.Equal(t, "", decoded["empty"])
	})

	t.Run("escaped characters", func(t *testing.T) {
		data := map[string]string{
			"quotes":    `"quoted text"`,
			"backslash": `path\to\file`,
			"newline":   "line1\nline2",
			"tab":       "col1\tcol2",
			"unicode":   "\u0048\u0065\u006C\u006C\u006F", // "Hello"
		}

		jsonData, err := json.Marshal(data)
		require.NoError(t, err)

		var decoded map[string]string
		err = json.Unmarshal(jsonData, &decoded)
		require.NoError(t, err)

		assert.Equal(t, `"quoted text"`, decoded["quotes"])
		assert.Equal(t, `path\to\file`, decoded["backslash"])
		assert.Equal(t, "line1\nline2", decoded["newline"])
		assert.Equal(t, "col1\tcol2", decoded["tab"])
		assert.Equal(t, "Hello", decoded["unicode"])
	})

	t.Run("unicode and emoji", func(t *testing.T) {
		unicode := JSONTestUnicode{
			English: "Hello World",
			Chinese: "你好世界",
			Arabic:  "مرحبا بالعالم",
			Emoji:   "🌍🚀💻🎉",
			Mixed:   "Hello 🌍 世界 💻",
		}

		jsonData, err := json.Marshal(unicode)
		require.NoError(t, err)

		var decoded JSONTestUnicode
		err = json.Unmarshal(jsonData, &decoded)
		require.NoError(t, err)

		assert.Equal(t, unicode.English, decoded.English)
		assert.Equal(t, unicode.Chinese, decoded.Chinese)
		assert.Equal(t, unicode.Arabic, decoded.Arabic)
		assert.Equal(t, unicode.Emoji, decoded.Emoji)
		assert.Equal(t, unicode.Mixed, decoded.Mixed)
	})

	t.Run("very long string", func(t *testing.T) {
		// Create a 10MB string
		longString := strings.Repeat("A", 10*1024*1024)
		data := map[string]string{"long": longString}

		jsonData, err := json.Marshal(data)
		require.NoError(t, err)

		var decoded map[string]string
		err = json.Unmarshal(jsonData, &decoded)
		require.NoError(t, err)
		assert.Equal(t, longString, decoded["long"])
		assert.Equal(t, len(longString), len(decoded["long"]))
	})

	t.Run("special characters", func(t *testing.T) {
		specialChars := map[string]string{
			"control":   "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09",
			"printable": "!@#$%^&*()_+-={}[]|\\:;\"'<>?,./ ",
			"mixed":     "Normal text with \x00 control and 🎉 emoji",
		}

		jsonData, err := json.Marshal(specialChars)
		require.NoError(t, err)

		var decoded map[string]string
		err = json.Unmarshal(jsonData, &decoded)
		require.NoError(t, err)

		for key, expected := range specialChars {
			assert.Equal(t, expected, decoded[key])
		}
	})
}

// --------------------- JSON Number Edge Cases ---------------------

func TestJSON_NumberEdgeCases(t *testing.T) {
	t.Run("special float values", func(t *testing.T) {
		numbers := JSONTestNumbers{
			Zero:        0.0,
			Positive:    123.456,
			Negative:    -789.012,
			Large:       1.7976931348623157e+308, // Near max float64
			Small:       4.9406564584124654e-324, // Near min positive float64
			Infinity:    math.Inf(1),
			NegInfinity: math.Inf(-1),
			NaN:         math.NaN(),
		}

		// JSON doesn't support Infinity or NaN, so Marshal should fail
		_, err := json.Marshal(numbers)
		assert.Error(t, err) // Should fail due to Infinity/NaN

		// Test with valid numbers only
		validNumbers := JSONTestNumbers{
			Zero:     0.0,
			Positive: 123.456,
			Negative: -789.012,
			Large:    1.7976931348623157e+308,
			Small:    4.9406564584124654e-324,
		}

		jsonData, err := json.Marshal(validNumbers)
		require.NoError(t, err)

		var decoded JSONTestNumbers
		err = json.Unmarshal(jsonData, &decoded)
		require.NoError(t, err)

		assert.Equal(t, 0.0, decoded.Zero)
		assert.Equal(t, 123.456, decoded.Positive)
		assert.Equal(t, -789.012, decoded.Negative)
		assert.Equal(t, validNumbers.Large, decoded.Large)
		assert.Equal(t, validNumbers.Small, decoded.Small)
		// The special values should be zero (default value)
		assert.Equal(t, 0.0, decoded.Infinity)
		assert.Equal(t, 0.0, decoded.NegInfinity)
		assert.Equal(t, 0.0, decoded.NaN)
	})

	t.Run("integer precision", func(t *testing.T) {
		testCases := []struct {
			name   string
			number int64
		}{
			{"small positive", 42},
			{"small negative", -42},
			{"large positive", 9223372036854775807},  // max int64
			{"large negative", -9223372036854775808}, // min int64
			{"javascript safe", 9007199254740991},    // Number.MAX_SAFE_INTEGER
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				data := map[string]int64{"number": tc.number}

				jsonData, err := json.Marshal(data)
				require.NoError(t, err)

				var decoded map[string]int64
				err = json.Unmarshal(jsonData, &decoded)
				require.NoError(t, err)
				assert.Equal(t, tc.number, decoded["number"])
			})
		}
	})

	t.Run("float precision", func(t *testing.T) {
		testCases := []float64{
			0.1 + 0.2,          // Classic floating point precision issue
			1.0 / 3.0,          // Repeating decimal
			math.Pi,            // Irrational number
			math.E,             // Euler's number
			1.2345678901234567, // Many decimal places
		}

		for i, number := range testCases {
			t.Run("precision_test_"+strconv.Itoa(i), func(t *testing.T) {
				data := map[string]float64{"number": number}

				jsonData, err := json.Marshal(data)
				require.NoError(t, err)

				var decoded map[string]float64
				err = json.Unmarshal(jsonData, &decoded)
				require.NoError(t, err)

				// Use approximate equality for floating point comparison
				assert.InDelta(t, number, decoded["number"], 1e-15)
			})
		}
	})
}

// --------------------- JSON Array Edge Cases ---------------------

func TestJSON_ArrayEdgeCases(t *testing.T) {
	t.Run("mixed type array", func(t *testing.T) {
		mixedArray := []any{
			"string",
			42,
			3.14159,
			true,
			nil,
			[]any{"nested", "array"},
			map[string]any{"nested": "object"},
		}

		jsonData, err := json.Marshal(mixedArray)
		require.NoError(t, err)

		var decoded []any
		err = json.Unmarshal(jsonData, &decoded)
		require.NoError(t, err)

		assert.Len(t, decoded, 7)
		assert.Equal(t, "string", decoded[0])
		assert.Equal(t, float64(42), decoded[1]) // JSON numbers become float64
		assert.Equal(t, 3.14159, decoded[2])
		assert.Equal(t, true, decoded[3])
		assert.Nil(t, decoded[4])

		nestedArray := decoded[5].([]any)
		assert.Equal(t, "nested", nestedArray[0])
		assert.Equal(t, "array", nestedArray[1])

		nestedObject := decoded[6].(map[string]any)
		assert.Equal(t, "object", nestedObject["nested"])
	})

	t.Run("deeply nested array", func(t *testing.T) {
		// Create deeply nested array: [[[[[0]]]]]
		deeply := any(0)
		for range 5 {
			deeply = []any{deeply}
		}

		jsonData, err := json.Marshal(deeply)
		require.NoError(t, err)

		var decoded any
		err = json.Unmarshal(jsonData, &decoded)
		require.NoError(t, err)

		// Navigate through the nested structure
		current := decoded
		for i := 0; i < 5; i++ {
			arr, ok := current.([]any)
			require.True(t, ok)
			require.Len(t, arr, 1)
			current = arr[0]
		}
		assert.Equal(t, float64(0), current)
	})

	t.Run("sparse array simulation", func(t *testing.T) {
		// JSON doesn't have sparse arrays, but we can simulate with nulls
		sparseArray := []any{
			"first",
			nil,
			nil,
			"fourth",
			nil,
			"sixth",
		}

		jsonData, err := json.Marshal(sparseArray)
		require.NoError(t, err)

		var decoded []any
		err = json.Unmarshal(jsonData, &decoded)
		require.NoError(t, err)

		assert.Len(t, decoded, 6)
		assert.Equal(t, "first", decoded[0])
		assert.Nil(t, decoded[1])
		assert.Nil(t, decoded[2])
		assert.Equal(t, "fourth", decoded[3])
		assert.Nil(t, decoded[4])
		assert.Equal(t, "sixth", decoded[5])
	})

	t.Run("very large array", func(t *testing.T) {
		// Create array with 10,000 elements
		largeArray := make([]int, 10000)
		for i := range largeArray {
			largeArray[i] = i
		}

		start := time.Now()
		jsonData, err := json.Marshal(largeArray)
		encodeTime := time.Since(start)
		require.NoError(t, err)

		start = time.Now()
		var decoded []int
		err = json.Unmarshal(jsonData, &decoded)
		decodeTime := time.Since(start)
		require.NoError(t, err)

		assert.Len(t, decoded, 10000)
		assert.Equal(t, largeArray, decoded)
		t.Logf("Large array - Encode: %v, Decode: %v", encodeTime, decodeTime)
	})
}

// --------------------- JSON Object Edge Cases ---------------------

func TestJSON_ObjectEdgeCases(t *testing.T) {
	t.Run("deeply nested object", func(t *testing.T) {
		// Create deeply nested object
		deeply := map[string]any{"value": "deep"}
		for i := 0; i < 5; i++ {
			deeply = map[string]any{"nested": deeply}
		}

		jsonData, err := json.Marshal(deeply)
		require.NoError(t, err)

		var decoded map[string]any
		err = json.Unmarshal(jsonData, &decoded)
		require.NoError(t, err)

		// Navigate through nested structure
		current := decoded
		for i := 0; i < 5; i++ {
			nested, ok := current["nested"]
			require.True(t, ok)
			current = nested.(map[string]any)
		}
		assert.Equal(t, "deep", current["value"])
	})

	t.Run("object with special keys", func(t *testing.T) {
		specialKeys := map[string]any{
			"":                "empty key",
			" ":               "space key",
			"\t":              "tab key",
			"\n":              "newline key",
			"unicode_🔑":       "unicode key",
			"key.with.dots":   "dotted key",
			"key with spaces": "spaced key",
			"123numeric":      "numeric start",
			"$pecial!@#":      "special chars",
		}

		jsonData, err := json.Marshal(specialKeys)
		require.NoError(t, err)

		var decoded map[string]any
		err = json.Unmarshal(jsonData, &decoded)
		require.NoError(t, err)

		for key, expectedValue := range specialKeys {
			assert.Equal(t, expectedValue, decoded[key])
		}
	})

	t.Run("object with duplicate keys handling", func(t *testing.T) {
		// JSON with duplicate keys - last one wins
		jsonWithDuplicates := `{"key": "first", "key": "second", "key": "third"}`

		var decoded map[string]any
		err := json.Unmarshal([]byte(jsonWithDuplicates), &decoded)
		require.NoError(t, err)

		// Last value should win
		assert.Equal(t, "third", decoded["key"])
	})

	t.Run("very large object", func(t *testing.T) {
		// Create object with many keys
		largeObject := make(map[string]int)
		for i := 0; i < 1000; i++ {
			largeObject["key_"+strconv.Itoa(i)] = i
		}

		start := time.Now()
		jsonData, err := json.Marshal(largeObject)
		encodeTime := time.Since(start)
		require.NoError(t, err)

		start = time.Now()
		var decoded map[string]int
		err = json.Unmarshal(jsonData, &decoded)
		decodeTime := time.Since(start)
		require.NoError(t, err)

		assert.Len(t, decoded, 1000)
		for key, expectedValue := range largeObject {
			assert.Equal(t, expectedValue, decoded[key])
		}
		t.Logf("Large object - Encode: %v, Decode: %v", encodeTime, decodeTime)
	})
}

// --------------------- JSON Malformed Input Tests ---------------------

func TestJSON_MalformedInput(t *testing.T) {
	malformedCases := []struct {
		name string
		json string
	}{
		{"unclosed object", `{"key": "value"`},
		{"unclosed array", `["item1", "item2"`},
		{"trailing comma object", `{"key": "value",}`},
		{"trailing comma array", `["item1", "item2",]`},
		{"unquoted key", `{key: "value"}`},
		{"single quotes", `{'key': 'value'}`},
		{"undefined value", `{"key": undefined}`},
		{"comment", `{"key": "value" /* comment */}`},
		{"extra closing", `{"key": "value"}}`},
		{"missing value", `{"key":}`},
		{"invalid escape", `{"key": "invalid\x"}`},
		{"incomplete unicode", `{"key": "\u00"}`},
	}

	for _, tc := range malformedCases {
		t.Run(tc.name, func(t *testing.T) {
			var result any
			err := json.Unmarshal([]byte(tc.json), &result)
			assert.Error(t, err)
		})
	}
}

// --------------------- JSON Performance Edge Cases ---------------------

func TestJSON_PerformanceEdgeCases(t *testing.T) {
	t.Run("deeply nested performance", func(t *testing.T) {
		// Create structure with 1000 levels of nesting
		const depth = 1000
		deeply := map[string]any{"value": "deep"}
		for i := 0; i < depth; i++ {
			deeply = map[string]any{"nested": deeply}
		}

		start := time.Now()
		jsonData, err := json.Marshal(deeply)
		encodeTime := time.Since(start)
		require.NoError(t, err)

		start = time.Now()
		var decoded map[string]any
		err = json.Unmarshal(jsonData, &decoded)
		decodeTime := time.Since(start)
		require.NoError(t, err)

		// Verify structure is correct
		current := decoded
		for i := 0; i < depth; i++ {
			current = current["nested"].(map[string]any)
		}
		assert.Equal(t, "deep", current["value"])

		t.Logf("Deep nesting (%d levels) - Encode: %v, Decode: %v", depth, encodeTime, decodeTime)
	})

	t.Run("wide object performance", func(t *testing.T) {
		// Create object with 10,000 keys
		const width = 10000
		wideObject := make(map[string]int, width)
		for i := 0; i < width; i++ {
			wideObject["key_"+strconv.Itoa(i)] = i
		}

		start := time.Now()
		jsonData, err := json.Marshal(wideObject)
		encodeTime := time.Since(start)
		require.NoError(t, err)

		start = time.Now()
		var decoded map[string]int
		err = json.Unmarshal(jsonData, &decoded)
		decodeTime := time.Since(start)
		require.NoError(t, err)

		assert.Len(t, decoded, width)
		t.Logf("Wide object (%d keys) - Encode: %v, Decode: %v", width, encodeTime, decodeTime)
	})
}

// --------------------- JSON Codec Integration Tests ---------------------

func TestJSON_CodecIntegration(t *testing.T) {
	codec := &jsonCodec{}

	t.Run("codec handles edge cases", func(t *testing.T) {
		testCases := []struct {
			name  string
			value any
		}{
			{"nil", nil},
			{"empty struct", struct{}{}},
			{"empty slice", []any{}},
			{"empty map", map[string]any{}},
			{"nested empty", map[string]any{
				"empty_array":  []any{},
				"empty_object": map[string]any{},
				"null_value":   nil,
			}},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Encode
				data, err := codec.Encode(tc.value)
				require.NoError(t, err)
				assert.True(t, json.Valid(data))

				// Decode
				var decoded any
				err = codec.Decode(data, &decoded)
				require.NoError(t, err)

				// For nil, decoded should also be nil
				if tc.value == nil {
					assert.Nil(t, decoded)
				} else {
					assert.NotNil(t, decoded)
				}
			})
		}
	})

	t.Run("codec content type", func(t *testing.T) {
		assert.Equal(t, "application/json", codec.ContentType())
	})

	t.Run("round trip fidelity", func(t *testing.T) {
		original := JSONTestStruct{
			String:  "test string",
			Number:  123.456,
			Integer: 789,
			Boolean: true,
			Null:    nil,
			Array:   []any{"item1", 42, true},
			Object:  map[string]any{"key": "value"},
		}

		// Encode
		data, err := codec.Encode(original)
		require.NoError(t, err)

		// Decode
		var decoded JSONTestStruct
		err = codec.Decode(data, &decoded)
		require.NoError(t, err)

		assert.Equal(t, original.String, decoded.String)
		assert.Equal(t, original.Number, decoded.Number)
		assert.Equal(t, original.Integer, decoded.Integer)
		assert.Equal(t, original.Boolean, decoded.Boolean)
		assert.Nil(t, decoded.Null)
		assert.Nil(t, decoded.Optional)
	})
}

// --------------------- Performance Benchmarks ---------------------

func BenchmarkJSON_EncodeSimple(b *testing.B) {
	data := map[string]any{
		"string":  "test",
		"number":  123.456,
		"boolean": true,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSON_DecodeSimple(b *testing.B) {
	jsonData := []byte(`{"string":"test","number":123.456,"boolean":true}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var result map[string]any
		err := json.Unmarshal(jsonData, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSON_EncodeComplex(b *testing.B) {
	complex := map[string]any{
		"array":  []any{1, 2, 3, "four", true},
		"object": map[string]any{"nested": "value"},
		"number": 123.456,
		"string": "test string with unicode 🚀",
		"null":   nil,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(complex)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSON_DecodeComplex(b *testing.B) {
	jsonData := []byte(`{"array":[1,2,3,"four",true],"object":{"nested":"value"},"number":123.456,"string":"test string with unicode 🚀","null":null}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var result map[string]any
		err := json.Unmarshal(jsonData, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSON_LargeArray(b *testing.B) {
	largeArray := make([]int, 1000)
	for i := range largeArray {
		largeArray[i] = i
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(largeArray)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSON_LargeObject(b *testing.B) {
	largeObject := make(map[string]int, 1000)
	for i := 0; i < 1000; i++ {
		largeObject["key_"+strconv.Itoa(i)] = i
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(largeObject)
		if err != nil {
			b.Fatal(err)
		}
	}
}
