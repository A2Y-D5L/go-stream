package message

import "encoding/json"

// Codec is a pluggable encoder/decoder (default JSON).
type Codec interface {
	Encode(v any) ([]byte, error)
	Decode(data []byte, v any) error
	ContentType() string
}

// jsonCodec implements Codec using the stdlib encoding/json.
type jsonCodec struct{}

func (jsonCodec) Encode(v any) ([]byte, error) { return json.Marshal(v) }
func (jsonCodec) Decode(b []byte, v any) error { return json.Unmarshal(b, v) }
func (jsonCodec) ContentType() string          { return "application/json" }

var JSONCodec Codec = jsonCodec{}
