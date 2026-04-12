package serde

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"

	goavro "github.com/hamba/avro/v2"
	"simple-semo-eos/internal/schemaregistry"
)

// Codec serializes and deserializes Kafka message payloads.
// AvroCodec uses the Confluent wire format + Schema Registry.
// JSONCodec is a drop-in for unit tests (no Schema Registry required).
type Codec interface {
	Serialize(subject string, v interface{}) ([]byte, error)
	Deserialize(data []byte, v interface{}) error
}

// ── AvroCodec ─────────────────────────────────────────────────────────────────

const confluentMagicByte = 0x00

type subjectMeta struct {
	schemaID int
	schema   goavro.Schema
}

// AvroCodec encodes messages using Avro binary + the Confluent wire format
// (magic byte + 4-byte schema ID prefix). Schemas must be pre-registered via
// Register before any Serialize or Deserialize calls are made.
type AvroCodec struct {
	registry schemaregistry.Registry
	mu       sync.RWMutex
	bySubj   map[string]*subjectMeta // subject -> meta
	byID     map[int]goavro.Schema   // schemaID -> parsed schema (for deserialize)
}

// NewAvroCodec creates an AvroCodec backed by the given Schema Registry client.
func NewAvroCodec(registry schemaregistry.Registry) *AvroCodec {
	return &AvroCodec{
		registry: registry,
		bySubj:   make(map[string]*subjectMeta),
		byID:     make(map[int]goavro.Schema),
	}
}

// Register registers avscJSON with the Schema Registry under subject and caches
// the schema ID + parsed schema locally. Call this once at startup per subject.
func (a *AvroCodec) Register(subject, avscJSON string) error {
	schema, err := goavro.Parse(avscJSON)
	if err != nil {
		return fmt.Errorf("parse avro schema for %s: %w", subject, err)
	}

	id, err := a.registry.EnsureSchema(subject, avscJSON)
	if err != nil {
		return fmt.Errorf("register schema for %s: %w", subject, err)
	}

	a.mu.Lock()
	a.bySubj[subject] = &subjectMeta{schemaID: id, schema: schema}
	a.byID[id] = schema
	a.mu.Unlock()
	return nil
}

// Serialize encodes v using the schema registered for subject and prepends the
// Confluent wire format header: [0x00][4-byte big-endian schema ID][avro bytes].
func (a *AvroCodec) Serialize(subject string, v interface{}) ([]byte, error) {
	a.mu.RLock()
	meta, ok := a.bySubj[subject]
	a.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("no schema registered for subject %q", subject)
	}

	avroBytes, err := goavro.Marshal(meta.schema, v)
	if err != nil {
		return nil, fmt.Errorf("avro marshal for %s: %w", subject, err)
	}

	// Confluent wire format: [magic][schemaID 4B BE][avro payload]
	out := make([]byte, 5+len(avroBytes))
	out[0] = confluentMagicByte
	binary.BigEndian.PutUint32(out[1:5], uint32(meta.schemaID))
	copy(out[5:], avroBytes)
	return out, nil
}

// Deserialize strips the Confluent wire format header, resolves the schema by
// schema ID, and decodes the Avro payload into v.
func (a *AvroCodec) Deserialize(data []byte, v interface{}) error {
	if len(data) < 5 {
		return fmt.Errorf("payload too short for Confluent wire format (%d bytes)", len(data))
	}
	if data[0] != confluentMagicByte {
		return fmt.Errorf("unexpected magic byte 0x%02x (expected 0x00)", data[0])
	}

	schemaID := int(binary.BigEndian.Uint32(data[1:5]))

	a.mu.RLock()
	schema, ok := a.byID[schemaID]
	a.mu.RUnlock()

	if !ok {
		// Not in local cache — fetch from registry and parse on the fly.
		avscJSON, err := a.registry.SchemaByID(schemaID)
		if err != nil {
			return fmt.Errorf("fetch schema id %d: %w", schemaID, err)
		}
		var parseErr error
		schema, parseErr = goavro.Parse(avscJSON)
		if parseErr != nil {
			return fmt.Errorf("parse schema id %d: %w", schemaID, parseErr)
		}
		a.mu.Lock()
		a.byID[schemaID] = schema
		a.mu.Unlock()
	}

	return goavro.Unmarshal(schema, data[5:], v)
}

// ── JSONCodec ─────────────────────────────────────────────────────────────────

// JSONCodec implements Codec using plain JSON. Intended for unit tests where no
// Schema Registry is available. The subject parameter is ignored.
type JSONCodec struct{}

func (JSONCodec) Serialize(_ string, v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (JSONCodec) Deserialize(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
