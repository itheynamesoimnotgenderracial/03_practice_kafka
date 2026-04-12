package models

type CustomerMessage struct {
	MessageID  string `json:"message_id"  avro:"message_id"`
	CustomerID string `json:"customer_id" avro:"customer_id"`
	Name       string `json:"name"        avro:"name"`
	Email      string `json:"email"       avro:"email"`
	// Action is an Avro enum on the wire (CREATE | UPDATE); maps to string in Go.
	Action string `json:"action" avro:"action"`
}

type DLQMessage struct {
	OriginalPayload CustomerMessage `json:"original_payload"`
	FailedAtStep    string          `json:"failed_at_step"`
	Reason          string          `json:"reason"`
	IdempotencyKey  string          `json:"idempotency_key"`
	RetryCount      int             `json:"retry_count"`
}

type ExternalAPIResponse struct {
	ExternalID string `json:"external_id"`
	Message    string `json:"message"`
	Valid       bool   `json:"valid"`
}
