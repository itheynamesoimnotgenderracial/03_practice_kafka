package api_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"simple-semo-eos/internal/api"
	"simple-semo-eos/internal/kafka"
	"simple-semo-eos/internal/serde"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func newRouter() (*gin.Engine, *kafka.MockProducer) {
	producer := kafka.NewMockProducer()
	serde := serde.NewMockCodec()
	return api.NewRouter(producer, serde, "simple.eos.validator"), producer
}

func post(router http.Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/customer", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	return rr
}

// ── 202 happy path ────────────────────────────────────────────────────────────

func TestHandleCustomer_ValidPayload_Returns202(t *testing.T) {
	router, _ := newRouter()
	rr := post(router, `{"customer_id":"c1","name":"Juan","email":"juan@example.com","action":"CREATE"}`)
	assert.Equal(t, http.StatusAccepted, rr.Code)
}

func TestHandleCustomer_ValidPayload_ResponseHasMessageIDAndQueuedStatus(t *testing.T) {
	router, _ := newRouter()
	rr := post(router, `{"customer_id":"c1","name":"Juan","email":"juan@example.com","action":"CREATE"}`)

	var resp api.QueuedResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.NotEmpty(t, resp.MessageID)
	assert.Equal(t, "queued", resp.Status)
}

func TestHandleCustomer_ValidPayload_PublishesToValidationTopic(t *testing.T) {
	router, producer := newRouter()
	post(router, `{"customer_id":"c1","name":"Juan","email":"juan@example.com","action":"CREATE"}`)
	assert.Len(t, producer.PublishedTo("simple.eos.validator"), 1)
}

func TestHandleCustomer_MessageKey_IsCustomerID(t *testing.T) {
	// customer_id as Kafka key guarantees ordering per customer per partition
	router, producer := newRouter()
	post(router, `{"customer_id":"key-test","name":"Juan","email":"j@e.com","action":"CREATE"}`)

	msgs := producer.PublishedTo("simple.eos.validator")
	require.Len(t, msgs, 1)
	assert.Equal(t, "key-test", msgs[0].Key)
}

func TestHandleCustomer_PublishedPayload_ContainsAllMessageFields(t *testing.T) {
	router, producer := newRouter()
	post(router, `{"customer_id":"c1","name":"Juan dela Cruz","email":"juan@example.com","action":"UPDATE"}`)

	msgs := producer.PublishedTo("simple.eos.validator")
	require.Len(t, msgs, 1)

	var payload map[string]string
	require.NoError(t, json.Unmarshal(msgs[0].Value, &payload))
	assert.Equal(t, "c1", payload["customer_id"])
	assert.Equal(t, "Juan dela Cruz", payload["name"])
	assert.Equal(t, "juan@example.com", payload["email"])
	assert.Equal(t, "UPDATE", payload["action"])
	assert.NotEmpty(t, payload["message_id"])
}

// ── Client-supplied message_id ────────────────────────────────────────────────

func TestHandleCustomer_ClientSuppliedMessageID_IsPreserved(t *testing.T) {
	router, producer := newRouter()
	post(router, `{"message_id":"my-id","customer_id":"c1","name":"Juan","email":"j@e.com","action":"CREATE"}`)

	rr := post(router, `{"message_id":"fixed-id","customer_id":"c1","name":"Juan","email":"j@e.com","action":"CREATE"}`)
	var resp api.QueuedResponse
	json.NewDecoder(rr.Body).Decode(&resp)
	assert.Equal(t, "fixed-id", resp.MessageID)

	msgs := producer.PublishedTo("simple.eos.validator")
	var payload map[string]string
	json.Unmarshal(msgs[len(msgs)-1].Value, &payload)
	assert.Equal(t, "fixed-id", payload["message_id"])
}

func TestHandleCustomer_TwoRequests_GetUniqueMessageIDs(t *testing.T) {
	router, _ := newRouter()
	body := `{"customer_id":"c1","name":"Juan","email":"j@e.com","action":"CREATE"}`

	var ids [2]string
	for i := range ids {
		rr := post(router, body)
		var resp api.QueuedResponse
		json.NewDecoder(rr.Body).Decode(&resp)
		ids[i] = resp.MessageID
	}
	assert.NotEqual(t, ids[0], ids[1], "each request must get a unique message_id")
}

// ── Gin binding validation → 400 ─────────────────────────────────────────────

func TestHandleCustomer_MissingRequiredFields_Returns400(t *testing.T) {
	cases := []struct {
		desc string
		body string
	}{
		{"missing customer_id", `{"name":"Juan","email":"j@e.com","action":"CREATE"}`},
		{"missing name", `{"customer_id":"c1","email":"j@e.com","action":"CREATE"}`},
		{"missing email", `{"customer_id":"c1","name":"Juan","action":"CREATE"}`},
		{"missing action", `{"customer_id":"c1","name":"Juan","email":"j@e.com"}`},
	}
	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			router, producer := newRouter()
			rr := post(router, tc.body)
			assert.Equal(t, http.StatusBadRequest, rr.Code)
			assert.Empty(t, producer.Published(), "no Kafka publish on validation failure")
		})
	}
}

func TestHandleCustomer_InvalidEmail_Returns400(t *testing.T) {
	router, _ := newRouter()
	rr := post(router, `{"customer_id":"c1","name":"Juan","email":"not-email","action":"CREATE"}`)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestHandleCustomer_InvalidAction_Returns400(t *testing.T) {
	router, _ := newRouter()
	rr := post(router, `{"customer_id":"c1","name":"Juan","email":"j@e.com","action":"DELETE"}`)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestHandleCustomer_MalformedJSON_Returns400(t *testing.T) {
	router, _ := newRouter()
	rr := post(router, `{not valid`)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestHandleCustomer_EmptyBody_Returns400(t *testing.T) {
	router, _ := newRouter()
	rr := post(router, ``)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

// ── HTTP method enforcement → 405 ────────────────────────────────────────────

func TestHandleCustomer_WrongMethods_Return405(t *testing.T) {
	router, _ := newRouter()
	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		req := httptest.NewRequest(method, "/api/customer", nil)
		rr := httptest.NewRecorder()
		router.ServeHTTP(rr, req)
		assert.Equal(t, http.StatusMethodNotAllowed, rr.Code, "method %s", method)
	}
}

// ── Kafka broker failure → 500 ────────────────────────────────────────────────

func TestHandleCustomer_KafkaPublishFails_Returns500(t *testing.T) {
	producer := kafka.NewMockProducer()
	serde := serde.NewMockCodec()
	producer.FailOnTopic = "validation.topic"
	router := api.NewRouter(producer, serde, "validation.topic")

	rr := post(router, `{"customer_id":"c1","name":"Juan","email":"j@e.com","action":"CREATE"}`)
	assert.Equal(t, http.StatusInternalServerError, rr.Code)
}

// ── Response format ───────────────────────────────────────────────────────────

func TestHandleCustomer_ContentType_IsApplicationJSON(t *testing.T) {
	router, _ := newRouter()
	rr := post(router, `{"customer_id":"c1","name":"Juan","email":"j@e.com","action":"CREATE"}`)
	assert.Contains(t, rr.Header().Get("Content-Type"), "application/json")
}

func TestHandleCustomer_UpdateAction_Returns202(t *testing.T) {
	router, _ := newRouter()
	rr := post(router, `{"customer_id":"c1","name":"Juan","email":"j@e.com","action":"UPDATE"}`)
	assert.Equal(t, http.StatusAccepted, rr.Code)
}

func TestHandleCustomer_ErrorResponse_HasErrorField(t *testing.T) {
	router, _ := newRouter()
	rr := post(router, `{"customer_id":"c1","name":"Juan","email":"j@e.com","action":"INVALID"}`)
	require.Equal(t, http.StatusBadRequest, rr.Code)

	var errResp api.ErrorResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&errResp))
	assert.NotEmpty(t, errResp.Error)
}
