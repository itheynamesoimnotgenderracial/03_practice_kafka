package api

import (
	"crypto/rand"
	"fmt"
	"log"
	"net/http"
	"simple-semo-eos/internal/kafka"
	"simple-semo-eos/internal/models"
	"simple-semo-eos/internal/serde"

	"github.com/gin-gonic/gin"
)

func newUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

// CustomerRequest is the shape the frontend POSTs.
// Gin validates binding tags automatically and returns 400 on failure
type CustomerRequest struct {
	MessageID  string `json:"message_id"`
	CustomerID string `json:"customer_id" binding:"required"`
	Name       string `json:"name" binding:"required"`
	Email      string `json:"email" binding:"required,email"`
	Action     string `json:"action" binding:"required,oneof=CREATE UPDATE"`
}

// QueuedResponse is returned on 202 Accepted
type QueuedResponse struct {
	MessageID string `json:"message_id"`
	Status    string `json:"status"`
}

// ErrorResponse is the standard error envelope.
type ErrorResponse struct {
	Error string `json:"error"`
}

// NewRouter wires all routes onto a Gin engine.
// producer and codec are injected so tests can swap in mocks without a real broker or Schema Registry.
func NewRouter(producer kafka.Producer, codec serde.Codec, validationTopic string) *gin.Engine {
	r := gin.New()
	r.HandleMethodNotAllowed = true
	r.Use(gin.Logger(), gin.Recovery())
	r.POST("/api/customer", handleCustomer(producer, codec, validationTopic))
	return r
}

func handleCustomer(producer kafka.Producer, codec serde.Codec, validationTopic string) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req CustomerRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, ErrorResponse{Error: err.Error()})
			return
		}

		if req.MessageID == "" {
			req.MessageID = newUUID()
		}

		msg := models.CustomerMessage{
			MessageID:  req.MessageID,
			CustomerID: req.CustomerID,
			Name:       req.Name,
			Email:      req.Email,
			Action:     req.Action,
		}

		// Serialize as Confluent Avro wire format for the validation topic
		subject := validationTopic + "-value"
		payload, err := codec.Serialize(subject, msg)
		if err != nil {
			log.Printf("[API] x serialize failed | message_id=%s err=%v", req.MessageID, err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "serialization error: " + err.Error()})
			return
		}

		// Use customer_id as the Kafka message key - guarantees ordering per
		// customer across all partitions.
		if err := producer.Publish(validationTopic, req.CustomerID, payload); err != nil {
			log.Printf("[API] x publish failed | message_id=%s err=%v", req.MessageID, err)
			c.JSON(http.StatusInternalServerError, ErrorResponse{Error: "publish error: " + err.Error()})
			return
		}

		log.Printf("[API] ✓ queued | message_id=%s customer_id=%s action=%s", req.MessageID, req.CustomerID, req.Action)

		c.JSON(http.StatusAccepted, QueuedResponse{
			MessageID: req.MessageID,
			Status:    "queued",
		})
	}
}
