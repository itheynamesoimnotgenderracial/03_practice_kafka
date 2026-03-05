package handlers

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sample-chat/cmd/api/repository"
	pkgmodels "sample-chat/cmd/pkg-models"
	"sample-chat/internal/kafka"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type MessageHandler struct {
	repo       *repository.MessageRepository
	producer   kafka.TxProducerStore
	serializer *avro.GenericSerializer
	ctx        context.Context
}

func NewMessageHandler(repo *repository.MessageRepository, producerCfg kafka.TxProducerStore, avroSerializer *avro.GenericSerializer, ctx context.Context) *MessageHandler {
	return &MessageHandler{
		repo:       repo,
		producer:   producerCfg,
		serializer: avroSerializer,
		ctx:        ctx,
	}
}

func (h *MessageHandler) GetMessages(c *gin.Context) {
	roomID := c.Query("roomId")
	if roomID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "roomId required"})
		return
	}

	limitStr := c.DefaultQuery("limit", "30")
	limit, err := strconv.ParseInt(limitStr, 10, 64)
	if err != nil || limit <= 0 {
		limit = 30
	}

	beforeStr := c.Query("before")
	var before *int64

	if beforeStr != "" {
		val, err := strconv.ParseInt(beforeStr, 10, 64)
		if err == nil {
			before = &val
		}
	}

	messages, err := h.repo.GetMessages(
		c.Request.Context(),
		roomID,
		before,
		limit,
	)

	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "failed to fetch messages",
		})
		return
	}

	c.JSON(http.StatusOK, messages)
}

func (h *MessageHandler) SendMessage(c *gin.Context) {
	var req struct {
		RoomID  string `json:"room_id"`
		Content string `json:"content"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}

	userID := c.GetString("user_id")

	event := pkgmodels.ChatRawEvent{
		MessageID: uuid.New().String(),
		RoomID:    req.RoomID,
		UserID:    userID,
		Content:   req.Content,
		Timestamp: time.Now().UnixMilli(),
	}

	serialzed, err := h.serializer.Serialize("chat.raw-value", &event)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "cannot serialize raw data type"})
		return
	}

	topic := "chat.raw"

	if err := h.producer.Begin(); err != nil {
		log.Println("handler msg error:", err)
		h.producer.Abort(h.ctx)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to begin send message transaction"})
		return
	}

	if err := h.producer.Produce(topic, []byte(req.RoomID), serialzed); err != nil {
		log.Println("output produce error:", err)
		h.producer.Abort(h.ctx)
		errMessage := fmt.Sprintf("failed to produce a raw message transaction: %s", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": errMessage})
		return
	}

	c.JSON(202, gin.H{
		"status": "accepted",
	})
}
