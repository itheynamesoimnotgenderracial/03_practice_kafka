package handlers

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sample-chat/cmd/api/repository"
	pkgmodels "sample-chat/cmd/pkg-models"

	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	baseKafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type MessageHandler struct {
	repo       *repository.MessageRepository
	producer   *baseKafka.Producer
	serializer *avro.GenericSerializer
	ctx        context.Context
}

func NewMessageHandler(repo *repository.MessageRepository, producerCfg *baseKafka.Producer, avroSerializer *avro.GenericSerializer, ctx context.Context) *MessageHandler {
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

	userID := c.GetHeader("user_id")
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
	deliveryChan := make(chan baseKafka.Event)

	if err := h.producer.Produce(&baseKafka.Message{
		TopicPartition: baseKafka.TopicPartition{
			Topic:     &topic,
			Partition: baseKafka.PartitionAny,
		},
		Key:   []byte(req.RoomID),
		Value: serialzed,
	}, deliveryChan); err != nil {
		errMessage := fmt.Sprintf("failed to produce a raw message transaction: %s", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": errMessage})
		return
	}

	select {
	case e := <-deliveryChan:
		m := e.(*kafka.Message)
		if m.TopicPartition.Error != nil {
			log.Println("delivery failed:", m.TopicPartition.Error)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "message delivery failed"})
			return
		}
		log.Printf("message delivered to %s[%d]@%d\n",
			*m.TopicPartition.Topic,
			m.TopicPartition.Partition,
			m.TopicPartition.Offset,
		)
	case <-c.Done():
		log.Println("delivery timeout")
		c.JSON(http.StatusGatewayTimeout, gin.H{"error": "message delivery timeout"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"status":     "accepted",
		"message_id": event.MessageID,
	})
}
