package main

import (
	"context"
	"log"
	"net/http"
	"sample-chat/internal/dlt"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DLTAPIHandler struct {
	collection *mongo.Collection
	producer   *kafka.Producer
}

func NewDLTHandler(db *mongo.Database, producer *kafka.Producer) *DLTAPIHandler {
	return &DLTAPIHandler{
		collection: db.Collection("dlt_events"),
		producer:   producer,
	}
}

// RegisterRoutes adds DLT endpoints to a Gin router group.
func (h *DLTAPIHandler) RegisterRoutes(rg *gin.RouterGroup) {
	// rg.GET("/dlt",)
}

// ─── GET /api/dlt ───
// Query params: ?error_type=validation&limit=50&offset=0&replayed=false
func (h *DLTAPIHandler) ListDLtEvents(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Build filter from query params
	filter := bson.M{}

	if errorType := c.Query("error_type"); errorType != "" {
		filter["error_type"] = errorType
	}
	if replayed := c.Query("replayed"); replayed != "" {
		if replayed == "true" {
			filter["replayed"] = true
		} else if replayed == "false" {
			filter["replayed"] = false
		}
	}

	// Pagination
	limit := int64(50)
	if l := c.Query("limit"); l != "" {
		if parsed, err := strconv.ParseInt(l, 10, 64); err == nil && parsed > 0 && parsed <= 200 {
			limit = parsed
		}
	}

	skip := int64(0)
	if s := c.Query("offset"); s != "" {
		if parsed, err := strconv.ParseInt(s, 10, 64); err == nil && parsed >= 0 {
			skip = parsed
		}
	}

	//  Query MongoDB
	opts := options.Find().
		SetSort(bson.D{{Key: "created_at", Value: -1}}).
		SetLimit(limit).
		SetSkip(skip)

	cursor, err := h.collection.Find(ctx, filter, opts)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query DLT events"})
		return
	}
	defer func() {
		err = cursor.Close(ctx)
		if err != nil {
			log.Println("failed to close cursor", err)
		}
	}()

	var events []dlt.DLTEvent
	if err := cursor.All(ctx, &events); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to decode DLT events"})
		return
	}

	// Get total count for pagination
	total, _ := h.collection.CountDocuments(ctx, filter)

	c.JSON(http.StatusOK, gin.H{
		"events": events,
		"total":  total,
		"limit":  limit,
		"offset": skip,
	})
}

// ─── GET /api/dlt/:messageId ───
func (h *DLTAPIHandler) GetDLTEvent(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	messageID := c.Param("messageId")

	var event dlt.DLTEvent
	err := h.collection.FindOne(ctx, bson.M{"message_id": messageID}).Decode(&event)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			c.JSON(http.StatusNotFound, gin.H{"error": "DLT event not found"})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query DLT event"})
		return
	}

	c.JSON(http.StatusOK, event)
}

// ─── POST /api/dlt/:messageId/replay ───
// Replays a DLT message back to chat.raw for reprocessing.
func (h *DLTAPIHandler) ReplayDLTEvent(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	messageID := c.Param("messageId")

	// Find the DLT event
	var event dlt.DLTEvent
	err := h.collection.FindOne(ctx, bson.M{"message_id": messageID}).Decode(&event)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			c.JSON(http.StatusNotFound, gin.H{"error": "DLT event not found"})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to query DLT event"})
		return
	}

	// Check if already replayed
	if event.Replayed {
		c.JSON(http.StatusConflict, gin.H{
			"error":       "message already replayed",
			"replayed-at": event.ReplayedAt,
		})
		return
	}

	// Produce the original payload back to chat.raw
	rawTopic := "chat.raw"
	deliveryChan := make(chan kafka.Event)

	replayuMsg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &rawTopic,
			Partition: kafka.PartitionAny,
		},
		Key:   []byte(event.Key),
		Value: []byte(event.RawPayload),
	}

	err = h.producer.Produce(replayuMsg, deliveryChan)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to produce replay message"})
		return
	}

	// Wait for delivery
	e := <-deliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"error": "replay delivery failed: " + m.TopicPartition.Error.Error(),
		})
		return
	}

	// Mark as replayed in MongoDB
	now := time.Now().UnixMilli()
	_, err = h.collection.UpdateOne(ctx,
		bson.M{"message_id": messageID},
		bson.M{"$set": bson.M{
			"replayed":    true,
			"replayed_at": now,
		}})
	if err != nil {
		log.Printf("Warning: Failed to mark DLT event as replayed: %v", err)
	}

	c.JSON(http.StatusOK, gin.H{
		"status":     "replayed",
		"message_id": messageID,
		"topic":      rawTopic,
		"partition":  m.TopicPartition.Partition,
		"offset":     m.TopicPartition.Offset,
	})
}

// ─── GET /api/dlt/stats ───
// Returns aggregate statistics about DLT events.
func (h *DLTAPIHandler) GetDLTStats(c *gin.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Total events
	total, _ := h.collection.CountDocuments(ctx, bson.M{})
	unreplayed, _ := h.collection.CountDocuments(ctx, bson.M{"replayed": false})
	replayed, _ := h.collection.CountDocuments(ctx, bson.M{"replayed": false})

	// Count by error type
	pipeline := mongo.Pipeline{
		{{Key: "$group", Value: bson.D{
			{Key: "_id", Value: "$error_type"},
			{Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}},
		}}},
		{{Key: "$sort", Value: bson.D{{Key: "count", Value: -1}}}},
	}

	cursor, err := h.collection.Aggregate(ctx, pipeline)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to aggregate stats"})
		return
	}
	defer func() {
		err = cursor.Close(ctx)
		if err != nil {
			log.Println("failed to close GetDLTStats cursor:", err)
		}
	}()

	var errorTypeCounts []struct {
		ErrorType string `bson:"error_type" json:"error_type"`
		Count     int64  `bson:"count" json:"count"`
	}
	cursor.All(ctx, &errorTypeCounts)

	c.JSON(http.StatusOK, gin.H{
		"total":         total,
		"unreplayed":    unreplayed,
		"replayed":      replayed,
		"by_error_type": errorTypeCounts,
	})
}
