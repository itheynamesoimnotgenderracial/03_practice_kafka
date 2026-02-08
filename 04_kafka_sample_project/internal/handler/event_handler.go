package handler

import (
	"context"
	"events_analytics_platform/internal/models"
	"events_analytics_platform/internal/services"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.mongodb.org/mongo-driver/mongo"
)

type EventHandler struct {
	service *services.EventService
}

func NewEventHandler(service *services.EventService) *EventHandler {
	return &EventHandler{service: service}
}

func (h *EventHandler) PostEvent(c *gin.Context) {
	var event models.Event

	if err := c.ShouldBindJSON(&event); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// ✅ Get user_id from header
	userID := c.GetHeader("user_id_from_token")
	if userID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id missing in header"})
		return
	}
	event.UserID = userID

	if err := h.service.IngestEvent(c.Request.Context(), &event); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{
		"status":  "queued",
		"eventId": event.EventID,
	})
}

func HandleMessage(ctx context.Context, event *models.AuditEvent, normalizedEvent *models.NormalizedOrderEvent, collection *mongo.Collection) error {
	event.ReceivedAt = time.Now().UTC()
	*normalizedEvent = models.ConvertModelToNormalizedOrder(event)

	_, err := collection.InsertOne(ctx, normalizedEvent)
	return err
}
