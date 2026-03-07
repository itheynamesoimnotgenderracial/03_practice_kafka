package repository

import (
	"context"
	"fmt"
	pkgmodels "sample-chat/cmd/pkg-models"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MessageRepository struct {
	collection *mongo.Collection
}

func NewMessageRepository(db *mongo.Database) *MessageRepository {
	return &MessageRepository{
		collection: db.Collection("chat_messages"),
	}
}

func (r *MessageRepository) GetMessages(
	ctx context.Context,
	roomID string,
	before *int64,
	limit int64,
) ([]pkgmodels.ChatMessage, error) {
	filter := bson.M{
		"room_id": roomID,
	}

	if before != nil {
		filter["sequence"] = bson.M{"$lt": *before}
	}

	opts := options.Find().
		SetSort(bson.D{{Key: "sequence", Value: -1}}).
		SetLimit(limit)
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	cursor, err := r.collection.Find(ctx, filter, opts)
	defer func() {
		cancel()
		err = cursor.Close(ctx)
		if err != nil {
			fmt.Println("failed at closing cursor collection:", err)
		}
	}()

	if err != nil {
		return nil, err
	}

	messages := make([]pkgmodels.ChatMessage, len(cursor.Current))
	if err := cursor.All(ctx, &messages); err != nil {
		return nil, err
	}

	return messages, nil
}
