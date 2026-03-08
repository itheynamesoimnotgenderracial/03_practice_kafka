package mongo

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type SequenceStore struct {
	collection *mongo.Collection
}

func NewSequenceStore(db *mongo.Database, collectionName string) *SequenceStore {
	coll := db.Collection(collectionName)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "room_id", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	if err != nil {
		log.Println("index creation skipped (may already exist):", err)
	}

	return &SequenceStore{collection: coll}
}

func (seqStore SequenceStore) NextSequence(ctx context.Context, roomID string) (int64, error) {
	filter := bson.M{"room_id": roomID}
	update := bson.M{"$inc": bson.M{"sequence": int64(1)}}
	opts := options.FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(options.After)

	var result struct {
		RoomID   string `bson:"room_id"`
		Sequence int64  `bson:"sequence"`
	}

	err := seqStore.collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&result)
	if err != nil {
		return 0, fmt.Errorf("atomic sequence increment for room %s: %w", roomID, err)
	}

	return result.Sequence, nil
}
