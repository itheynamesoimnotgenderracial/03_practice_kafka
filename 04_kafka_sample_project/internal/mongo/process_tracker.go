package mongo

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type ProcessTrackerStore interface {
	IsProcessed(ctx context.Context, eventID string) (bool, error)
	MarkProcessed(ctx context.Context, eventID string) error
}

type ProcessTracker struct {
	coll *mongo.Collection
}

func NewProcessTracker(db *mongo.Database) ProcessTrackerStore {
	return &ProcessTracker{
		coll: db.Collection("processed_events"),
	}
}

func (p *ProcessTracker) IsProcessed(ctx context.Context, eventID string) (bool, error) {
	count, err := p.coll.CountDocuments(ctx, bson.M{
		"_id":         eventID,
		"prcessed_at": time.Now().UTC(),
	})

	return count > 0, err
}

func (p *ProcessTracker) MarkProcessed(ctx context.Context, eventID string) error {
	_, err := p.coll.InsertOne(ctx, bson.M{
		"_id":          eventID,
		"processed_at": time.Now().UTC(),
	})
	return err
}
