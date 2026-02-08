package mongo

import (
	"context"
	"events_analytics_platform/internal/models"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type AggregateRepoStore interface {
	Update(ctx context.Context, e models.NormalizedOrderEvent) error
	UserTotalOrder(ctx context.Context, userID string) (*models.UserTotalOrder, error)
}

type AggregateRepo struct {
	Coll          *mongo.Collection
	WindowAggrCol *mongo.Collection
}

func NewAggregateRepo(db *mongo.Database) AggregateRepoStore {
	return &AggregateRepo{
		Coll:          db.Collection("user_order_aggregates"),
		WindowAggrCol: db.Collection("user_order_window_aggregates"),
	}
}

func (r *AggregateRepo) Update(ctx context.Context, e models.NormalizedOrderEvent) error {
	_, err := r.Coll.UpdateOne(
		ctx,
		bson.M{"_id": e.UserID},
		bson.M{
			"$inc": bson.M{
				"total_orders": 1,
				"total_amount": e.Amount,
			},
		},
		options.Update().SetUpsert(true),
	)
	return err
}

func (r *AggregateRepo) UserTotalOrder(ctx context.Context, userID string) (*models.UserTotalOrder, error) {
	var result models.UserTotalOrder
	err := r.Coll.FindOne(ctx, bson.M{"_id": userID}).Decode(&result)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

func (r *AggregateRepo) UpdateWindow(
	ctx context.Context,
	event models.NormalizedOrderEvent,
	windowType string,
	windowStart time.Time,
) (*models.UserOrderWindowAggregate, error) {
	filter := bson.M{
		"user_id":      event.UserID,
		"window_type":  windowType,
		"window_start": windowStart,
	}

	update := bson.M{
		"$inc": bson.M{
			"total_orders": 1,
			"total_amount": event.Amount,
		},
		"$set": bson.M{
			"updated_at": time.Now().UTC(),
		},
		"$setOnInsert": bson.M{
			"user_id":      event.UserID,
			"window_type":  windowType,
			"window_start": windowStart,
		},
	}

	opts := options.
		FindOneAndUpdate().
		SetUpsert(true).
		SetReturnDocument(options.After)

	var agg models.UserOrderWindowAggregate
	err := r.WindowAggrCol.FindOneAndUpdate(
		ctx,
		filter,
		update,
		opts,
	).Decode(&agg)

	return &agg, err
}
