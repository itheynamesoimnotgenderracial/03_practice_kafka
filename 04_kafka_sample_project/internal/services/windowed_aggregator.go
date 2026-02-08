package services

import (
	"context"
	"events_analytics_platform/internal/kafka"
	"events_analytics_platform/internal/models"
	"events_analytics_platform/internal/mongo"
	"fmt"
)

type WindowedAggregator interface {
	Process(ctx context.Context, event models.NormalizedOrderEvent) error
}

type WindowedAggregatorStore struct {
	repo           *mongo.AggregateRepo
	hourlyProducer *kafka.Producer
	dailyProducer  *kafka.Producer
}

func NewWindowedAggregator(
	repo *mongo.AggregateRepo,
	hourly *kafka.Producer,
	dailyProducer *kafka.Producer,
) WindowedAggregator {
	return &WindowedAggregatorStore{repo, hourly, dailyProducer}
}

func (a *WindowedAggregatorStore) Process(ctx context.Context, event models.NormalizedOrderEvent) error {

	// Hourly
	hourStart := HourWindow(event.ProcessedAt)
	hourAgg, err := a.repo.UpdateWindow(ctx, event, "hourly", hourStart)
	if err != nil {
		return err
	}

	err = a.hourlyProducer.Publish(ctx, event.UserID, hourAgg)
	if err != nil {
		return fmt.Errorf("error encounter when publishing hourly producer:%w", err)
	}

	// Daily
	dayStart := DayWindow(event.ProcessedAt)
	dayAgg, err := a.repo.UpdateWindow(ctx, event, "daily", dayStart)
	if err != nil {
		return err
	}

	err = a.dailyProducer.Publish(ctx, event.UserID, dayAgg)
	if err != nil {
		return fmt.Errorf("error encounter when publishing daily producer:%w", err)
	}

	return nil
}
