package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"simple-semo-eos/external"
	"simple-semo-eos/internal/api"
	"simple-semo-eos/internal/config"
	"simple-semo-eos/internal/consumer"
	"simple-semo-eos/internal/db"
	"simple-semo-eos/internal/kafka"
	"simple-semo-eos/internal/schemaregistry"
	"simple-semo-eos/internal/serde"
	"syscall"
	"time"
)

func main() {
	cfg := config.Load()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	store, err := db.NewPostgresStore(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("[Main] DB init: %v", err)
	}
	defer store.Close()
	log.Println("[Main] PostgreSQL connected")

	// ── Avro codec + Schema Registry ───────────────────────────────────────────
	srClient := schemaregistry.New(cfg.SchemaRegistryURL)
	codec := serde.NewAvroCodec(srClient)

	// Register all topic schemas at startup. If the schema is already present in
	// the registry the call is idempotent (returns the existing schema ID).
	schemas := []struct{ subject, avsc string }{
		{cfg.KafkaValidationTopic + "-value", serde.CustomerMessageAvsc},
		{cfg.KafkaConsumerTopic + "-value", serde.CustomerMessageAvsc},
		{cfg.KafkaDLQTopic + "-value", serde.DLQMessageAvsc},
	}
	for _, s := range schemas {
		if err := codec.Register(s.subject, s.avsc); err != nil {
			log.Fatalf("[Main] schema register %s: %v", s.subject, err)
		}
		log.Printf("[Main] ✓ schema registered | subject=%s", s.subject)
	}

	// ── Kafka producer (shared by API + consumers for DLQ) ─────────────────────
	producer, err := kafka.NewConfluentProducer(cfg.KafkaBrokers)
	if err != nil {
		log.Fatalf("[Main] producer: %v", err)
	}
	defer producer.Close()

	// ── Validator consumer ──────────────────────────────────────────────────────
	valConsumer, err := kafka.NewConfluentConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID+"-validator")
	if err != nil {
		log.Fatalf("[Main] validator consumer: %v", err)
	}

	go func() {
		if err := consumer.RunValidatorConsumer(ctx, consumer.ValidatorConfig{
			Consumer:        valConsumer,
			Producer:        producer,
			Serde:           codec,
			ValidationTopic: cfg.KafkaValidationTopic,
			CustomerTopic:   cfg.KafkaConsumerTopic,
			DLQTopic:        cfg.KafkaDLQTopic,
		}); err != nil {
			log.Printf("[Main] validator exited: %v", err)
		}
	}()

	// ── Customer consumer ───────────────────────────────────────────────────────
	custConsumer, err := kafka.NewConfluentConsumer(cfg.KafkaBrokers, cfg.KafkaGroupID+"-customer")
	if err != nil {
		log.Fatalf("[Main] customer consumer: %v", err)
	}
	go func() {
		if err := consumer.RunCustomerConsumer(ctx, consumer.CustomerConfig{
			Consumer:      custConsumer,
			Producer:      producer,
			Store:         store,
			Serde:         codec,
			CustomerTopic: cfg.KafkaConsumerTopic,
			DLQTopic:      cfg.KafkaDLQTopic,
			ExternalAPI:   external.CallCustomerAPI,
		}); err != nil {
			log.Printf("[Main]: customer consumer exited: %v", err)
		}
	}()

	// ── Gin HTTP server ─────────────────────────────────────────────────────────
	router := api.NewRouter(producer, codec, cfg.KafkaValidationTopic)
	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("[Main] API on: %s", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[Main] server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("[Main] shutting down")
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutCancel()
	srv.Shutdown(shutCtx)
}
