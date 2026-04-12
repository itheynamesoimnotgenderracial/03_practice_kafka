package config

import "os"

type Config struct {
	PostgresDSN          string
	KafkaBrokers         string
	KafkaGroupID         string
	KafkaValidationTopic string
	KafkaConsumerTopic   string
	KafkaDLQTopic        string
	SchemaRegistryURL    string
	Port                 string
}

func Load() *Config {
	return &Config{
		PostgresDSN:          getEnv("POSTGRES_DSN", "postgres://postgres:postgres@localhost:5432/customerdb?sslmode=disable"),
		KafkaBrokers:         getEnv("KAFKA_BROKERS", "kafka1:29092"),
		KafkaGroupID:         getEnv("KAFKA_GROUP_ID", "semi-eos"),
		KafkaValidationTopic: getEnv("KAFKA_VALIDATION_TOPIC", "simple.eos.validator"),
		KafkaConsumerTopic:   getEnv("KAFKA_CONSUMER_TOPIC", "simple.eos.consumer"),
		KafkaDLQTopic:        getEnv("KAFKA_DLQ_TOPIC", "simple.eos.dlq"),
		SchemaRegistryURL:    getEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081"),
		Port:                 getEnv("PORT", "8084"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
