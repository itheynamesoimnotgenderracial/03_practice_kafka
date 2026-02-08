package main

import (
	"bufio"
	"errors"
	"fmt"
	"kafka_mongodb/config"
	"kafka_mongodb/pkg/screen"
	"kafka_mongodb/pkg/stats"
	logger "log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/jessevdk/go-flags"
	log "github.com/rs/zerolog/log"
)

const bootstrapServersKey = "bootstrap.servers"

func stringPtr(s string) *string {
	return &s
}

func run(cfg *config.Config, transactionFile string) error {
	log.Info().Msg("main: Initializing Kafka producer")
	log.Info().Msg("main: Completed")
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		bootstrapServersKey: cfg.KafkaBrokerHost,
	})
	if err != nil {
		return fmt.Errorf("creating producer: %v", err)
	}
	defer producer.Close()
	file, err := os.Open(transactionFile)
	if err != nil {
		return fmt.Errorf("opening file %s: %v", transactionFile, err)
	}

	defer file.Close()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	serverErrors := make(chan error, 1)

	stats := &stats.KafkaProducerStats{}
	screen, err := screen.NewKafkaProducerScreen(stats)
	if err != nil {
		return errors.New("starting screen")
	}

	start := time.Now()

	go func() {
		for {
			time.Sleep(time.Second * time.Duration(1))
			stats.UpdateElapsedTime(time.Since(start))
			screen.UpdateContent(false)
		}
	}()

	deliveryChan := make(chan kafka.Event)
	scanner := bufio.NewScanner(file)

	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			if err := producer.Produce(&kafka.Message{
				TopicPartition: kafka.TopicPartition{
					Topic:     stringPtr(cfg.KafkaTopic),
					Partition: kafka.PartitionAny,
				},
				Value: []byte(line),
			}, deliveryChan); err != nil {
				log.Error().Msgf("%v when publishing to kafka topic %s", err, cfg.KafkaTopic)
			}
		}
		if err := scanner.Err(); err != nil {
			log.Error().Msgf("reading file %s: %v", transactionFile, err)
		}
	}()

	select {
	case err := <-serverErrors:
		return err
	case sig := <-shutdown:
		screen.UpdateContent(true)
		log.Error().Msgf("run: %v: Start shutdown", sig)
		return nil
	}
}

var opts struct {
	File string `short:"f" long:"file" description:"input file" required:"true"`
}

const (
	ENV_FILE      = "app"
	LOG_FILE_NAME = "logs/producer.txt"
)

func main() {

	flags.ParseArgs(&opts, os.Args)
	logFile, err := os.OpenFile(LOG_FILE_NAME, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Error().Msgf(`opening log file: "%s": %v`, LOG_FILE_NAME, err)
	}
	logger.New(logFile, "KAFKA PRODUCER : ")
	cfg, err := config.LoadConfig(ENV_FILE)
	if err != nil {
		log.Error().Msgf("reading config error: %v", err)
		os.Exit(1)
	}
	if err := run(&cfg, opts.File); err != nil {
		log.Error().Msg("err")
		os.Exit(1)
	}
}
