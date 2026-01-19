package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	topicrawTopic  = "raw-user-events"
	URL            = "localhost:9092"
	groupID        = "user-event-transformer-grp"
	processedTopic = "process-user-events"
)

// Input Event represents the structure of the raw incoming message
type InputEvent struct {
	Type    string `json:"type"`
	UserId  string `json:"user_id"`
	Payload string `json:"payload"`
}

// Input Event represents the structure of the cleaned transformed outgoing message
type OutputEvent struct {
	UserId       string `json:"user_id"`
	Action       string `json:"action"`
	Timestamp    int64  `json:"timestamp"`
	OriginalData string `json:"original_data"`
}

func main() {
	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{URL},
		Topic:   topicrawTopic,
		GroupID: groupID,
	})
	defer consumer.Close()

	// Procuer setup (Transformer)
	producer := kafka.Writer{
		Addr: kafka.TCP(URL),
	}
	defer producer.Close()

	log.Println("Starting stream transformer...")

	ctx := context.Background()

	for {
		// Read tge raw message
		inMsg, err := consumer.ReadMessage(ctx)
		if err != nil {
			log.Println("Could not read the message")
			break
		}

		// Transformation 1: Peek/ForEach (The Inspector)
		// we log every message that we receive for auditing/debugging
		log.Printf("PEEK: Received raw message: key: %s, value=%s", string(inMsg.Key), string(inMsg.Value))

		// Transformation2: Filter (the bouncer)
		var inputEvent InputEvent
		err = json.Unmarshal(inMsg.Value, &inputEvent)
		if err != nil {
			log.Printf("FILTER: Discarding malformed JSON message %s. Error %v\n", string(inMsg.Value), err)
			continue
		}

		if inputEvent.Type != "login" {
			log.Println("FILTER: Discarding message of type:", inputEvent.Type)
			continue
		}

		// Traansformation 3: Map (The Translator)
		outputEvent := OutputEvent{
			UserId:       inputEvent.UserId,
			Action:       strings.ToUpper(inputEvent.Type),
			Timestamp:    time.Now().Unix(),
			OriginalData: inputEvent.Payload,
		}

		outValue, err := json.Marshal(outputEvent)
		if err != nil {
			log.Printf("Error marshalling output event: %v\n", err)
		}

		outMsg := kafka.Message{
			Topic: processedTopic,
			Key:   []byte(outputEvent.UserId),
			Value: outValue,
		}

		err = producer.WriteMessages(ctx, outMsg)
		if err != nil {
			log.Println("Failed to write transform message: ", err)
		} else {
			log.Println("MAP: Successfully produced transformed message for key:", string(outMsg.Key))
		}
	}
}
