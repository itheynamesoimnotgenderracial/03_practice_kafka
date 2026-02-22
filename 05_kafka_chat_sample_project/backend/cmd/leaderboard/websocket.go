package main

import (
	"context"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}
var clients = make(map[*websocket.Conn]bool)

func StartWebsocker(redis *RedisClientStore) {
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, _ := upgrader.Upgrade(w, r, nil)
		clients[conn] = true
	})

	go subscribe(redis)

	log.Println("WebSocket server running on :8084")
	http.ListenAndServe(":8084", nil)
}

func subscribe(redis *RedisClientStore) {
	pubsub := redis.Client.Subscribe(context.Background(), "leaderboard_updates")
	ch := pubsub.Channel()

	for msg := range ch {
		broadcast([]byte(msg.Payload))
	}
}

func broadcast(message []byte) {
	for client := range clients {
		client.WriteMessage(websocket.TextMessage, message)
	}
}
