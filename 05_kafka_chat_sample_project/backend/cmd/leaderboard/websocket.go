package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}
var clients = make(map[*websocket.Conn]bool)

func StartWebsocketServer(ctx context.Context, redis *RedisClientStore) {
	hourlyHub := NewHub()
	dailyHub := NewHub()

	go hourlyHub.Run()
	go dailyHub.Run()

	server := &http.Server{
		Addr: ":8084",
	}

	http.HandleFunc("/ws/hourly", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-ctx.Done():
			http.Error(w, "Server shutting down", http.StatusServiceUnavailable)
			return
		default:
			serveWs(hourlyHub, w, r)
		}
	})

	http.HandleFunc("/ws/daily", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-ctx.Done():
			http.Error(w, "Server shutting down", http.StatusServiceUnavailable)
			return
		default:
			serveWs(dailyHub, w, r)
		}
	})

	go func() {
		pubsub := redis.Client.Subscribe(ctx, "leaderboard_updates:hourly")
		ch := pubsub.Channel()

		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-ch:
				hourlyHub.broadcast <- []byte(msg.Payload)
			}
		}
	}()

	go func() {
		pubsub := redis.Client.Subscribe(ctx, "leaderboard_updates:daily")
		ch := pubsub.Channel()

		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-ch:
				dailyHub.broadcast <- []byte(msg.Payload)
			}
		}
	}()

	go func() {
		log.Println("🌐 WebSocket server running on :8084")
		log.Println("  /ws/hourly  — hourly leaderboard updates")
		log.Println("  /ws/daily   — daily leaderboard updates")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Println("ListenAndServe:", err)
		}
	}()

	<-ctx.Done()
	log.Println("🛑 WebSocket server shutting down")

	hourlyHub.broadcast <- []byte(`{"type": "server_shutdown"}`)
	dailyHub.broadcast <- []byte(`{"type": "server_shutdown"}`)

	time.Sleep(2 * time.Second)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Println("server shutdown failed:", err)
	}

	log.Println("✅ WebSocket server exited cleanly")
}
