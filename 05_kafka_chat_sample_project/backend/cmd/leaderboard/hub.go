package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Hub interface {
	Run()
	AddClient(conn *websocket.Conn)
	StartPing(interval time.Duration)
}

const (
	pongWait   = 90 * time.Second
	pingPeriod = 30 * time.Second
)

type HubStore struct {
	clients map[*websocket.Conn]bool
	mutex   sync.Mutex
}

func NewHub() *HubStore {
	return &HubStore{
		clients: make(map[*websocket.Conn]bool),
	}
}

func (h *HubStore) AddClient(conn *websocket.Conn) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.clients[conn] = true
}

func (h *HubStore) RemoveClient(conn *websocket.Conn) {
	h.mutex.Lock()
	delete(h.clients, conn)
	defer func() {
		h.mutex.Unlock()
		err := conn.Close()
		if err != nil {
			fmt.Println("error in remove hub client:", err)
		}
	}()
}

func (h *HubStore) Broadcast(message []byte) {
	h.mutex.Lock()
	defer h.mutex.Unlock()

	for conn := range h.clients {
		err := conn.WriteMessage(websocket.TextMessage, message)
		if err != nil {
			fmt.Println("🚨 error when writing data in the socket!")
			conn.Close()
			delete(h.clients, conn)
		}
	}
}

func (h *HubStore) StartPing(interval time.Duration) {
	ticker := time.NewTicker(interval)

	go func() {
		for range ticker.C {
			h.mutex.Lock()
			for conn := range h.clients {
				err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err != nil {
					fmt.Println("error in setWriteDeadline", err)
					conn.Close()
					delete(h.clients, conn)
					break
				}
				if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
					fmt.Println("error in writing websocket message", err)
					conn.Close()
					delete(h.clients, conn)
					break
				}
			}
			h.mutex.Unlock()
		}
	}()
}

func StartWebsocketServer(ctx context.Context, redis *RedisClientStore) {
	hub := NewHub()

	// Start ping every 30 seconds
	hub.StartPing(pingPeriod)

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("Upgrade error:", err)
			return
		}

		// Configure connection timeouts
		conn.SetReadLimit(512)
		err = conn.SetReadDeadline(time.Now().Add(pongWait))
		if err != nil {
			fmt.Println("error when set readline outside:", err)
			return
		}
		conn.SetPongHandler(func(appData string) error {
			err := conn.SetReadDeadline(time.Now().Add(pongWait))
			if err != nil {
				fmt.Println("set pong handler error:", err)
				return err
			}
			return nil
		})

		hub.AddClient(conn)

		go func() {
			defer hub.RemoveClient(conn)
			for {
				_, _, err := conn.ReadMessage()
				if err != nil {
					fmt.Println("websocket read message error:", err)
					break
				}
			}
		}()
	})

	go func() {
		pubsub := redis.Client.Subscribe(ctx, "leaderboard_updates")
		ch := pubsub.Channel()

		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-ch:
				hub.Broadcast([]byte(msg.Payload))
			}
		}
	}()

	log.Println("🌐 WebSocket server running on :8085")
	http.ListenAndServe(":8084", nil)
}
