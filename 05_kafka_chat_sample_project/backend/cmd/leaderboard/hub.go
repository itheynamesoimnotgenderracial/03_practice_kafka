package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 90 * time.Second
	pingPeriod     = 30 * time.Second
	maxMessageSize = 512
	sendBufferSize = 256
)

type Client struct {
	hub  *Hub
	conn *websocket.Conn
	send chan []byte
}

type Hub struct {
	clients    map[*Client]bool
	register   chan *Client
	unregister chan *Client
	broadcast  chan []byte
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan []byte),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
		case message := <-h.broadcast:
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					fmt.Println("🚨 Bffer is full. Client receive must be slow. Closing this connection for now")
					close(client.send)
					delete(h.clients, client)
				}
			}
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		err := c.conn.Close()
		if err != nil {
			log.Println("failed to close websocket connection")
		}
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				fmt.Println("websocket write message failed:", err)
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				fmt.Println("error with ping message:", err)
				return
			}
		}
	}
}

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		err := c.conn.Close()
		log.Println("failure to close connection when reading to pump:", err)
	}()

	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(appData string) error {
		err := c.conn.SetReadDeadline(time.Now().Add(pongWait))
		if err != nil {
			fmt.Println("failed to meet the pong deadline:", err)
			return err
		}
		return nil
	})

	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			break
		}
	}
}

func StartWebsocketServer(ctx context.Context, redis *RedisClientStore) {
	hub := NewHub()
	go hub.Run()

	server := &http.Server{
		Addr: ":8084",
	}

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-ctx.Done():
			http.Error(w, "Server shutting down", http.StatusServiceUnavailable)
			return
		default:
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				log.Println("Upgrade error:", err)
				return
			}

			client := &Client{
				hub:  hub,
				conn: conn,
				send: make(chan []byte, sendBufferSize),
			}

			hub.register <- client

			go client.writePump()
			go client.readPump()
		}
	})

	go func() {
		pubsub := redis.Client.Subscribe(ctx, "leaderboard_updates")
		ch := pubsub.Channel()

		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-ch:
				hub.broadcast <- []byte(msg.Payload)
			}
		}
	}()

	go func() {
		log.Println("🌐 WebSocket server running on :8085")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Println("ListenAndServe:", err)
		}
	}()

	<-ctx.Done()
	log.Println("🛑 WebSocket server shutting down")

	// 1️⃣ Notify clients
	hub.broadcast <- []byte(`"type:": "server_shutdown"`)

	// 2️⃣ Give clients time to receive
	time.Sleep(2 * time.Second)

	// 3️⃣ Graceful HTTP shutdown
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Println("server shutdown failed:", err)
	}

	log.Println("✅ WebSocket server exited cleanly")
}
