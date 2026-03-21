package ws

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 90 * time.Second
	pingPeriod     = 30 * time.Second
	maxMessage     = 4096
	sendBufferSize = 256
)

type Client struct {
	hub  *RoomHub
	conn *websocket.Conn
	send chan []byte
}

type RoomHub struct {
	roomID     string
	clients    map[*Client]bool
	register   chan *Client
	unregister chan *Client
	broadcast  chan []byte
	cancel     context.CancelFunc
	manager    *RoomManager
}

type RoomManager struct {
	mu    sync.Mutex
	rooms map[string]*RoomHub
	redis *redis.Client
}

func NewRoomManager(rdb *redis.Client) *RoomManager {
	return &RoomManager{
		rooms: make(map[string]*RoomHub),
		redis: rdb,
	}
}

func (rm *RoomManager) GetOrCreateHub(roomID string) *RoomHub {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if hub, exists := rm.rooms[roomID]; exists {
		return hub
	}

	ctx, cancel := context.WithCancel(context.Background())

	hub := &RoomHub{
		roomID:     roomID,
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		broadcast:  make(chan []byte, 64),
		cancel:     cancel,
		manager:    rm,
	}

	rm.rooms[roomID] = hub

	go hub.run()
	go hub.subscribedRedis(ctx, rm.redis)

	log.Printf("🔌 Room hub created for %s\n", roomID)
	return hub
}

func (rm *RoomManager) removeHub(roomID string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if hub, exists := rm.rooms[roomID]; exists {
		hub.cancel()
		delete(rm.rooms, roomID)
		log.Printf("🧹 Room hub removed for %s (no clients)\n", roomID)
	}
}

func (h *RoomHub) run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client] = true
			log.Printf("👤 Client joined room %s (total: %d)\n", h.roomID, len(h.clients))

		case client := <-h.unregister:
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
				log.Printf("👤 Client left room %s (total: %d)\n", h.roomID, len(h.clients))

				if len(h.clients) == 0 {
					go h.manager.removeHub(h.roomID)
					return
				}
			}
		case message := <-h.broadcast:
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					fmt.Println("🚨 Buffer full, dropping slow client in room", h.roomID)

					close(client.send)
					delete(h.clients, client)
				}
			}
		}
	}
}

func (h *RoomHub) subscribedRedis(ctx context.Context, rdb *redis.Client) {
	channel := fmt.Sprintf("chat_room:%s", h.roomID)
	pubsub := rdb.Subscribe(ctx, channel)
	defer func() {
		err := pubsub.Close()
		if err != nil {
			log.Println("failed on closing API pubsub closure:", err)
		}
	}()

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-ch:
			if msg != nil {
				h.broadcast <- []byte(msg.Payload)
			}
		}
	}
}

func ServeWs(hub *RoomHub, conn *websocket.Conn) {
	client := &Client{
		hub:  hub,
		conn: conn,
		send: make(chan []byte, sendBufferSize),
	}

	hub.register <- client

	go client.writePump()
	go client.readPump()
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		if err := c.conn.Close(); err != nil {
			log.Println("failed at closing writePump websocket connection:", err)
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
				return
			}
		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		if err := c.conn.Close(); err != nil {
			log.Println("failed at closing readPump websocket connection:", err)
		}
	}()

	c.conn.SetReadLimit(maxMessage)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(appData string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			break
		}
	}
}
