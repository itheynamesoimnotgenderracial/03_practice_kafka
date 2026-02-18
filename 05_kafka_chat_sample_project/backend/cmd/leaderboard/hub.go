package main

import (
	"sync"

	"github.com/gorilla/websocket"
)

type Hub interface {
	Run()
}

type HubStore struct {
	clients    map[*websocket.Conn]bool
	broadcast  chan []byte
	register   chan *websocket.Conn
	unregister chan *websocket.Conn
	mutex      sync.Mutex
}

func NewHub() *HubStore {
	return &HubStore{
		clients:    make(map[*websocket.Conn]bool),
		broadcast:  make(chan []byte),
		register:   make(chan *websocket.Conn),
		unregister: make(chan *websocket.Conn),
	}
}

func (h *HubStore) Run() {
	for {
		select {
		case conn := <-h.register:
			h.mutex.Lock()
			h.clients[conn] = true
			h.mutex.Unlock()

		case conn := <-h.unregister:
			h.mutex.Lock()
			delete(h.clients, conn)
			conn.Close()
			h.mutex.Unlock()

		case message := <-h.broadcast:
			h.mutex.Lock()
			for conn := range h.clients {
				conn.WriteMessage(websocket.TextMessage, message)
			}
			h.mutex.Unlock()
		}
	}
}
