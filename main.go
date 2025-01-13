package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"net/http"
	"time"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

var (
	upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			// Дозволити всі підключення
			return true
		},
	}

	//  url := "redis://user:password@localhost:6379/0?protocol=3"
    // opts, err := redis.ParseURL(url)
    // if err != nil {
    //     panic(err)
    // }
    // return redis.NewClient(opts)


	rdb = redis.NewClient(&redis.Options{
		Addr:     "test-go-redis-redis-tpqofm:6379",
		Password: "vfpiebtu5hhuvcbk", // Replace with your password if needed
		DB:       0,
	})
	ctx = context.Background() // Define the context globally
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}
	http.HandleFunc("/ws", handleWebSocket)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Failed to upgrade to WebSocket:", err)
		return
	}
	defer conn.Close()

	for {
		messageType, messageBytes, err := conn.ReadMessage()
		if err != nil {
			log.Println("Read error:", err)
			break
		}

		if messageType != websocket.TextMessage {
			log.Println("Non-text message received")
			continue
		}

		var data map[string]string
		err = json.Unmarshal(messageBytes, &data)
		if err != nil {
			log.Println("JSON unmarshal error:", err)
			continue
		}

		log.Println("Received event:", data["event"])
		switch data["event"] {
		case "parsing_start":
			subscriptionId := data["subscriptionId"]
			if isSubscriptionActive(subscriptionId) {
				sendResponse(conn, "parsing_response", map[string]string{"success": "false", "message": "Subscription in use"})
			} else {
				markSubscriptionActive(subscriptionId)
				sendResponse(conn, "parsing_response", map[string]string{"success": "true", "message": "Parsing started"})
			}
		case "parsing_end":
			subscriptionId := data["subscriptionId"]
			unmarkSubscriptionActive(subscriptionId)
		case "heartbeat":
			subscriptionId := data["subscriptionId"]
			refreshSubscription(subscriptionId)
		default:
			log.Println("Unknown event:", data["event"])
		}
	}
}

func isSubscriptionActive(subscriptionId string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	exists, err := rdb.Exists(ctx, subscriptionId).Result()
	if err != nil {
		log.Println("Redis error:", err)
		return false
	}
	return exists == 1
}

func markSubscriptionActive(subscriptionId string) {
	err := rdb.Set(ctx, subscriptionId, "active", 2*60*time.Second).Err()
	if err != nil {
		log.Println("Redis error:", err)
	}
}

func unmarkSubscriptionActive(subscriptionId string) {
	err := rdb.Del(ctx, subscriptionId).Err()
	if err != nil {
		log.Println("Redis error:", err)
	}
}

func refreshSubscription(subscriptionId string) {
	log.Println("Subscription refreshed:", subscriptionId)
	err := rdb.Expire(ctx, subscriptionId, 10*60*time.Second).Err()
	if err != nil {
		log.Println("Redis error:", err)
	}
}

func sendResponse(conn *websocket.Conn, eventType string, data map[string]string) {
	respBytes, err := json.Marshal(map[string]interface{}{
		"event": eventType,
		"data":  data,
	})
	if err != nil {
		log.Println("JSON marshal error:", err)
		return
	}
	err = conn.WriteMessage(websocket.TextMessage, respBytes)
	if err != nil {
		log.Println("Write error:", err)
	}
}