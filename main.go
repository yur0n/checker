package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
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
	http.HandleFunc("/event/", handleEvent)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func handleEvent(w http.ResponseWriter, r *http.Request) {
	subscriptionId := r.URL.Path[len("/event/"):]
	if subscriptionId == "" {
		http.Error(w, "Subscription ID is required", http.StatusBadRequest)
		return
	}

	log.Println("Received request for subscription ID:", subscriptionId)

	switch r.Method {
		case http.MethodGet:
			if isSubscriptionActive(subscriptionId) {
				sendResponse(w, "parsing_response", map[string]string{"success": "false", "message": "Subscription in use", "subscriptionId": subscriptionId})
			} else {
				markSubscriptionActive(subscriptionId)
				sendResponse(w, "parsing_response", map[string]string{"success": "true", "message": "Parsing allowed", "subscriptionId": subscriptionId})
			}
		case http.MethodDelete:
			unmarkSubscriptionActive(subscriptionId)
			sendResponse(w, "parsing_response", map[string]string{"success": "true", "message": "Parsing ended", "subscriptionId": subscriptionId})
		default:
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
	}
}

func isSubscriptionActive(subscriptionId string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	exists, err := rdb.Exists(ctx, subscriptionId).Result()
	if err != nil {
		log.Println("Redis error:", err)
		return true
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

func sendResponse(w http.ResponseWriter, eventType string, data map[string]string) {
	respBytes, err := json.Marshal(map[string]interface{}{
		"event": eventType,
		"data":  data,
	})
	if err != nil {
		log.Println("JSON marshal error:", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(respBytes)
}