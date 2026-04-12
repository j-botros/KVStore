package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	// "kvstore/pkg/kvstore"
)

type Config struct {
	Port                     int    `json:"port"`
	LogDestination           string `json:"log_destination"`
	MaxNumberOfKeys          int    `json:"max_number_of_keys"`
	MaxValueMemoryUsageBytes int    `json:"max_value_memory_usage_bytes"`
	MaxValueSizeBytes        int    `json:"max_value_size_bytes"`
	MaxKeySizeBytes          int    `json:"max_key_size_bytes"`
	ReadTimeoutMs            int    `json:"read_timeout_ms"`
	WriteTimeoutMs           int    `json:"write_timeout_ms"`
}

func main() {
	// Read the config file
	data, err := os.ReadFile("config.json")
	if err != nil {
		log.Fatalf("Failed to read config.json: %v", err)
	}

	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		log.Fatalf("Failed to parse config.json: %v", err)
	}

	// Configure logging
	if config.LogDestination != "stdout" && config.LogDestination != "" {
		file, err := os.OpenFile(config.LogDestination, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Fatalf("Failed to open log file %s: %v", config.LogDestination, err)
		}
		defer file.Close()
		log.SetOutput(file)
	} else {
		log.SetOutput(os.Stdout)
	}

	// Define root handler
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		w.Write([]byte(`{
			"service": "kvstore",
			"status": "running",
			"endpoints": {
				"GET /get?key=foo": "retrieve value",
				"POST /set": "set key/value",
				"DELETE /delete?key=foo": "delete key"
			}
		}`))
	})

	// Configure HTTP server port
	portStr := fmt.Sprintf(":%d", config.Port)
	log.Printf("Starting server on port %s...\n", portStr)

	// Open the port and start listening for HTTP requests
	if err := http.ListenAndServe(portStr, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
