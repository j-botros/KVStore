package controller

import (
	"context"
	"encoding/json"
	"errors"
	coordinator "kvstore/pkg/kvstore/coordinator"
	"net/http"
	"time"
)

// StoreTimeouts holds the per-operation deadline for HTTP handlers.
// Values come from config.yaml; main.go applies defaults when a field is zero.
type StoreTimeouts struct {
	Get    time.Duration
	Put    time.Duration
	Delete time.Duration
}

type StoreController struct {
	coordinator *coordinator.Coordinator
	timeouts    StoreTimeouts
}

// Constructor
func NewStoreController(coordinator *coordinator.Coordinator, timeouts StoreTimeouts) *StoreController {
	return &StoreController{coordinator: coordinator, timeouts: timeouts}
}

// Register the routes
func (c *StoreController) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /kv/{key}", c.handleGet)
	mux.HandleFunc("POST /kv/{key}", c.handlePut)
	mux.HandleFunc("DELETE /kv/{key}", c.handleDelete)
}

func (c *StoreController) handleGet(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		GetLatencyMs.Observe(float64(time.Since(start).Milliseconds()))
		GetRequestsTotal.Inc()
	}()

	// r.Context() is already wired to cancel on client disconnect.
	// WithTimeout adds a hard ceiling on top of that — whichever fires first wins.
	ctx, cancel := context.WithTimeout(r.Context(), c.timeouts.Get)
	defer cancel()

	key := r.PathValue("key")

	value, err := c.coordinator.Get(ctx, key)
	if errors.Is(err, coordinator.ErrNotFound) {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, "failed to fetch value", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	json.NewEncoder(w).Encode(map[string]string{
		"key":   key,
		"value": string(value),
	})
}

type putRequest struct {
	Value string `json:"value"`
}

func (c *StoreController) handlePut(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		PutLatencyMs.Observe(float64(time.Since(start).Milliseconds()))
		PutRequestsTotal.Inc()
	}()

	ctx, cancel := context.WithTimeout(r.Context(), c.timeouts.Put)
	defer cancel()

	key := r.PathValue("key")

	var body putRequest
	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	err = c.coordinator.Put(ctx, key, []byte(body.Value))
	if err != nil {
		http.Error(w, "failed to put key-value", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (c *StoreController) handleDelete(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	defer func() {
		DeleteLatencyMs.Observe(float64(time.Since(start).Milliseconds()))
		DeleteRequestsTotal.Inc()
	}()

	ctx, cancel := context.WithTimeout(r.Context(), c.timeouts.Delete)
	defer cancel()

	key := r.PathValue("key")

	err := c.coordinator.Delete(ctx, key)
	if errors.Is(err, coordinator.ErrNotFound) {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, "failed to delete key", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
