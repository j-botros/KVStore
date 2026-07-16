package controller

import (
	"encoding/json"
	"errors"
	service "kvstore/pkg/kvstore/service"
	"net/http"
)

type StoreController struct {
	service *service.Service
}

// Constructor
func NewStoreController(service *service.Service) *StoreController {
	return &StoreController{service: service}
}

// Register the routes
func (c *StoreController) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /kv/{key}", c.handleGet)
	mux.HandleFunc("POST /kv/{key}", c.handlePut)
	mux.HandleFunc("DELETE /kv/{key}", c.handleDelete)
}

func (c *StoreController) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	value, err := c.service.Get(key)
	if errors.Is(err, service.ErrNotFound) {
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
	key := r.PathValue("key")

	var body putRequest
	err := json.NewDecoder(r.Body).Decode(&body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	err = c.service.Put(key, []byte(body.Value))
	if err != nil {
		http.Error(w, "failed to put key-value", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (c *StoreController) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	err := c.service.Delete(key)
	if errors.Is(err, service.ErrNotFound) {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	} else if err != nil {
		http.Error(w, "failed to delete key", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
