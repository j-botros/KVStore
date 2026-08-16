package kvstore

import (
	"fmt"
	ctrl "kvstore/pkg/kvstore/interface/http"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type KVStore struct {
	ctrl *ctrl.StoreController
}

func NewKVStore(ctrl *ctrl.StoreController) *KVStore {
	return &KVStore{ctrl: ctrl}
}

func (kvstore *KVStore) Start(port int, nodeId string) error {
	mux := http.NewServeMux()

	// 1. Register KV CRUD routes via Controller
	kvstore.ctrl.RegisterRoutes(mux)

	// 2. Register operational routes
	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		w.Write([]byte(`{
			"service": "kvstore",
			"status": "running",
			"node_id": "` + nodeId + `",
			"endpoints": {
				"GET /kv/{key}": "retrieve value",
				"POST /kv/{key}": "set key/value",
				"DELETE /kv/{key}": "delete key"
			}
		}`))
	})

	// 3. Start the blocking server
	addr := fmt.Sprintf(":%d", port)
	return http.ListenAndServe(addr, mux)
}
