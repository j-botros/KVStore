package kvstore

import (
	"fmt"
	"log"
	"net"
	"net/http"

	grpcInterface "kvstore/pkg/kvstore/interface/grpc"
	ctrl "kvstore/pkg/kvstore/interface/http"

	"google.golang.org/grpc"
)

type KVStore struct {
	storeCtrl      *ctrl.StoreController
	monitoringCtrl *ctrl.MonitoringController
	grpcServer     *grpcInterface.GRPCServer
	nodeId         string
}

func NewKVStore(storeCtrl *ctrl.StoreController, monitoringCtrl *ctrl.MonitoringController, grpcServer *grpcInterface.GRPCServer, nodeId string) *KVStore {
	return &KVStore{
		storeCtrl:      storeCtrl,
		monitoringCtrl: monitoringCtrl,
		grpcServer:     grpcServer,
	}
}

func (kvstore *KVStore) Start(httpPort int, grpcPort int) error {
	// 1. Start the gRPC server in a background goroutine
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
	if err != nil {
		return fmt.Errorf("failed to listen on gRPC port %d: %w", grpcPort, err)
	}

	grpcServer := grpc.NewServer()
	grpcInterface.RegisterKVStoreServer(grpcServer, kvstore.grpcServer)

	go func() {
		log.Printf("gRPC server listening on :%d", grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	// 2. Set up HTTP routes
	mux := http.NewServeMux()

	// Register KV CRUD routes via Controller
	kvstore.storeCtrl.RegisterRoutes(mux)

	// Register operational routes
	kvstore.monitoringCtrl.RegisterRoutes(mux)

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		w.Write([]byte(`{
			"service": "kvstore",
			"status": "running",
			"node_id": "` + kvstore.nodeId + `",
			"endpoints": {
				"GET /kv/{key}": "retrieve value",
				"POST /kv/{key}": "set key/value",
				"DELETE /kv/{key}": "delete key"
			}
		}`))
	})

	// 3. Start the blocking HTTP server
	addr := fmt.Sprintf(":%d", httpPort)
	log.Printf("HTTP server listening on %s", addr)
	return http.ListenAndServe(addr, mux)
}
