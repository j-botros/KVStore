package grpc

import (
	"context"
	"errors"

	service "kvstore/pkg/kvstore/service"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GRPCServer struct {
	UnimplementedKVStoreServer
	service *service.Service
}

func NewGRPCServer(service *service.Service) *GRPCServer {
	return &GRPCServer{service: service}
}

// Request forwarding

func (s *GRPCServer) ForwardGet(ctx context.Context, req *ForwardGetRequest) (*ForwardGetResponse, error) {
	val, err := s.service.Get(req.Key)
	if errors.Is(err, service.ErrNotFound) {
		return &ForwardGetResponse{Found: false}, nil
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "get failed: %v", err)
	}

	return &ForwardGetResponse{
		Value: val,
		Found: true,
	}, nil
}

func (s *GRPCServer) ForwardPut(ctx context.Context, req *ForwardPutRequest) (*ForwardPutResponse, error) {
	err := s.service.Put(req.Key, req.Value)
	if err != nil {
		return &ForwardPutResponse{Success: false}, status.Errorf(codes.Internal, "put failed: %v", err)
	}
	return &ForwardPutResponse{Success: true}, nil
}

func (s *GRPCServer) ForwardDelete(ctx context.Context, req *ForwardDeleteRequest) (*ForwardDeleteResponse, error) {
	err := s.service.Delete(req.Key)
	if err != nil && !errors.Is(err, service.ErrNotFound) {
		return &ForwardDeleteResponse{Success: false}, status.Errorf(codes.Internal, "delete failed: %v", err)
	}
	return &ForwardDeleteResponse{Success: true}, nil
}

// Replication

func (s *GRPCServer) ReplicateEntry(ctx context.Context, req *ReplicateEntryRequest) (*ReplicateEntryResponse, error) {
	entry := req.Entry
	if entry == nil {
		return &ReplicateEntryResponse{Success: false, ErrorDetail: "nil entry"}, nil
	}

	err := s.service.ReplicateEntry(service.LogEntry{
		SeqNum:    entry.SeqNum,
		Operation: entry.Operation,
		Key:       entry.Key,
		Value:     entry.Value,
	})
	if err != nil {
		return &ReplicateEntryResponse{Success: false, ErrorDetail: err.Error()}, nil
	}
	return &ReplicateEntryResponse{Success: true}, nil
}
