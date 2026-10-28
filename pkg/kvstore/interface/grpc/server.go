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
	nodeId  string
	service *service.Service
}

func NewGRPCServer(nodeId string, service *service.Service) *GRPCServer {
	return &GRPCServer{
		nodeId:  nodeId,
		service: service,
	}
}

// Request Forwarding

func (s *GRPCServer) ForwardGet(ctx context.Context, req *ForwardGetRequest) (*ForwardGetResponse, error) {
	val, err := s.service.Get(ctx, req.Key)
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
	err := s.service.Put(ctx, req.Key, req.Value)
	if err != nil {
		return &ForwardPutResponse{Success: false}, status.Errorf(codes.Internal, "put failed: %v", err)
	}
	return &ForwardPutResponse{Success: true}, nil
}

func (s *GRPCServer) ForwardDelete(ctx context.Context, req *ForwardDeleteRequest) (*ForwardDeleteResponse, error) {
	err := s.service.Delete(ctx, req.Key)
	if err != nil && !errors.Is(err, service.ErrNotFound) {
		return &ForwardDeleteResponse{Success: false}, status.Errorf(codes.Internal, "delete failed: %v", err)
	}
	return &ForwardDeleteResponse{Success: true}, nil
}

// Fault Tolerance

// Replay streams the full dataset of this node's shard to a recovering peer.
//
// It calls service.Scan, which holds a read lock on the storage engine for the
// entire duration. Entries are batched into chunks of up to 500 entries each to
// bound per-chunk memory usage. The final chunk carries SenderSeqNum and
// IsLast=true so the receiver knows it received a complete snapshot and can
// update its replication sequence number.
//
// If the stream breaks mid-scan, yield returns the send error, which propagates
// back through Scan and out of Replay immediately (no further disk I/O occurs).
func (s *GRPCServer) Replay(req *ReplayRequest, stream KVStore_ReplayServer) error {
	const batchSize = 500
	batch := make([]*LogEntry, 0, batchSize)

	err := s.service.Scan(func(key string, value []byte) error {
		batch = append(batch, &LogEntry{Key: key, Value: value})
		if len(batch) >= batchSize {
			if err := stream.Send(&ReplayChunk{Entries: batch}); err != nil {
				return err
			}
			batch = batch[:0]
		}
		return nil
	})
	if err != nil {
		return status.Errorf(codes.Internal, "Replay: scan failed: %v", err)
	}

	// Send the final (possibly partial) batch with the donor's sequence number
	// and the IsLast marker so the receiver knows the snapshot is complete.
	return stream.Send(&ReplayChunk{
		Entries:      batch,
		SenderSeqNum: s.service.GetSeqnum(),
		IsLast:       true,
	})
}

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

func (s *GRPCServer) GetReplicationStatus(
	ctx context.Context,
	req *GetSeqnumRequest,
) (*GetSeqnumResponse, error) {
	return &GetSeqnumResponse{
		SeqNum: s.service.GetSeqnum(),
		NodeId: s.nodeId,
	}, nil
}
