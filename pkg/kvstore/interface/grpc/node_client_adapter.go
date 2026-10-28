package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"

	coordinator "kvstore/pkg/kvstore/coordinator"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
)

// NodeClientAdapter wraps the generated KVStoreClient and satisfies
// coordinator.NodeClient, keeping the grpc package out of the coordinator layer.
type NodeClientAdapter struct {
	client KVStoreClient
}

var _ coordinator.NodeClient = (*NodeClientAdapter)(nil)

// NewNodeClientAdapter connects to addr and returns an adapter ready for use.
func NewNodeClientAdapter(addr string) (*NodeClientAdapter, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &NodeClientAdapter{client: NewKVStoreClient(conn)}, nil
}

func (a *NodeClientAdapter) ForwardGet(ctx context.Context, key string) ([]byte, bool, error) {
	resp, err := a.client.ForwardGet(ctx, &ForwardGetRequest{Key: key})
	if err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return resp.Value, resp.Found, nil
}

func (a *NodeClientAdapter) ForwardPut(ctx context.Context, key string, value []byte) error {
	resp, err := a.client.ForwardPut(ctx, &ForwardPutRequest{Key: key, Value: value})
	if err != nil {
		return err
	}
	if !resp.Success {
		return errors.New("remote put reported failure")
	}
	return nil
}

func (a *NodeClientAdapter) ForwardDelete(ctx context.Context, key string) error {
	resp, err := a.client.ForwardDelete(ctx, &ForwardDeleteRequest{Key: key})
	if err != nil {
		if isNotFound(err) {
			return coordinator.ErrNotFound
		}
		return err
	}
	if !resp.Success {
		return errors.New("remote delete reported failure")
	}
	return nil
}

func (a *NodeClientAdapter) ReplicateEntry(ctx context.Context, entry coordinator.LogEntry) error {
	resp, err := a.client.ReplicateEntry(ctx, &ReplicateEntryRequest{
		Entry: &LogEntry{
			SeqNum:    entry.SeqNum,
			Operation: entry.Operation,
			Key:       entry.Key,
			Value:     entry.Value,
		},
	})
	if err != nil {
		return err
	}
	if !resp.Success {
		return fmt.Errorf("follower replication failed: %s", resp.ErrorDetail)
	}
	return nil
}

func (a *NodeClientAdapter) GetSeqNum(ctx context.Context) (uint64, error) {
	resp, err := a.client.GetSeqNum(ctx, &GetSeqNumRequest{})
	if err != nil {
		return 0, err
	}
	return resp.SeqNum, nil
}

func (a *NodeClientAdapter) ReplaySnapshot(
	ctx context.Context,
	recoveringSeqNum uint64,
	onChunk func(donorSeqNum uint64, entries []coordinator.LogEntry, isLast bool) error,
) error {
	stream, err := a.client.Replay(ctx, &ReplayRequest{RecoveringSeqNum: recoveringSeqNum})
	if err != nil {
		return err
	}

	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		entries := make([]coordinator.LogEntry, 0, len(chunk.Entries))
		for _, e := range chunk.Entries {
			entries = append(entries, coordinator.LogEntry{
				SeqNum:    e.SeqNum,
				Operation: e.Operation,
				Key:       e.Key,
				Value:     e.Value,
			})
		}

		if err := onChunk(chunk.SenderSeqNum, entries, chunk.IsLast); err != nil {
			return err
		}
		if chunk.IsLast {
			break
		}
	}
	return nil
}

// isNotFound returns true when the gRPC status code is NotFound.
func isNotFound(err error) bool {
	st, ok := grpcstatus.FromError(err)
	return ok && st.Code() == codes.NotFound
}
