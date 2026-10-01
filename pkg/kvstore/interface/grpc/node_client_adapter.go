package grpc

import (
	"context"
	"errors"

	service "kvstore/pkg/kvstore/service"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"
)

// NodeClientAdapter wraps the generated KVStoreClient and satisfies
// service.NodeClient, keeping the grpc package out of the service layer.
type NodeClientAdapter struct {
	client KVStoreClient
}

var _ service.NodeClient = (*NodeClientAdapter)(nil)

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
			return service.ErrNotFound
		}
		return err
	}
	if !resp.Success {
		return errors.New("remote delete reported failure")
	}
	return nil
}

// isNotFound returns true when the gRPC status code is NotFound.
func isNotFound(err error) bool {
	st, ok := grpcstatus.FromError(err)
	return ok && st.Code() == codes.NotFound
}
