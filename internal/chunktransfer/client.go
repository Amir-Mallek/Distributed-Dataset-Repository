package chunktransfer

import (
	"context"
	"fmt"
	"hash/crc32"

	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/chunktransfer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const blockSize = 1024 * 1024 // 1 MB per block

type Client struct {
	conn   *grpc.ClientConn
	client pb.ChunkTransferServiceClient
}

func NewClient(serverAddr string) (*Client, error) {
	conn, err := grpc.NewClient(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", serverAddr, err)
	}
	return &Client{
		conn:   conn,
		client: pb.NewChunkTransferServiceClient(conn),
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// SendChunk sends all data as a single chunk identified by chunkId.
// It streams the data as 1 MB blocks, carrying clientId and datasetId on every block.
func (c *Client) SendChunk(ctx context.Context, clientId, datasetId string, chunkId uint32, data []byte) error {
	// Step 1: Open a client-streaming WriteChunk call
	stream, err := c.client.WriteChunk(ctx)
	if err != nil {
		return fmt.Errorf("WriteChunk stream open failed: %w", err)
	}

	// Step 2: Split data into fixed-size blocks and stream them
	blockIndex := uint32(0)
	for offset := 0; offset < len(data); offset += blockSize {
		end := offset + blockSize
		if end > len(data) {
			end = len(data)
		}
		block := data[offset:end]

		err := stream.Send(&pb.WriteBlockRequest{
			ChunkId:    chunkId,
			ClientId:   clientId,
			DatasetId:  datasetId,
			BlockIndex: blockIndex,
			Data:       block,
			Checksum:   crc32.ChecksumIEEE(block),
		})
		if err != nil {
			return fmt.Errorf("failed to send block %d: %w", blockIndex, err)
		}
		blockIndex++
	}

	// Step 4: Close the stream and wait for the server's acknowledgement
	if _, err := stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("CloseAndRecv failed: %w", err)
	}
	return nil
}
