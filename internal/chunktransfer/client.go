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
// It first creates the chunk on the server, then streams the data as blocks.
func (c *Client) SendChunk(ctx context.Context, chunkId uint32, data []byte) error {
	// Step 1: Tell the server to create a new chunk
	_, err := c.client.CreateChunk(ctx, &pb.CreateChunkRequest{ChunkId: chunkId})
	if err != nil {
		return fmt.Errorf("CreateChunk failed: %w", err)
	}

	// Step 2: Open a client-streaming WriteBlock call
	stream, err := c.client.WriteBlock(ctx)
	if err != nil {
		return fmt.Errorf("WriteBlock stream open failed: %w", err)
	}

	// Step 3: Split data into fixed-size blocks and stream them
	blockIndex := uint32(0)
	for offset := 0; offset < len(data); offset += blockSize {
		end := offset + blockSize
		if end > len(data) {
			end = len(data)
		}
		block := data[offset:end]

		err := stream.Send(&pb.WriteBlockRequest{
			ChunkId:    chunkId,
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
