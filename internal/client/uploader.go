package client

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	masterpb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/master"
	chunktransfer "github.com/Amir-Mallek/Distributed-Dataset-Repository/internal/chunktransfer"
)

const (
	DefaultChunkSize = 64 * 1024 * 1024
)

func composeHostAddress(addr string) string {
	switch addr {
	case "storage1:50051":
		return "localhost:50052"
	case "storage2:50051":
		return "localhost:50053"
	case "storage3:50051":
		return "localhost:50054"
	case "storage4:50051":
		return "localhost:50055"
	default:
		return addr
	}
}

type FileUploader struct {
	masterClient masterpb.ClientServiceClient
}

func NewFileUploader(mc masterpb.ClientServiceClient) *FileUploader {
	return &FileUploader{masterClient: mc}
}

func (u *FileUploader) UploadFile(ctx context.Context, localPath string) error {
	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	fileName := filepath.Base(localPath)
	totalSize := stat.Size()
	numChunks := (totalSize + DefaultChunkSize - 1) / DefaultChunkSize

	// Create dataset on master
	dsResp, err := u.masterClient.CreateDataset(ctx, &masterpb.CreateDatasetRequest{Name: fileName, ClientId: "client-1"})
	if err != nil {
		return fmt.Errorf("failed to create dataset: %w", err)
	}
	datasetID := dsResp.DatasetId
	fmt.Printf("Created dataset %s (id=%s)\n", fileName, datasetID)

	for i := uint32(0); i < uint32(numChunks); i++ {
		offset := int64(i) * DefaultChunkSize
		chunkData := make([]byte, DefaultChunkSize)
		n, err := file.ReadAt(chunkData, offset)
		if err != nil && err != io.EOF {
			return err
		}

		finalData := chunkData[:n]

		// Request chunk id and replicas from master
		reqResp, err := u.masterClient.RequestChunkWrite(ctx, &masterpb.RequestChunkWriteRequest{DatasetId: datasetID, ChunkIndex: i, ReplicationFactor: 3})
		if err != nil {
			return fmt.Errorf("failed to request chunk write for chunk %d: %w", i, err)
		}

		chunkID := reqResp.ChunkId
		// collect replica addresses
		var replicaAddrs []string
		for _, r := range reqResp.Replicas {
			if r != nil && r.Address != "" {
				replicaAddrs = append(replicaAddrs, r.Address)
			}
		}
		fmt.Printf("Uploading chunk %d -> chunkID=%s to replica %v\n", i, chunkID, replicaAddrs)
		if len(replicaAddrs) == 0 {
			return fmt.Errorf("no replica addresses returned for chunk %d", i)
		}

		// upload to first replica (forwarder handles others)
		ctClient, err := chunktransfer.NewClient(composeHostAddress(replicaAddrs[0]))
		if err != nil {
			return fmt.Errorf("failed to connect to storage server %s: %w", replicaAddrs[0], err)
		}
		if err := ctClient.SendChunk(ctx, chunkID, "client-1", datasetID, replicaAddrs, finalData); err != nil {
			_ = ctClient.Close()
			return fmt.Errorf("failed to send chunk %s: %w", chunkID, err)
		}
		_ = ctClient.Close()
		fmt.Printf("uploaded chunk_id=%s chunk_index=%d\n", chunkID, i)
	}

	return nil
}
