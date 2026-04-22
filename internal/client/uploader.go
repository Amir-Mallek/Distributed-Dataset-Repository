package client
package client

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	masterpb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/master"
)

const (
	DefaultChunkSize = 64 * 1024 * 1024 
)

type FileUploader struct {
	masterClient masterpb.MasterServiceClient
}

func NewFileUploader(mc masterpb.MasterServiceClient) *FileUploader {
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


	for i := uint32(0); i < uint32(numChunks); i++ {
		servers, err := u.getServersForChunk(ctx, fileName, i)
		if err != nil {
			return fmt.Errorf("chunk %d: master failed: %w", i, err)
		}

		offset := int64(i) * DefaultChunkSize
		chunkData := make([]byte, DefaultChunkSize)
		n, err := file.ReadAt(chunkData, offset)
		if err != nil && err != io.EOF {
			return err
		}
		
		finalData := chunkData[:n]

		
		// TODO: call the stserver gRPC 
		// to actually transfer finalData to servers.ServerAddresses
	}

	return nil
}

func (u *FileUploader) getServersForChunk(ctx context.Context, name string, index uint32) (*masterpb.GetServersResponse, error) {
	return u.masterClient.GetServers(ctx, &masterpb.GetServersRequest{
		FileName:          name,
		ChunkIndex:        index,
		ReplicationFactor: 3,
	})
}