package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/storage"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

const (
	chunksDirName   = "chunks"
	metaDbName      = "meta.db"
	chunksBucket    = "chunks"
	chunkFileFormat = "chunk_%d.dat"
	blockSize       = 64 * 1024
)

var (
	ErrChunkNotFound      = errors.New("chunk not found")
	ErrChunkAlreadyExists = errors.New("chunk already exists")
)

type DiskEngine struct {
	db      *bbolt.DB
	dataDir string
}

func (e *DiskEngine) getFilePath(clientID, datasetID string, chunkID uint32) string {
	fileName := fmt.Sprintf(chunkFileFormat, chunkID)
	return filepath.Join(e.dataDir, clientID, datasetID, fileName)
}

// NewDiskEngine initializes the DB and base directory
func NewDiskEngine(baseDir string) (*DiskEngine, error) {
	dataDir := filepath.Join(baseDir, chunksDirName)
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	dbPath := filepath.Join(baseDir, metaDbName)
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(chunksBucket))
		return err
	})

	return &DiskEngine{
		db:      db,
		dataDir: dataDir,
	}, err
}

func uint32ToBytes(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// CreateChunk: Uses Protobuf binary marshalling
func (e *DiskEngine) CreateChunk(chunkID uint32, clientID string, datasetID string, totalSize uint64) error {
	return e.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(chunksBucket))
		key := uint32ToBytes(chunkID)

		if b.Get(key) != nil {
			return ErrChunkAlreadyExists
		}

		shardDir := filepath.Join(e.dataDir, clientID, datasetID)
		if err := os.MkdirAll(shardDir, 0755); err != nil {
			return err
		}

		f, err := os.Create(e.getFilePath(clientID, datasetID, chunkID))
		if err != nil {
			return err
		}
		f.Close()

		meta := &pb.ChunkMetaProto{
			ChunkId:        chunkID,
			ClientId:       clientID,
			DatasetId:      datasetID,
			Status:         "Writing",
			TotalSize:      totalSize,
			BlockChecksums: make(map[uint32]uint32),
		}

		metaBytes, err := proto.Marshal(meta)
		if err != nil {
			return err
		}
		return b.Put(key, metaBytes)
	})
}

func (e *DiskEngine) WriteBlock(chunkID uint32, blockIndex uint32, data []byte, checksum uint32, filePath string) error {

	f, err := os.OpenFile(filePath, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	offset := int64(blockIndex) * int64(blockSize)
	_, writeErr := f.WriteAt(data, offset)
	f.Close()

	if writeErr != nil {
		return writeErr
	}

	return e.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(chunksBucket))
		val := b.Get(uint32ToBytes(chunkID))

		meta := &pb.ChunkMetaProto{}
		if err := proto.Unmarshal(val, meta); err != nil {
			return err
		}

		meta.BlockChecksums[blockIndex] = checksum

		metaBytes, err := proto.Marshal(meta)
		if err != nil {
			return err
		}
		return b.Put(uint32ToBytes(chunkID), metaBytes)
	})
}

func (e *DiskEngine) GetChunkForRead(chunkID uint32) (*pb.ChunkMetaProto, error) {
	meta := &pb.ChunkMetaProto{}
	err := e.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(chunksBucket))
		val := b.Get(uint32ToBytes(chunkID))
		if val == nil {
			return ErrChunkNotFound
		}
		return proto.Unmarshal(val, meta)
	})
	return meta, err
}

func (e *DiskEngine) SealChunk(chunkID uint32) error {
	return e.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(chunksBucket))
		val := b.Get(uint32ToBytes(chunkID))
		if val == nil {
			return ErrChunkNotFound
		}

		meta := &pb.ChunkMetaProto{}
		proto.Unmarshal(val, meta)

		expectedBlocks := (meta.TotalSize + blockSize - 1) / blockSize

		if uint32(len(meta.BlockChecksums)) != uint32(expectedBlocks) {
			return fmt.Errorf("missing blocks: have %d, want %d",
				len(meta.BlockChecksums), expectedBlocks)
		}

		filePath := e.getFilePath(meta.ClientId, meta.DatasetId, meta.ChunkId)
		info, err := os.Stat(filePath)
		if err != nil {
			return err
		}

		if uint64(info.Size()) != meta.TotalSize {
			return fmt.Errorf("integrity fail: expected %d bytes, got %d",
				meta.TotalSize, info.Size())
		}

		meta.Status = "Sealed"
		metaBytes, _ := proto.Marshal(meta)
		return b.Put(uint32ToBytes(chunkID), metaBytes)
	})
}
