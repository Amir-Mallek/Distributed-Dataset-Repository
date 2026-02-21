package storage

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"go.etcd.io/bbolt"
)

var (
	ErrChunkNotFound      = errors.New("chunk not found")
	ErrChunkAlreadyExists = errors.New("chunk already exists")
)

// ChunkMeta is what we store in BoltDB as JSON
type ChunkMeta struct {
	ChunkID        uint32            `json:"chunk_id"`
	ClientID       string            `json:"client_id"`  
	DatasetID      string            `json:"dataset_id"` 
	FilePath       string            `json:"file_path"`
	Status         string            `json:"status"` // "Writing" or "Sealed"
	BlockChecksums map[uint32]uint32 `json:"block_checksums"` // blockIndex -> checksum
}

type DiskEngine struct {
	db      *bbolt.DB
	dataDir string
}

// NewDiskEngine initializes the DB and base data directory
func NewDiskEngine(baseDir string) (*DiskEngine, error) {
	err := os.MkdirAll(filepath.Join(baseDir, "chunks"), 0755)
	if err != nil {
		return nil, err
	}

	dbPath := filepath.Join(baseDir, "meta.db")
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, err
	}

	// bucket is a table-like structure in BoltDB
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("chunks"))
		return err
	})

	return &DiskEngine{
		db:      db,
		dataDir: filepath.Join(baseDir, "chunks"),
	}, err
}

func uint32ToBytes(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// CreateChunk builds the sharded directory structure and reserves the DB entry
func (e *DiskEngine) CreateChunk(chunkID uint32, clientID string, datasetID string) error {
	return e.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("chunks"))
		key := uint32ToBytes(chunkID)

		if b.Get(key) != nil {
			return ErrChunkAlreadyExists
		}

		shardDir := filepath.Join(e.dataDir, clientID, datasetID)
		if err := os.MkdirAll(shardDir, 0755); err != nil {
			return err
		}

		filePath := filepath.Join(shardDir, fmt.Sprintf("chunk_%d.dat", chunkID))
		
		f, err := os.Create(filePath)
		if err != nil {
			return err
		}
		f.Close()

		meta := ChunkMeta{
			ChunkID:        chunkID,
			ClientID:       clientID,
			DatasetID:      datasetID,
			FilePath:       filePath,
			Status:         "Writing",
			BlockChecksums: make(map[uint32]uint32),
		}

		metaBytes, _ := json.Marshal(meta)
		return b.Put(key, metaBytes)
	})
}

// WriteBlock executes parallel file I/O and atomic database updates
func (e *DiskEngine) WriteBlock(chunkID uint32, blockIndex uint32, data []byte, checksum uint32) error {
	
	var filePath string
	err := e.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("chunks"))
		val := b.Get(uint32ToBytes(chunkID))
		if val == nil {
			return ErrChunkNotFound
		}
		var meta ChunkMeta
		if err := json.Unmarshal(val, &meta); err != nil {
			return err
		}
		filePath = meta.FilePath
		return nil
	})
	if err != nil {
		return err
	}

	f, err := os.OpenFile(filePath, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	
	blockSize := int64(64 * 1024)
	offset := int64(blockIndex) * blockSize
	
	_, writeErr := f.WriteAt(data, offset)
	f.Close() 
	
	if writeErr != nil {
		return writeErr
	}

	
	// BoltDB guarantees this block executes sequentially across threads
	return e.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("chunks"))
		val := b.Get(uint32ToBytes(chunkID))
		if val == nil {
			return ErrChunkNotFound
		}
		
		var meta ChunkMeta
		if err := json.Unmarshal(val, &meta); err != nil {
			return err
		}

		// Update the checksum for this specific block
		meta.BlockChecksums[blockIndex] = checksum
		
		metaBytes, _ := json.Marshal(meta)
		return b.Put(uint32ToBytes(chunkID), metaBytes)
	})
}

// GetChunkForRead fetches metadata so the gRPC server can stream the file back
func (e *DiskEngine) GetChunkForRead(chunkID uint32) (ChunkMeta, error) {
	var meta ChunkMeta
	err := e.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("chunks"))
		val := b.Get(uint32ToBytes(chunkID))
		if val == nil {
			return ErrChunkNotFound
		}
		return json.Unmarshal(val, &meta)
	})
	return meta, err
}