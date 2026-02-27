package metastorage

import (
	"encoding/binary"
	"errors"
	"path/filepath"

	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/metastorage"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

const (
	metaDbName   = "meta.db"
	chunksBucket = "chunks"
)

var (
	ErrChunkNotFound      = errors.New("chunk not found")
	ErrChunkAlreadyExists = errors.New("chunk already exists")
)

type DiskEngine struct {
	db *bbolt.DB
}

func NewDiskEngine(baseDir string) (*DiskEngine, error) {
	dbPath := filepath.Join(baseDir, metaDbName)
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(chunksBucket))
		return err
	})
	if err != nil {
		return nil, err
	}

	return &DiskEngine{
		db: db,
	}, nil
}

func uint32ToBytes(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func (e *DiskEngine) CreateChunk(chunkID uint32, clientID string, datasetID string, totalSize uint64) error {
	return e.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(chunksBucket))
		key := uint32ToBytes(chunkID)

		if b.Get(key) != nil {
			return ErrChunkAlreadyExists
		}

		meta := &pb.ChunkMetaProto{
			ChunkId:   chunkID,
			ClientId:  clientID,
			DatasetId: datasetID,
			Status:    pb.ChunkStatus_CREATED,
			TotalSize: totalSize,
		}

		metaBytes, err := proto.Marshal(meta)
		if err != nil {
			return err
		}
		return b.Put(key, metaBytes)
	})
}

func (e *DiskEngine) SealChunk(chunkID uint32, blockChecksums []uint32) error {
	return e.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(chunksBucket))
		val := b.Get(uint32ToBytes(chunkID))
		if val == nil {
			return ErrChunkNotFound
		}

		meta := &pb.ChunkMetaProto{}
		if err := proto.Unmarshal(val, meta); err != nil {
			return err
		}
		meta.BlockChecksums = blockChecksums
		meta.Status = pb.ChunkStatus_SEALED

		metaBytes, _ := proto.Marshal(meta)
		return b.Put(uint32ToBytes(chunkID), metaBytes)
	})
}

func (e *DiskEngine) GetChunkMetadata(chunkID uint32) (*pb.ChunkMetaProto, error) {
	meta := &pb.ChunkMetaProto{}
	err := e.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(chunksBucket))
		val := b.Get(uint32ToBytes(chunkID))
		if val == nil {
			return ErrChunkNotFound
		}
		return proto.Unmarshal(val, meta)
	})
	if err != nil {
		return nil, err
	}
	return meta, nil
}
