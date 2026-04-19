package chunkmap

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/chunkmap"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	dbName        = "chunkmap.db"
	chunksBucket  = "chunks"
	uploadTimeout = 5 * time.Minute
)

var (
	ErrChunkNotFound      = errors.New("chunk not found")
	ErrChunkAlreadyExists = errors.New("chunk already exists")
	ErrNotSealed          = errors.New("chunk is not sealed")
	ErrHasActiveReaders   = errors.New("chunk has active readers")
)

// Engine is the chunk-server mapping store.
// All records live in a Go map (in-memory) for nanosecond reads.
// Every mutation is also written to BoltDB so the map can be rebuilt after a restart.
type Engine struct {
	mu     sync.RWMutex
	chunks map[uint32]*pb.ChunkRecord
	db     *bbolt.DB
}

// NewEngine opens (or creates) the BoltDB file inside baseDir and loads all
// persisted chunk records into the in-memory map.
func NewEngine(baseDir string) (*Engine, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}
	dbPath := filepath.Join(baseDir, dbName)
	db, err := bbolt.Open(dbPath, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(chunksBucket))
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	e := &Engine{chunks: make(map[uint32]*pb.ChunkRecord), db: db}
	if err := e.loadFromDB(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return e, nil
}

// loadFromDB deserialises every record from BoltDB into the in-memory map.
// ActiveReaders is NOT restored (it resets to 0 on every restart by design).
func (e *Engine) loadFromDB() error {
	return e.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(chunksBucket))
		return b.ForEach(func(_, v []byte) error {
			record := &pb.ChunkRecord{}
			if err := proto.Unmarshal(v, record); err != nil {
				return err
			}
			record.ActiveReaders = 0 // always reset on restart
			e.chunks[record.ChunkId] = record
			return nil
		})
	})
}

func uint32ToBytes(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// persist serialises a record and writes it to BoltDB.
// Must be called while the engine write-lock is held.
func (e *Engine) persist(record *pb.ChunkRecord) error {
	return e.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(chunksBucket))
		data, err := proto.Marshal(record)
		if err != nil {
			return err
		}
		return b.Put(uint32ToBytes(record.ChunkId), data)
	})
}

// InsertChunk registers a brand-new chunk in PENDING state.
// The master calls this before it decides which storage servers to use.
func (e *Engine) InsertChunk(chunkID uint32, clientID, datasetID string, chunkIndex uint32) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.chunks[chunkID]; exists {
		return ErrChunkAlreadyExists
	}

	record := &pb.ChunkRecord{
		ChunkId:    chunkID,
		ClientId:   clientID,
		DatasetId:  datasetID,
		ChunkIndex: chunkIndex,
		Status:     pb.ChunkStatus_PENDING,
		Version:    1,
		CreatedAt:  timestamppb.Now(),
	}

	if err := e.persist(record); err != nil {
		return err
	}
	e.chunks[chunkID] = record
	return nil
}

// AssignReplicas tells the engine which servers will hold this chunk and marks
// it as UPLOADING. A deadline is set so stale uploads can be garbage-collected.
func (e *Engine) AssignReplicas(chunkID uint32, servers []string, primary string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	record, exists := e.chunks[chunkID]
	if !exists {
		return ErrChunkNotFound
	}

	record.Replicas = servers
	record.PrimaryServer = primary
	record.Status = pb.ChunkStatus_UPLOADING
	record.WriteLocked = true
	record.UploadDeadline = timestamppb.New(time.Now().Add(uploadTimeout))
	record.Version++

	return e.persist(record)
}

// ConfirmReplica records that a specific storage server has successfully
// persisted the chunk. Idempotent — safe to call more than once per server.
func (e *Engine) ConfirmReplica(chunkID uint32, serverAddr string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	record, exists := e.chunks[chunkID]
	if !exists {
		return ErrChunkNotFound
	}

	for _, s := range record.ConfirmedReplicas {
		if s == serverAddr {
			return nil // already confirmed, nothing to do
		}
	}

	record.ConfirmedReplicas = append(record.ConfirmedReplicas, serverAddr)
	record.Version++
	return e.persist(record)
}

// SealChunk marks the chunk as fully written and readable.
// The master calls this once enough replicas have confirmed.
func (e *Engine) SealChunk(chunkID uint32, actualSize uint32, blockChecksums []uint32) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	record, exists := e.chunks[chunkID]
	if !exists {
		return ErrChunkNotFound
	}

	record.Status = pb.ChunkStatus_SEALED
	record.WriteLocked = false
	record.ActualSize = actualSize
	record.BlockChecksums = blockChecksums
	record.SealedAt = timestamppb.Now()
	record.Version++

	return e.persist(record)
}

// GetChunk returns a deep copy of the full record for a chunk.
// A copy is returned so callers cannot accidentally mutate in-memory state.
func (e *Engine) GetChunk(chunkID uint32) (*pb.ChunkRecord, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	record, exists := e.chunks[chunkID]
	if !exists {
		return nil, ErrChunkNotFound
	}
	return proto.Clone(record).(*pb.ChunkRecord), nil
}

// GetServers returns the confirmed replica addresses for routing a read request.
// Only works on SEALED chunks.
func (e *Engine) GetServers(chunkID uint32) ([]string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	record, exists := e.chunks[chunkID]
	if !exists {
		return nil, ErrChunkNotFound
	}
	if record.Status != pb.ChunkStatus_SEALED {
		return nil, ErrNotSealed
	}

	servers := make([]string, len(record.ConfirmedReplicas))
	copy(servers, record.ConfirmedReplicas)
	return servers, nil
}

// AcquireReadLock increments the active reader counter.
// Prevents the chunk from being deleted while a client is reading it.
// Not persisted — resets to 0 on master restart by design.
func (e *Engine) AcquireReadLock(chunkID uint32) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	record, exists := e.chunks[chunkID]
	if !exists {
		return ErrChunkNotFound
	}
	if record.Status != pb.ChunkStatus_SEALED {
		return ErrNotSealed
	}

	record.ActiveReaders++
	record.LastAccessedAt = timestamppb.Now()
	return nil
}

// ReleaseReadLock decrements the active reader counter.
func (e *Engine) ReleaseReadLock(chunkID uint32) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	record, exists := e.chunks[chunkID]
	if !exists {
		return ErrChunkNotFound
	}
	if record.ActiveReaders > 0 {
		record.ActiveReaders--
	}
	return nil
}

// MarkCorrupted removes a server from the confirmed replicas list.
// If no confirmed replicas remain the chunk status becomes CORRUPTED,
// signalling that re-replication or recovery is required.
func (e *Engine) MarkCorrupted(chunkID uint32, serverAddr string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	record, exists := e.chunks[chunkID]
	if !exists {
		return ErrChunkNotFound
	}

	filtered := make([]string, 0, len(record.ConfirmedReplicas))
	for _, s := range record.ConfirmedReplicas {
		if s != serverAddr {
			filtered = append(filtered, s)
		}
	}
	record.ConfirmedReplicas = filtered
	record.Version++

	if len(record.ConfirmedReplicas) == 0 {
		record.Status = pb.ChunkStatus_CORRUPTED
	}

	return e.persist(record)
}

// DeleteChunk removes the chunk record from both the in-memory map and BoltDB.
// Refused while any reader holds a lock.
func (e *Engine) DeleteChunk(chunkID uint32) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	record, exists := e.chunks[chunkID]
	if !exists {
		return ErrChunkNotFound
	}
	if record.ActiveReaders > 0 {
		return ErrHasActiveReaders
	}

	delete(e.chunks, chunkID)
	return e.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket([]byte(chunksBucket)).Delete(uint32ToBytes(chunkID))
	})
}

// Close flushes and closes the underlying BoltDB file.
func (e *Engine) Close() error {
	return e.db.Close()
}
