package chunkmap

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/chunkmap"
	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	dbName         = "chunkmap.db"
	chunksBucket   = "chunks"
	datasetsBucket = "datasets"
	serversBucket  = "servers"
)

var (
	ErrChunkNotFound      = errors.New("chunk not found")
	ErrChunkAlreadyExists = errors.New("chunk already exists")
	ErrNotSealed          = errors.New("chunk is not sealed")
	ErrHasActiveReaders   = errors.New("chunk has active readers")
	ErrServerNotFound     = errors.New("server not found")
)

// Engine is the chunk-server mapping store.
// All records live in a Go map (in-memory) for nanosecond reads.
// Every mutation is also written to BoltDB so the map can be rebuilt after a restart.
type Engine struct {
	mu       sync.RWMutex
	chunks   map[string]*pb.ChunkRecord
	datasets map[string]*pb.DatasetRecord
	servers  map[string]*pb.ServerRecord
	db       *bbolt.DB
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
		if _, err := tx.CreateBucketIfNotExists([]byte(chunksBucket)); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(datasetsBucket)); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists([]byte(serversBucket)); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	e := &Engine{
		chunks: make(map[string]*pb.ChunkRecord),
		db:     db, datasets: make(map[string]*pb.DatasetRecord),
		servers: make(map[string]*pb.ServerRecord),
	}
	e.datasets = make(map[string]*pb.DatasetRecord)
	e.servers = make(map[string]*pb.ServerRecord)
	if err := e.loadFromDB(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return e, nil
}

// loadFromDB deserialises every record from BoltDB into the in-memory map.
func (e *Engine) loadFromDB() error {
	return e.db.View(func(tx *bbolt.Tx) error {
		// load chunks
		if b := tx.Bucket([]byte(chunksBucket)); b != nil {
			if err := b.ForEach(func(_, v []byte) error {
				record := &pb.ChunkRecord{}
				if err := proto.Unmarshal(v, record); err != nil {
					return err
				}
				e.chunks[record.ChunkId] = record
				return nil
			}); err != nil {
				return err
			}
		}
		// load datasets
		if b := tx.Bucket([]byte(datasetsBucket)); b != nil {
			if err := b.ForEach(func(_, v []byte) error {
				ds := &pb.DatasetRecord{}
				if err := proto.Unmarshal(v, ds); err != nil {
					return err
				}
				e.datasets[ds.DatasetId] = ds
				return nil
			}); err != nil {
				return err
			}
		}
		// load servers
		if b := tx.Bucket([]byte(serversBucket)); b != nil {
			if err := b.ForEach(func(_, v []byte) error {
				sr := &pb.ServerRecord{}
				if err := proto.Unmarshal(v, sr); err != nil {
					return err
				}
				e.servers[sr.ServerId] = sr
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

func keyFromString(s string) []byte {
	return []byte(s)
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
		return b.Put(keyFromString(record.ChunkId), data)
	})
}

func (e *Engine) persistDataset(ds *pb.DatasetRecord) error {
	return e.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(datasetsBucket))
		data, err := proto.Marshal(ds)
		if err != nil {
			return err
		}
		return b.Put(keyFromString(ds.DatasetId), data)
	})
}

// CreateDataset persists a dataset record and stores it in-memory.
func (e *Engine) CreateDataset(ds *pb.DatasetRecord) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.datasets[ds.DatasetId]; exists {
		return nil
	}
	if err := e.persistDataset(ds); err != nil {
		return err
	}
	e.datasets[ds.DatasetId] = ds
	return nil
}

// GetDataset returns a dataset record and a boolean indicating existence.
func (e *Engine) GetDataset(datasetID string) (*pb.DatasetRecord, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	ds, ok := e.datasets[datasetID]
	return ds, ok
}

// CreateOrUpdateServer persists a server record and updates the in-memory registry.
func (e *Engine) CreateOrUpdateServer(sr *pb.ServerRecord) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.persistServer(sr); err != nil {
		return err
	}
	e.servers[sr.ServerId] = sr
	return nil
}

func (e *Engine) persistServer(sr *pb.ServerRecord) error {
	return e.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(serversBucket))
		data, err := proto.Marshal(sr)
		if err != nil {
			return err
		}
		return b.Put(keyFromString(sr.ServerId), data)
	})
}

// InsertChunk registers a brand-new chunk in PENDING state.
// The master calls this before it decides which storage servers to use.
func (e *Engine) InsertChunk(chunkID string, clientID, datasetID string, chunkIndex uint32) error {
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
		CreatedAt:  timestamppb.Now(),
	}

	if err := e.persist(record); err != nil {
		return err
	}
	e.chunks[chunkID] = record
	return nil
}

// AssignReplicas tells the engine which servers will hold this chunk and marks
func (e *Engine) AssignReplicas(chunkID string, serverIDs []string, primaryServerID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	record, exists := e.chunks[chunkID]
	if !exists {
		return ErrChunkNotFound
	}

	// Build replicas list from known servers registry when available
	var recs []*pb.ServerRecord
	for _, sid := range serverIDs {
		if s, ok := e.servers[sid]; ok {
			recs = append(recs, s)
		} else {
			// unknown server id — create placeholder with id only
			recs = append(recs, &pb.ServerRecord{ServerId: sid})
		}
	}
	record.Replicas = recs
	record.PrimaryServerId = primaryServerID
	record.Status = pb.ChunkStatus_UPLOADING

	return e.persist(record)
}

// ConfirmReplica records that a specific storage server has successfully
// persisted the chunk. Idempotent — safe to call more than once per server.
func (e *Engine) ConfirmReplica(chunkID string, serverID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	record, exists := e.chunks[chunkID]
	if !exists {
		return ErrChunkNotFound
	}

	for _, s := range record.ConfirmedReplicaIds {
		if s == serverID {
			return nil
		}
	}
	record.ConfirmedReplicaIds = append(record.ConfirmedReplicaIds, serverID)
	return e.persist(record)
}

// SealChunk marks the chunk as fully written and readable.
// The master calls this once enough replicas have confirmed.
func (e *Engine) SealChunk(chunkID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	record, exists := e.chunks[chunkID]
	if !exists {
		return ErrChunkNotFound
	}

	record.Status = pb.ChunkStatus_SEALED
	record.SealedAt = timestamppb.Now()

	return e.persist(record)
}

// GetChunk returns a deep copy of the full record for a chunk.
// A copy is returned so callers cannot accidentally mutate in-memory state.
func (e *Engine) GetChunk(chunkID string) (*pb.ChunkRecord, error) {
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
func (e *Engine) GetServers(chunkID string) ([]string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	record, exists := e.chunks[chunkID]
	if !exists {
		return nil, ErrChunkNotFound
	}
	if record.Status != pb.ChunkStatus_SEALED {
		return nil, ErrNotSealed
	}

	var addrs []string
	// prefer confirmed replica ids
	if len(record.ConfirmedReplicaIds) > 0 {
		for _, sid := range record.ConfirmedReplicaIds {
			if s, ok := e.servers[sid]; ok && s.Address != "" {
				addrs = append(addrs, s.Address)
			}
		}
		if len(addrs) > 0 {
			return addrs, nil
		}
	}
	// fallback to replicas list
	for _, r := range record.Replicas {
		if r != nil && r.Address != "" {
			addrs = append(addrs, r.Address)
		}
	}
	return addrs, nil
}

// MarkCorrupted removes a server from the confirmed replicas list.
// If no confirmed replicas remain the chunk status becomes CORRUPTED,
// signalling that re-replication or recovery is required.
func (e *Engine) MarkCorrupted(chunkID string, serverID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	record, exists := e.chunks[chunkID]
	if !exists {
		return ErrChunkNotFound
	}

	filtered := make([]string, 0, len(record.ConfirmedReplicaIds))
	for _, s := range record.ConfirmedReplicaIds {
		if s != serverID {
			filtered = append(filtered, s)
		}
	}
	record.ConfirmedReplicaIds = filtered

	if len(record.ConfirmedReplicaIds) == 0 {
		record.Status = pb.ChunkStatus_CORRUPTED
	}

	return e.persist(record)
}

// ListServers returns a copy of all server records.
func (e *Engine) ListServers() []*pb.ServerRecord {
	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make([]*pb.ServerRecord, 0, len(e.servers))
	for _, s := range e.servers {
		out = append(out, proto.Clone(s).(*pb.ServerRecord))
	}
	return out
}

// ListChunks returns a copy of all chunk records.
func (e *Engine) ListChunks() []*pb.ChunkRecord {
	e.mu.RLock()
	defer e.mu.RUnlock()

	out := make([]*pb.ChunkRecord, 0, len(e.chunks))
	for _, c := range e.chunks {
		out = append(out, proto.Clone(c).(*pb.ChunkRecord))
	}
	return out
}

// UpdateServerStatus updates the status of a known server record.
func (e *Engine) UpdateServerStatus(serverID string, status pb.ServerStatus) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	sr, ok := e.servers[serverID]
	if !ok {
		return ErrServerNotFound
	}
	sr.Status = status
	return e.persistServer(sr)
}

// RemoveReplicaFromChunk removes a server from replica and confirmed lists.
func (e *Engine) RemoveReplicaFromChunk(chunkID string, serverID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	record, exists := e.chunks[chunkID]
	if !exists {
		return ErrChunkNotFound
	}

	filteredReplicas := make([]*pb.ServerRecord, 0, len(record.Replicas))
	for _, r := range record.Replicas {
		if r != nil && r.ServerId == serverID {
			continue
		}
		filteredReplicas = append(filteredReplicas, r)
	}
	record.Replicas = filteredReplicas

	filteredConfirmed := make([]string, 0, len(record.ConfirmedReplicaIds))
	for _, s := range record.ConfirmedReplicaIds {
		if s == serverID {
			continue
		}
		filteredConfirmed = append(filteredConfirmed, s)
	}
	record.ConfirmedReplicaIds = filteredConfirmed

	if record.PrimaryServerId == serverID {
		record.PrimaryServerId = ""
		for _, r := range record.Replicas {
			if r != nil && r.ServerId != "" {
				record.PrimaryServerId = r.ServerId
				break
			}
		}
	}

	if len(record.ConfirmedReplicaIds) == 0 {
		record.Status = pb.ChunkStatus_CORRUPTED
	}

	return e.persist(record)
}

// AddReplica adds a new replica server to the chunk if missing.
func (e *Engine) AddReplica(chunkID string, server *pb.ServerRecord) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	record, exists := e.chunks[chunkID]
	if !exists {
		return ErrChunkNotFound
	}

	for _, r := range record.Replicas {
		if r != nil && r.ServerId == server.ServerId {
			return nil
		}
	}

	record.Replicas = append(record.Replicas, server)
	return e.persist(record)
}

// Close flushes and closes the underlying BoltDB file.
func (e *Engine) Close() error {
	return e.db.Close()
}
