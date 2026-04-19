package chunkmap

import (
	"testing"

	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/chunkmap"
)

// newTestEngine creates an Engine backed by a temporary directory that is
// automatically cleaned up when the test finishes.
func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := NewEngine(t.TempDir())
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	t.Cleanup(func() { e.Close() })
	return e
}

// sealChunk runs the full write lifecycle so other tests start from a known SEALED state.
func sealChunk(t *testing.T, e *Engine, chunkID uint32) {
	t.Helper()
	servers := []string{"server-a:50051", "server-b:50051"}
	e.InsertChunk(chunkID, "test-client", "test-dataset", 0)
	e.AssignReplicas(chunkID, servers, servers[0])
	e.ConfirmReplica(chunkID, servers[0])
	e.ConfirmReplica(chunkID, servers[1])
	e.SealChunk(chunkID, 2*1024*1024, []uint32{111, 222})
}

func TestInsertChunk(t *testing.T) {
	e := newTestEngine(t)

	if err := e.InsertChunk(1, "alice", "ds1", 0); err != nil {
		t.Fatalf("InsertChunk: %v", err)
	}

	// Inserting the same chunkID again must fail
	if err := e.InsertChunk(1, "alice", "ds1", 0); err != ErrChunkAlreadyExists {
		t.Errorf("want ErrChunkAlreadyExists, got %v", err)
	}
}

func TestFullLifecycle(t *testing.T) {
	e := newTestEngine(t)
	servers := []string{"server-a:50051", "server-b:50051"}

	e.InsertChunk(1, "alice", "ds1", 0)

	// Chunk must not be readable until it is SEALED
	if _, err := e.GetServers(1); err != ErrNotSealed {
		t.Errorf("want ErrNotSealed before sealing, got %v", err)
	}

	e.AssignReplicas(1, servers, servers[0])

	// Confirm both replicas
	for _, s := range servers {
		if err := e.ConfirmReplica(1, s); err != nil {
			t.Fatalf("ConfirmReplica(%s): %v", s, err)
		}
	}
	// ConfirmReplica is idempotent
	if err := e.ConfirmReplica(1, servers[0]); err != nil {
		t.Errorf("duplicate ConfirmReplica should be a no-op, got %v", err)
	}

	if err := e.SealChunk(1, 2*1024*1024, []uint32{111, 222}); err != nil {
		t.Fatalf("SealChunk: %v", err)
	}

	got, err := e.GetServers(1)
	if err != nil {
		t.Fatalf("GetServers: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("want 2 confirmed servers, got %d", len(got))
	}
}

func TestReadLockBlocksDeletion(t *testing.T) {
	e := newTestEngine(t)
	sealChunk(t, e, 1)

	if err := e.AcquireReadLock(1); err != nil {
		t.Fatalf("AcquireReadLock: %v", err)
	}

	// Delete must be refused while a reader holds the lock
	if err := e.DeleteChunk(1); err != ErrHasActiveReaders {
		t.Errorf("want ErrHasActiveReaders, got %v", err)
	}

	e.ReleaseReadLock(1)

	// Now deletion must succeed
	if err := e.DeleteChunk(1); err != nil {
		t.Fatalf("DeleteChunk after releasing lock: %v", err)
	}
	if _, err := e.GetChunk(1); err != ErrChunkNotFound {
		t.Errorf("want ErrChunkNotFound after delete, got %v", err)
	}
}

func TestMarkCorrupted(t *testing.T) {
	e := newTestEngine(t)
	sealChunk(t, e, 1)

	// Lose one replica — chunk stays SEALED because one replica remains
	if err := e.MarkCorrupted(1, "server-a:50051"); err != nil {
		t.Fatalf("MarkCorrupted: %v", err)
	}
	r, _ := e.GetChunk(1)
	if r.Status != pb.ChunkStatus_SEALED {
		t.Errorf("want SEALED with one replica remaining, got %v", r.Status)
	}

	// Lose last replica — chunk must become CORRUPTED
	e.MarkCorrupted(1, "server-b:50051")
	r, _ = e.GetChunk(1)
	if r.Status != pb.ChunkStatus_CORRUPTED {
		t.Errorf("want CORRUPTED when no replicas remain, got %v", r.Status)
	}
}

func TestPersistenceAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	// First engine instance — write a record then close
	e1, _ := NewEngine(dir)
	e1.InsertChunk(42, "bob", "ds2", 3)
	e1.Close()

	// Second engine instance — record must survive the restart
	e2, err := NewEngine(dir)
	if err != nil {
		t.Fatalf("reopen engine: %v", err)
	}
	defer e2.Close()

	r, err := e2.GetChunk(42)
	if err != nil {
		t.Fatalf("GetChunk after restart: %v", err)
	}
	if r.ClientId != "bob" || r.DatasetId != "ds2" || r.ChunkIndex != 3 {
		t.Errorf("unexpected record after restart: %+v", r)
	}
	// ActiveReaders must always reset to 0 on restart
	if r.ActiveReaders != 0 {
		t.Errorf("ActiveReaders must be 0 after restart, got %d", r.ActiveReaders)
	}
}

