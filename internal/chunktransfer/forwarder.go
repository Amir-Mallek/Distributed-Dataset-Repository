package chunktransfer

import (
	"context"
	"fmt"
	"sync"

	pb "github.com/Amir-Mallek/Distributed-Dataset-Repository/api/chunktransfer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type AsyncForwarder struct {
	address string
	conn    *grpc.ClientConn
	stream  pb.ChunkTransferService_WriteChunkClient
	blocks  chan *pb.ChunkBlock
	done    chan error
	once    sync.Once
}

func cloneBlock(block *pb.ChunkBlock) *pb.ChunkBlock {
	dataCopy := make([]byte, len(block.Data))
	copy(dataCopy, block.Data)
	return &pb.ChunkBlock{Data: dataCopy, Checksum: block.Checksum}
}

func (f *AsyncForwarder) start() {
	go func() {
		for block := range f.blocks {
			err := f.stream.Send(&pb.WriteChunkRequest{Msg: &pb.WriteChunkRequest_Block{Block: block}})
			if err != nil {
				f.done <- fmt.Errorf("failed to forward block to server %s: %v", f.address, err)
				close(f.done)
				return
			}
		}

		if _, err := f.stream.CloseAndRecv(); err != nil {
			f.done <- fmt.Errorf("failed to close forwarding block to server %s: %v", f.address, err)
			close(f.done)
			return
		}

		f.done <- nil
		close(f.done)
	}()
}

func (f *AsyncForwarder) Send(block *pb.ChunkBlock) error {
	select {
	case err := <-f.done:
		if err != nil {
			return err
		}
		return fmt.Errorf("forwarder to replica %s closed unexpectedly", f.address)
	case f.blocks <- cloneBlock(block):
		return nil
	}
}

func (f *AsyncForwarder) CloseAndWait() error {
	f.once.Do(func() {
		close(f.blocks)
	})
	err := <-f.done
	_ = f.conn.Close()
	return err
}

func (f *AsyncForwarder) Abort() {
	f.once.Do(func() {
		close(f.blocks)
	})
	_ = f.conn.Close()
}

func (s *Server) NewForwarder(ctx context.Context, meta *pb.ChunkMetadata) (*AsyncForwarder, error) {
	nextAddr := meta.ReplicaSet[0]
	remaining := meta.ReplicaSet[1:]
	if len(remaining) == 0 {
		return nil, nil
	}

	conn, err := grpc.NewClient(nextAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "failed to connect to replica %s: %v", nextAddr, err)
	}

	client := pb.NewChunkTransferServiceClient(conn)
	fwdStream, err := client.WriteChunk(ctx)
	if err != nil {
		_ = conn.Close()
		return nil, status.Errorf(codes.Unavailable, "failed to open write stream to replica %s: %v", nextAddr, err)
	}

	metaCopy := &pb.ChunkMetadata{
		ChunkId:    meta.ChunkId,
		ClientId:   meta.ClientId,
		DatasetId:  meta.DatasetId,
		ReplicaSet: remaining,
	}
	if err := fwdStream.Send(&pb.WriteChunkRequest{Msg: &pb.WriteChunkRequest_Meta{Meta: metaCopy}}); err != nil {
		_ = conn.Close()
		return nil, status.Errorf(codes.Unavailable, "failed to send metadata to replica %s: %v", nextAddr, err)
	}

	forwarder := &AsyncForwarder{
		address: nextAddr,
		conn:    conn,
		stream:  fwdStream,
		blocks:  make(chan *pb.ChunkBlock, 16),
		done:    make(chan error, 1),
	}
	forwarder.start()

	return forwarder, nil
}
