package master

import (
	"errors"
	"sync"
)

type StorageNode struct {
	ID      string
	Address string
	IsAlive bool
}

type Distributor interface {
	SelectServers(count int, exclude []string) ([]StorageNode, error)
}

type RoundRobinDistributor struct {
	mu      sync.Mutex
	nodes   []StorageNode
	lastIdx int
}

func NewRoundRobinDistributor(nodes []StorageNode) *RoundRobinDistributor {
	return &RoundRobinDistributor{nodes: nodes}
}

// AddOrUpdateNode adds a node or updates its status/address.
func (d *RoundRobinDistributor) AddOrUpdateNode(node StorageNode) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for i := range d.nodes {
		if d.nodes[i].ID == node.ID {
			d.nodes[i] = node
			return
		}
	}
	d.nodes = append(d.nodes, node)
}

func (d *RoundRobinDistributor) SelectServers(count int, exclude []string) ([]StorageNode, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	excludeMap := make(map[string]bool)
	for _, id := range exclude {
		excludeMap[id] = true
	}

	var available []StorageNode
	for _, n := range d.nodes {
		if n.IsAlive && !excludeMap[n.ID] {
			available = append(available, n)
		}
	}

	if len(available) < count {
		return nil, errors.New("not enough healthy storage servers available")
	}

	var selected []StorageNode
	for i := 0; i < count; i++ {
		d.lastIdx = (d.lastIdx + 1) % len(available)
		selected = append(selected, available[d.lastIdx])
	}
	return selected, nil
}
