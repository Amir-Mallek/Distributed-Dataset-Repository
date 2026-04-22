package master

import "errors"

type StorageNode struct {
	ID      string
	Address string
	IsAlive bool
}

type Distributor interface {
	SelectServers(count int, exclude []string) ([]StorageNode, error)
}

type RoundRobinDistributor struct {
	nodes   []StorageNode
	lastIdx int
}

func NewRoundRobinDistributor(nodes []StorageNode) *RoundRobinDistributor {
	return &RoundRobinDistributor{nodes: nodes}
}

func (d *RoundRobinDistributor) SelectServers(count int, exclude []string) ([]StorageNode, error) {
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
