package repair

import (
	"context"

	"github.com/ethersphere/swarm/storage"
)

type MockRepair struct {
	Repairer
	Getter storage.Getter
}

func NewMockRepair(getter storage.Getter) *MockRepair {
	return &MockRepair{Getter: getter}
}

func (mr *MockRepair) GetChunk(addr []byte, index int) ([]byte, error) {
	return mr.Getter.Get(context.Background(), addr)
}
func (mr *MockRepair) GetLeaf(rootaddr []byte, leafindex int) ([]byte, error) { return nil, nil }
func (mr *MockRepair) GetRootIndex() int                                      { return -1 }

func (mr *MockRepair) RepairChunk(index int) ([]byte, error) { return nil, nil }
func (mr *MockRepair) RepairAll()                            {}
