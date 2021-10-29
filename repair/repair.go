package repair

type Repairer interface {
	// Get
	GetChunk(addr []byte, index int) ([]byte, error)
	GetLeaf(rootaddr []byte, leafindex int) ([]byte, error)
	GetRootIndex() int

	// Repair
	RepairChunk(index int) ([]byte, error)
	RepairAll()
}
