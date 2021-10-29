package swarmconnector

import (
	"math"

	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/storage"
)

const (
	ChunkSizeOffset = 8
	ChunkMaxBranch  = chunk.DefaultSize / chunk.AddressLength
	StdSizeLevel1   = chunk.DefaultSize * ChunkMaxBranch
	StdSizeLevel2   = StdSizeLevel1 * ChunkMaxBranch
	StdSizeLevel3   = StdSizeLevel2 * ChunkMaxBranch
	StdSizeLevel4   = StdSizeLevel3 * ChunkMaxBranch
	StdSizeLevel5   = StdSizeLevel4 * ChunkMaxBranch
	StdSizeLevel6   = StdSizeLevel5 * ChunkMaxBranch
)

const (
	Leaves1 = ChunkMaxBranch
	Leaves2 = Leaves1 * ChunkMaxBranch
	Leaves3 = Leaves2 * ChunkMaxBranch
	Leaves4 = Leaves3 * ChunkMaxBranch
	Leaves5 = Leaves4 * ChunkMaxBranch
	Leaves6 = Leaves5 * ChunkMaxBranch
)

const (
	Index0 = 1
	Index1 = Index0 + Leaves1
	Index2 = Index1 + Leaves2
	Index3 = Index2 + Leaves3
	Index4 = Index3 + Leaves4
	Index5 = Index4 + Leaves5
	Index6 = Index5 + Leaves6
)

func GetChildOffsetByStandardSize(size uint64) int {
	switch {
	case size <= chunk.DefaultSize:
		return 0
	case size <= StdSizeLevel1:
		return Index0
	case size <= StdSizeLevel2:
		return Index1
	case size <= StdSizeLevel3:
		return Index2
	case size <= StdSizeLevel4:
		return Index3
	case size <= StdSizeLevel5:
		return Index4
	case size <= StdSizeLevel6:
		return Index5
	}
	return Index6
}

func indexAt(depth int) (index int) {
	switch depth {
	case 1:
		return Index1
	case 2:
		return Index2
	case 3:
		return Index3
	case 4:
		return Index4
	case 5:
		return Index5
	case 6:
		return Index6
	}
	return 0
}

func leavesAt(depth int) (leaves int) {
	switch depth {
	case 1:
		return Leaves1
	case 2:
		return Leaves2
	case 3:
		return Leaves3
	case 4:
		return Leaves4
	case 5:
		return Leaves5
	case 6:
		return Leaves6
	}
	return 0
}

// GetTreeIndexChunkData calculates the canonical index of the given chunk in the tree
func GetTreeIndexChunkData(ch storage.ChunkData) int {
	return GetTreeIndexBySize(ch.Size())
}

// GetTreeIndex calculates the canonical index of the given chunk in the tree
func GetTreeIndex(ch chunk.Chunk) int {
	return GetTreeIndexBySize(ChunkSize(ch))
}

var logMaxBranch = math.Log(ChunkMaxBranch)

// treeDepth returns the depth of a tree with the number of leaves.
func treeDepth(leaves int) (float64, int) {
	depth := math.Log(float64(leaves)) / logMaxBranch
	return depth, int(depth)
}

func getChildSize(numLeaves float64) float64 {
	depth := math.Ceil(math.Log(numLeaves) / logMaxBranch)
	switch depth {
	case 1:
		return 1
	case 2:
		return Leaves1
	case 3:
		return Leaves2
	case 4:
		return Leaves3
	case 5:
		return Leaves4
	case 6:
		return Leaves5
	case 7:
		return Leaves6
	}
	return math.Pow(ChunkMaxBranch, depth)
}
