package swarmconnector

import (
	"bytes"
	"context"
	"math"

	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/storage"
	"github.com/relab/snarl-mw21/repair"
	"github.com/relab/snarl-mw21/utils"
)

func IsChunkLeaf(chunk storage.ChunkData) bool {
	return chunk.Size() <= uint64(len(chunk))
}

// GetDepthCanonicalIndex returns the depths of a tree with a root node that has the
// given canonical index.
func GetDepthCanonicalIndex(maxIndex int) int {
	if maxIndex <= 1 {
		return 1
	} else if maxIndex <= 129 {
		return 2
	} else if maxIndex <= 16513 {
		return 3
	} else if maxIndex <= 2113665 {
		return 4
	} else if maxIndex <= 270549121 {
		return 5
	} else if maxIndex <= 34361852033 {
		return 6
	} else if maxIndex <= 4432408363137 {
		return 7
	} else {
		var depth float64 = 8
		maxNodes := int(4432408363137 + math.Pow(ChunkMaxBranch, depth))
		for maxNodes < maxIndex {
			depth++
			maxNodes += int(math.Pow(ChunkMaxBranch, depth))
		}
		return int(depth)
	}
}

type ChunkMetadata struct {
	Size     uint64
	Length   int
	Parent   int
	Children []int
}

func GenerateChunkMetadata(size uint64) ([]ChunkMetadata, error) {
	b := make([]byte, size)
	data := bytes.NewReader(b)

	testtag := chunk.NewTag(0, "test-tag", 0, false)

	putGetter := storage.NewHasherStore(utils.NewMapChunkStore(),
		storage.MakeHashFunc(storage.DefaultHash), false, testtag)

	ctx := context.Background()

	rootAddr, wait, err := storage.TreeSplit(ctx, data, int64(size), putGetter)
	err = wait(ctx)
	if err != nil {
		return nil, err
	}

	treeRoot, err := BuildCompleteTree(ctx, putGetter, storage.Reference(rootAddr),
		BuildTreeOptions{EmptyLeaves: true}, repair.NewMockRepair(putGetter))
	if err != nil {
		return nil, err
	}

	sizeList := make([]ChunkMetadata, treeRoot.Index)
	var currIndex int = 0
	var walker func(*TreeChunk)
	walker = func(parent *TreeChunk) {
		for j := range parent.Children {
			walker(parent.Children[j])
		}
		children := make([]int, len(parent.Children))
		for i := 0; i < len(parent.Children); i++ {
			children[i] = parent.Children[i].Index
		}
		var parnt int
		if parent.Parent == nil {
			parnt = 0
		} else {
			parnt = parent.Parent.Index
		}
		sizeList[currIndex] = ChunkMetadata{
			Size:     parent.SubtreeSize,
			Length:   parent.Length,
			Parent:   parnt,
			Children: children,
		}
		currIndex++
	}

	walker(treeRoot)

	return sizeList, nil
}

// GetLeavesCanonIndex returns the number of leaves a regular tree has, based on the canonical index.
func GetLeavesCanonIndex(maxIndex int) int {
	depth := GetDepthCanonicalIndex(maxIndex)

	if depth < 2 {
		return 1
	}
	levelCtr := make([]int, depth)
	var leaves int
	maxBranch := int(ChunkMaxBranch)
	var calc func(index, level int)
	calc = func(index, level int) {
		if levelCtr[level] == maxBranch {
			calc(index, level+1)
		} else {
			levelCtr[level]++
			if level == 0 {
				leaves++
			} else {
				if levelCtr[level] != maxBranch {
					for i := 0; i < level; i++ {
						levelCtr[i] = 0
					}
				}
			}
		}
	}
	maxIndex -= depth
	for i := 0; i < maxIndex; i++ {
		calc(i, 0)
	}

	// Need to check for empty levels from leaf to root to get correct count.
	levelCtr[0]++
	leaves++

	for i := depth - 2; i > 1; i-- {
		if levelCtr[i] != 0 && levelCtr[i-1] == 0 {
			calc(i, 0)
		}
	}

	return leaves
}
