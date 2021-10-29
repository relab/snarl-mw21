package swarmconnector

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/ethersphere/swarm/chunk"
	"github.com/relab/snarl-mw21/utils"
)

// TreeChunk represents a node in the merkle tree.
type TreeChunk struct {
	Depth       int
	BranchCount int64
	Length      int
	SubtreeSize uint64
	Data        []byte
	Key         []byte // Address of chunk
	Index       int    // Position of chunk
	Children    []*TreeChunk
	Parent      *TreeChunk
}

// NewTreeChunk creates a reference to a new TreeChunk struct
func NewTreeChunk(depth, index int, key, data []byte, parent *TreeChunk) *TreeChunk {
	subTreeSize := RawChunkSize(data)
	var numChildren int
	if subTreeSize > uint64(len(data)) {
		numChildren = (len(data) - 8) / len(key)
	}
	return &TreeChunk{
		Depth:       depth,
		Index:       index,
		Key:         key,
		Data:        data,
		Children:    make([]*TreeChunk, numChildren),
		SubtreeSize: subTreeSize,
		Length:      len(data),
		Parent:      parent,
	}
}

// SetChildren sets the children of the given chunk
func (tc *TreeChunk) SetChildren(children []*TreeChunk) {
	tc.Children = children
}

// GetChildFromMem gets the n-th child from the root. Assuming it already being loaded into memory
func (tc *TreeChunk) GetChildFromMem(index int) (*TreeChunk, error) {
	child := tc.getChildFromMem(float64(index))
	if child == nil {
		return nil, errors.New("could not find child")
	}
	return child, nil
}

// GetChildFromMemWithFail gets the n-th child from the root. Assuming it already being loaded into memory
// Accepts input failedChunks which has the canonical index of failed nodes.
func (tc *TreeChunk) GetChildFromMemWithFail(index int, failedChunks map[int]BlockFailure) (*TreeChunk, error) {
	if fail, myselfFailed := failedChunks[tc.Index]; myselfFailed {
		if fail.Class == Unavailable {
			if fail.Delay > 0 {
				time.Sleep(fail.Delay)
			}
			return nil, errors.New("could not find root")
		} else if fail.Class == Delay {
			time.Sleep(fail.Delay)
		} else if fail.Class == Corrupt {
			tc.Data = utils.GenerateRandomBytes(chunk.DefaultSize, time.Now().UnixNano())
			return tc, nil
		}
	}
	child := tc.getChildFromMemWithFail(float64(index), failedChunks)
	if child == nil {
		return nil, errors.New("could not find child")
	}
	return child, nil
}

// We need this function because intermediate nodes can also fail. Not only root and leaves.
func (tc *TreeChunk) getChildFromMemWithFail(index float64, failedChunks map[int]BlockFailure) *TreeChunk {
	if len(tc.Data) == 0 || index == 0 || uint64(len(tc.Data)) == tc.SubtreeSize+ChunkSizeOffset {
		return tc
	}

	var subChildren, prevSubChildren float64
	for i := 0; i < len(tc.Children); i++ {
		subSize := math.Ceil(float64(tc.Children[i].SubtreeSize) / chunk.DefaultSize)
		prevSubChildren = subChildren
		subChildren += subSize

		if index <= subChildren {
			if fail, childFailed := failedChunks[tc.Children[i].Index]; childFailed {
				if fail.Class == Unavailable {
					if fail.Delay > 0 {
						time.Sleep(fail.Delay)
					}
					return nil
				} else if fail.Class == Delay {
					time.Sleep(fail.Delay)
				} else if fail.Class == Corrupt {
					tc.Data = utils.GenerateRandomBytes(chunk.DefaultSize, time.Now().UnixNano())
					return tc
				}
			}
			return tc.Children[i].getChildFromMemWithFail(index-prevSubChildren, failedChunks)
		}
	}

	return nil
}

func (tc *TreeChunk) getChildFromMem(index float64) *TreeChunk {
	if len(tc.Data) == 0 || index == 0 || uint64(len(tc.Data)) == tc.SubtreeSize+ChunkSizeOffset {
		return tc
	}

	var subChildren, prevSubChildren float64
	for i := 0; i < len(tc.Children); i++ {
		subSize := math.Ceil(float64(tc.Children[i].SubtreeSize) / chunk.DefaultSize)
		prevSubChildren = subChildren
		subChildren += subSize

		if index <= subChildren {
			return tc.Children[i].getChildFromMem(index - prevSubChildren)
		}
	}

	return nil
}

// Depricated: Use FlattenTreeWindow to entangle.
// FlattenTree will flatten the tree in the same order as its canonical indexing.
func (tc *TreeChunk) FlattenTree() (out []*TreeChunk) {
	out = make([]*TreeChunk, tc.Index)
	var walker func(*TreeChunk)
	var currIndex int = 0
	walker = func(parent *TreeChunk) {
		for j := range parent.Children {
			walker(parent.Children[j])
		}
		out[currIndex] = parent
		currIndex++
	}

	walker(tc)

	return
}

// FlattenTreeWindow flattens the tree in a way that takes care not to put dependencies inside lattice windows.
// s - Horizontal, p - Helical
func (tc *TreeChunk) FlattenTreeWindow(s, p int) (out []*TreeChunk) {
	out = make([]*TreeChunk, tc.Index)
	var walker func(*TreeChunk)
	var currIndex int = 0
	internalNodes := make([]*TreeChunk, 0)
	walker = func(parent *TreeChunk) {
		if len(parent.Children) > 0 {
			for j := range parent.Children {
				walker(parent.Children[j])
			}
			if parent.Index != tc.Index {
				internalNodes = append(internalNodes, parent)
			}
		}
		out[currIndex] = parent
		currIndex++
	}
	walker(tc)
	windowSize := s * p // s - Horizontal, p - Helical
	for i := 0; i < len(internalNodes); i++ {
		im := internalNodes[i]
		lowestChild := im.Children[0].Index
		highestChild := im.Children[len(im.Children)-1].Index
		for j := windowSize; j < tc.Index; j += windowSize + s {
			inWindow := out[j].Index > lowestChild-windowSize && out[j].Index < highestChild+windowSize
			if !inWindow && len(out[j].Children) == 0 {
				// Swap position of im and the data
				out[j], out[im.Index-1] = out[im.Index-1], out[j]
				break
			}
		}
	}

	return
}

// FilterChunks can be used if you only want to retrieve some chunks of the tree. I.e only leaves or intermediate.
func (tc *TreeChunk) FilterChunks(filter func(*TreeChunk) bool) (out []*TreeChunk) {
	out = make([]*TreeChunk, 0, tc.Index)
	var walker func(*TreeChunk)
	walker = func(parent *TreeChunk) {
		for j := range parent.Children {
			walker(parent.Children[j])
		}
		if filter(parent) {
			out = append(out, parent)
		}
	}

	walker(tc)

	return
}

func (tc *TreeChunk) String() (output string) {
	var hierarchyStr map[int]string = make(map[int]string)
	var walker func(*TreeChunk)
	walker = func(parent *TreeChunk) {
		hierarchyStr[parent.Depth] += fmt.Sprintf("[%v:%08x]", parent.Index, parent.Key[:4])
		for j := range parent.Children {
			walker(parent.Children[j])
		}
	}

	walker(tc)

	height := len(hierarchyStr)
	for i := height; i > 0; i-- {
		output += fmt.Sprintf("--- Level %v (%v) ---\n%v\n", height-i, strings.Count(hierarchyStr[i], ":"), hierarchyStr[i])
	}
	return
}
