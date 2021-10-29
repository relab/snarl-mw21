package swarmconnector

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sync"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/storage"
	"github.com/relab/snarl-mw21/repair"
	"github.com/relab/snarl-mw21/utils"
)

func GetTreeIndexBySize(size uint64) (index int) {
	if size <= chunk.DefaultSize {
		return 1
	}

	// Number of leaves
	leaves := int(size / chunk.DefaultSize)

	// We have at least a root and a number of leaves
	index = 1
	depth, floorDepth := treeDepth(leaves)
	maxDepth := depth
	prevDepth := floorDepth

	// Partial child: Do we have a chunk under the space limit?
	if size%chunk.DefaultSize != 0 {
		// Are we adding a child to a saturated tree?
		if depth > 0 && depth == float64(floorDepth) {
			index = 3
		} else {
			index = 2
		}
	}

	// How many full trees do we have ?
	for prevDepthCtr := 0; depth >= 1; {
		index += indexAt(floorDepth)
		leaves -= leavesAt(floorDepth)

		// Because of siblings root. Depth of "sibling" is not equal
		hasUnevenTree := prevDepth != floorDepth
		if hasUnevenTree {
			index++
		} else {
			prevDepthCtr++
		}
		prevDepth = floorDepth
		depth, floorDepth = treeDepth(leaves)

		// If we have multiple siblings of the same height, we should offset the index by 1.
		if hasSiblingsAtSameHeight(depth, maxDepth, leaves, prevDepthCtr, hasUnevenTree) {
			index++
		}
	}
	if leaves == 0 {
		index--
	} else {
		index += leaves
	}

	return index
}

func hasSiblingsAtSameHeight(depth, maxDepth float64, numLeaves, prevDepthCounter int, unevenTree bool) bool {
	switch {
	case depth < 0:
		return prevDepthCounter > 1 || prevDepthCounter == 1 && numLeaves == 0 && unevenTree
	case depth < 1:
		return numLeaves > 0
	case maxDepth >= 2:
		return numLeaves < ChunkMaxBranch
	}
	return false
}

type BuildTreeOptions struct {
	EmptyLeaves bool
	EagerRepair bool
}

// BuildCompleteTree reconstruct the Pyramid tree with a canonical naming on all
// the chunks of the tree. This will later be used when entangling the chunks
// and helpful in the repair process.
func BuildCompleteTree(ctx context.Context, getter storage.Getter, rootAddr storage.Reference,
	options BuildTreeOptions, repairer repair.Repairer) (*TreeChunk, error) {
	addr := utils.RemoveDecryptionKeyFromChunkHash(rootAddr, len(rootAddr))

	var rootChunk []byte
	var rootIndex int = repairer.GetRootIndex()
	var err error
	if rootIndex == -1 {
		rootChunk, err = getter.Get(ctx, addr)
		rootIndex = GetTreeIndexChunkData(rootChunk)
	} else {
		rootChunk, err = repairer.GetChunk(addr, rootIndex)
	}

	if err != nil {
		rootChunk, err = repairer.RepairChunk(rootIndex)
		if err != nil {
			return nil, err
		}
	}

	depth := GetDepthCanonicalIndex(rootIndex)
	tc := NewTreeChunk(depth, rootIndex, rootAddr, rootChunk, nil)
	ctxWithCancel, cancel := context.WithCancel(ctx)
	// Build Merkle tree by traversing child nodes recursively
	err = tc.walkTreeChunk(ctxWithCancel, cancel, getter, 0, options, repairer)

	return tc, err
}

// GetChildrenFromNet gets child chunks based on their canonical index.
func GetChildrenFromNet(ctx context.Context, getter storage.Getter, rootAddr storage.Reference, repairer *repair.Repairer, indexes ...int) ([]*TreeChunk, error) {
	addr := utils.RemoveDecryptionKeyFromChunkHash(rootAddr, len(rootAddr))
	rootChunk, err := getter.Get(ctx, addr)
	if err != nil {
		return nil, err
	}

	tc := NewTreeChunk(0, GetTreeIndexChunkData(rootChunk), rootAddr, rootChunk, nil)

	treeChunks := make([]*TreeChunk, len(indexes))
	knownChunks := make(map[string]storage.ChunkData)
	requestChan := make(chan storage.Reference)
	resultChan := make(chan storage.ChunkData)

	go func() {
	Getter:
		for r := range requestChan {
			str := hexutil.Encode(r)
			if _, ok := knownChunks[str]; !ok {
				chunkdata, err := getter.Get(ctx, r)
				if err != nil {
					break Getter
				}
				knownChunks[str] = chunkdata
			}
			resultChan <- knownChunks[str]
		}
	}()

	var wg sync.WaitGroup

	for i := 0; i < len(indexes); i++ {
		wg.Add(1)
		go func(index, iter int) {
			defer wg.Done()
			treeChunks[iter] = tc.getChildFromNet(float64(index), requestChan, resultChan)
		}(indexes[i], i)
	}
	wg.Wait()

	return treeChunks, nil
}

func (tc *TreeChunk) getChildFromNet(index float64, requestChan chan storage.Reference, resultChan chan storage.ChunkData) *TreeChunk {
	numLeavesFloat := float64(tc.SubtreeSize) / chunk.DefaultSize
	numLeaves := math.Ceil(numLeavesFloat)

	if tc.SubtreeSize <= uint64(len(tc.Data)) || index == 0 {
		return tc
	} else if index > numLeaves {
		return nil // The node we are looking for is located somewhere else
	}

	lenAddr := len(tc.Key)

	var childNum int
	var offset float64
	numChildren := (len(tc.Data) - ChunkSizeOffset) / chunk.AddressLength
	childSize := getChildSize(numLeaves)
	for ; childNum < numChildren; childNum++ {
		if index <= offset+childSize {
			childNum++
			break
		} else {
			offset += childSize
		}
	}
	childNum--

	start, end := ChunkSizeOffset+childNum*lenAddr, ChunkSizeOffset+(childNum+1)*lenAddr
	childAddr := tc.Data[start:end]

	requestChan <- childAddr
	child := <-resultChan

	childtc := NewTreeChunk(tc.Depth-1, 0, childAddr, child, tc)

	nextIndex := index
	if childNum != 0 {
		nextIndex -= offset
	}

	return childtc.getChildFromNet(nextIndex, requestChan, resultChan)
}

// childOffset returns the offset for the parent's child.
func (tc *TreeChunk) childOffset() int {
	if len(tc.Children) > 1 {
		return GetChildOffsetByStandardSize(tc.SubtreeSize)
	}
	// Have only one child; the size of parent and child is equal.
	return GetTreeIndexBySize(tc.SubtreeSize)
}

// childIndex returns the index for childNum.
func (tc *TreeChunk) childIndex(lastChild bool, parentOffset int, offset int, childNum int) int {
	if lastChild {
		return tc.Index - 1
	}
	return parentOffset + (offset * childNum)
}

func nextParentOffset(lastChild bool, hasChildren bool, lastOffset, childIndex, offset int) int {
	var lastChildOffset int
	if lastChild && hasChildren {
		if lastOffset < offset {
			lastChildOffset = offset - lastOffset
		}
	}
	return childIndex - offset + lastChildOffset
}

// walkTreeChunk takes a tree chunk and walks down all its branches.
// The function returns on the first error encountered (if any), not performing further processing.
func (tc *TreeChunk) walkTreeChunk(ctx context.Context, cancel context.CancelFunc,
	getter storage.Getter, parentOffset int, options BuildTreeOptions, repairer repair.Repairer) error {
	// This is a leaf node, since it contains all the data in its subtree
	if tc.SubtreeSize <= uint64(len(tc.Data)) {
		return nil
	}

	lenKey := len(tc.Key)

	numChildren := len(tc.Children)

	// Index offset for each child
	offset := tc.childOffset()

	// Goroutines processing child nodes send their results here
	res := make(chan error, numChildren)

	for i, j := ChunkSizeOffset, 1; j <= numChildren; i, j = i+lenKey, j+1 {
		go func(childHashStart, childNum int) {
			// Error occurred or we are finished
			if err := ctx.Err(); err != nil {
				res <- err
				return
			}

			childHashEnd := childHashStart + lenKey
			// True if this will be the last child of tc
			lastChild := len(tc.Data) == childHashEnd
			childIndex := tc.childIndex(lastChild, parentOffset, offset, childNum)
			childAddr := utils.RemoveDecryptionKeyFromChunkHash(tc.Data[childHashStart:childHashEnd], lenKey)

			// Try to retrieve chunk normally
			child, err := repairer.GetChunk(childAddr, childIndex)
			if err != nil {
				// Try to repair chunk since it was not directly available
				child, err = repairer.RepairChunk(childIndex)
			}
			if err == nil && len(child) == 0 {
				err = errors.New("empty child")
			}
			if err != nil {
				cancel()
				res <- err
				return // Do not continue as we need the entire thing
			}

			hasChildren := RawChunkSize(child) > uint64(len(child))
			nextParent := nextParentOffset(lastChild, hasChildren, GetTreeIndexChunkData(child), childIndex, offset)

			childChunk := NewTreeChunk(tc.Depth-1, childIndex, childAddr, child, tc)

			// Tree chunk
			if hasChildren {
				if err := childChunk.walkTreeChunk(ctx, cancel, getter, nextParent, options, repairer); err != nil {
					res <- err
					return
				}
			} else if options.EmptyLeaves {
				// Do not put payload data into memory.
				childChunk.Data = nil
			}

			tc.Children[childNum-1] = childChunk
			res <- nil
		}(i, j)
	}

	for i := 0; i < numChildren; i++ {
		// Wait for goroutines to finish processing child nodes
		if err := <-res; err != nil {
			return err
		}
	}
	close(res)
	return nil
}

// Get allows us to inject code upon retrieving chunks from the local storage.
func (db *DB) Get(ctx context.Context, addr storage.Reference) (storage.ChunkData, error) {
	ch, err := db.DB.Get(ctx, chunk.ModeGetRequest, chunk.Address(addr))
	if err != nil {
		return nil, err
	} else if ch == nil || len(ch.Data()) <= ChunkSizeOffset {
		return nil, chunk.ErrChunkNotFound
	}
	return ch.Data(), err
}

// ChunkSize returns the size of the underlying chunks.
func ChunkSize(c chunk.Chunk) uint64 {
	return binary.LittleEndian.Uint64(c.Data()[:ChunkSizeOffset])
}

func RawChunkSize(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data[:ChunkSizeOffset])
}
