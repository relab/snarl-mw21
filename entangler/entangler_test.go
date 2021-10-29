package entangler

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/storage"
	"github.com/ethersphere/swarm/testutil"
	"github.com/relab/snarl-mw21/repair"
	"github.com/relab/snarl-mw21/swarmconnector"
	"github.com/relab/snarl-mw21/utils"
	"github.com/stretchr/testify/assert"
)

func TestGetMemoryPosition(t *testing.T) {
	var r, h, l int
	S, P := 5, 5
	// Datablock 1
	r, h, l = GetMemoryPosition(1, S, P)
	assert.Equal(t, 0, r, "Datablock 1: R position should be 0")
	assert.Equal(t, 5, h, "Datablock 1: H position should be 5")
	assert.Equal(t, 11, l, "Datablock 1: L position should be 11")

	// Datablock 2
	r, h, l = GetMemoryPosition(2, S, P)
	assert.Equal(t, 4, r, "Datablock 2: R position should be 4")
	assert.Equal(t, 6, h, "Datablock 2: H position should be 6")
	assert.Equal(t, 12, l, "Datablock 2: L position should be 12")

	// Datablock 3
	r, h, l = GetMemoryPosition(3, S, P)
	assert.Equal(t, 3, r, "Datablock 3: R position should be 3")
	assert.Equal(t, 7, h, "Datablock 3: H position should be 7")
	assert.Equal(t, 13, l, "Datablock 3: L position should be 13")

	// Datablock 4
	r, h, l = GetMemoryPosition(4, S, P)
	assert.Equal(t, 2, r, "Datablock 4: R position should be 2")
	assert.Equal(t, 8, h, "Datablock 4: H position should be 8")
	assert.Equal(t, 14, l, "Datablock 4: L position should be 14")

	// Datablock 5
	r, h, l = GetMemoryPosition(5, S, P)
	assert.Equal(t, 1, r, "Datablock 5: R position should be 1")
	assert.Equal(t, 9, h, "Datablock 5: H position should be 9")
	assert.Equal(t, 10, l, "Datablock 5: L position should be 10")

	// Datablock 21
	r, h, l = GetMemoryPosition(21, S, P)
	assert.Equal(t, 4, r, "Datablock 21: R position should be 4")
	assert.Equal(t, 5, h, "Datablock 21: H position should be 5")
	assert.Equal(t, 10, l, "Datablock 21: L position should be 10")

	// Datablock 22
	r, h, l = GetMemoryPosition(22, S, P)
	assert.Equal(t, 3, r, "Datablock 22: R position should be 3")
	assert.Equal(t, 6, h, "Datablock 22: H position should be 6")
	assert.Equal(t, 11, l, "Datablock 22: L position should be 11")

	// Datablock 23
	r, h, l = GetMemoryPosition(23, S, P)
	assert.Equal(t, 2, r, "Datablock 23: R position should be 2")
	assert.Equal(t, 7, h, "Datablock 23: H position should be 7")
	assert.Equal(t, 12, l, "Datablock 23: L position should be 12")

	// Datablock 24
	r, h, l = GetMemoryPosition(24, S, P)
	assert.Equal(t, 1, r, "Datablock 24: R position should be 1")
	assert.Equal(t, 8, h, "Datablock 24: H position should be 8")
	assert.Equal(t, 13, l, "Datablock 24: L position should be 13")

	// Datablock 25
	r, h, l = GetMemoryPosition(25, S, P)
	assert.Equal(t, 0, r, "Datablock 25: R position should be 0")
	assert.Equal(t, 9, h, "Datablock 25: H position should be 9")
	assert.Equal(t, 14, l, "Datablock 25: L position should be 14")
}

func TestGetReplacedParityIndices(t *testing.T) {
	alpha, s, p := 3, 5, 5
	chunkSize := chunk.DefaultSize // bytes

	tests := []struct {
		maxIndex        int
		replacedIndices []int
	}{
		{1, []int{1}},
		{111, []int{1, 2, 3, 4, 5}},
		{6, []int{1, 2, 3, 4, 5}},
		{8, []int{1, 2, 3, 4, 5}},
		{9, []int{1, 2, 3, 4, 5}},
		{10, []int{1, 2, 3, 4, 5}},
		{11, []int{1, 2, 3, 4, 5}},
		{12, []int{1, 2, 3, 4, 5}},
		{13, []int{1, 2, 3, 4, 5}},
		{112, []int{1, 2, 3, 4, 5}},
		{113, []int{1, 2, 3, 4, 5}},
		{114, []int{1, 2, 3, 4, 5}},
		{115, []int{1, 2, 3, 4, 5}},
	}

	for i, test := range tests {
		tangler := NewEntangler(p, p, s, alpha, chunkSize)
		tangler.NumDataBlocks = test.maxIndex
		repIndices := tangler.GetReplacedParityIndices()
		keys, j := make([]int, len(repIndices)), 0
		for key := range repIndices {
			keys[j], j = key, j+1
		}

		assert.ElementsMatch(t, keys, test.replacedIndices, "Did not contain correct elements. Test %v. Expected: %v, Got: %v",
			i, test.replacedIndices, keys)
	}
}

func TestGetWrapPosition(t *testing.T) {
	var r, h, l int
	S, P := 5, 5

	tests := []struct {
		index    int
		wrapPos  []int
		testnum  int
		maxIndex int
	}{
		{1, []int{1, 1, 1}, 1, 0},
		{111, []int{4, 1, 3}, 2, 0},
		{6, []int{12, 11, 2}, 3, 13},
		{8, []int{2, 13, 12}, 4, 13},
		{9, []int{3, 4, 13}, 5, 13},
		{10, []int{11, 5, 1}, 6, 13},
		{11, []int{4, 1, 3}, 7, 13},
		{12, []int{5, 2, 4}, 8, 13},
		{13, []int{1, 3, 5}, 9, 13},
		{112, []int{5, 2, 4}, 10, 0},
		{113, []int{1, 3, 5}, 11, 0},
		{114, []int{2, 4, 1}, 12, 0},
		{115, []int{3, 5, 2}, 13, 0},
	}

	for _, test := range tests {
		if test.maxIndex > 0 {
			r, h, l = GetWrapPositionMaxLen(test.index, S, P, test.maxIndex)
		} else {
			r, h, l = GetWrapPosition(test.index, S, P)
		}

		assert.Equal(t, test.wrapPos[0], r, "Incorrect Right position. Testnum %d", test.testnum)
		assert.Equal(t, test.wrapPos[1], h, "Incorrect Horizontal position. Testnum %d", test.testnum)
		assert.Equal(t, test.wrapPos[2], l, "Incorrect Left position. Testnum %d", test.testnum)
	}
}

// TestSwarmEntanglerWithoutSize tests the entangling of a file encoded into a merkle tree.
// !! Without checking the size metadata of any chunk.
// The test does the following steps:
// 1. Create random data in memory
// 2. Build Merkle tree from original data
// 3. Flatten tree
// 4. Entangle the flatten tree
// 5. Close the lattice(s)
// 6. Put the entangled data into files (One per alpha)
// 7. Build Merkle tree from entangled data
// 8. Assert that every node (tree and leaf) from the original Merkle tree built in step 2.,
// is able to be reconstructed from the leaves from the entangled trees.
func TestSwarmEntanglerWithoutSize(t *testing.T) {
	alpha, s, p := 3, 5, 5
	chunkSize := chunk.DefaultSize // bytes
	inputSize := 1000 * chunkSize  // 22000 * 4k bytes (100 MB)
	tangler := NewEntangler(p, p, s, alpha, chunkSize)

	// dir := t.TempDir() (Add once we get 1.15 installed on bbchain cluster.)
	// Setup temp directory
	dir, err := ioutil.TempDir("", "test-entangler")
	defer os.RemoveAll(dir)

	// 1. Create random data in memory
	addr, reader, getter, err := utils.GenerateRandomData(inputSize, storage.DefaultHash, dir)
	if err != nil {
		t.Fatalf("Got Error: %v", err)
	}

	// 2. Build Merkle tree
	treeRoot, err := swarmconnector.BuildCompleteTree(reader.Context(), getter, storage.Reference(addr),
		swarmconnector.BuildTreeOptions{}, repair.NewMockRepair(getter))
	if err != nil {
		t.Fatal(err.Error())
	}

	// 3. Flatten tree
	flatTree := treeRoot.FlattenTreeWindow(s, p)

	resultChan := make(chan *EntangledBlock)
	done := make(chan struct{})

	// Setup for step 6
	sc := swarmconnector.NewSwarmConnector(dir, "testing", dir)
	files := make([]*os.File, alpha)
	buffers := make([]*bufio.Writer, alpha)
	entangledBlocks := make([]*EntangledBlock, 0)
	for i := 0; i < alpha; i++ {
		files[i], err = os.Create(filepath.Join(dir, strconv.Itoa(i)))
		if err != nil {
			t.Error(err)
		}
		buffers[i] = bufio.NewWriter(files[i])
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer close(resultChan)
	ResultLoop:
		for {
			select {
			case block := <-resultChan:
				if block.LeftIndex < 1 {
					continue
				}
				if block.Replace {
					for i := 0; i < len(entangledBlocks); i++ {
						if entangledBlocks[i].LeftIndex == block.LeftIndex &&
							entangledBlocks[i].RightIndex == block.RightIndex {
							entangledBlocks[i].Data = block.Data
							entangledBlocks[i].Replace = true
							break
						}
					}
				} else {
					entangledBlocks = append(entangledBlocks, block)
				}
				assert.Equalf(t, chunkSize, len(block.Data), "Size of entangled chunk is incorrect. Class: %d, Parity (L: %d, R: %d), ", block.Class, block.LeftIndex, block.RightIndex)
			case <-done:
				break ResultLoop
			}
		}
		wg.Done()
	}()

	// 4. Entangle the tree
	for i := 0; i < len(flatTree); i++ {
		tangler.Entangle(flatTree[i].Data[swarmconnector.ChunkSizeOffset:], i+1, resultChan)
	}

	// 5. Wrap the lattice(s)
	tangler.WrapLattice(resultChan)
	done <- struct{}{}

	wg.Wait()

	// 6. Put the entangled data into files (One per alpha)
	for i := 0; i < alpha; i++ {
		for k := 0; k < len(entangledBlocks); k++ {
			if int(entangledBlocks[k].Class) != i {
				continue
			}
			_, err := buffers[i].Write(entangledBlocks[k].Data)
			if err != nil {
				t.Error(err)
			}

		}
		buffers[i].Flush()
		files[i].Close()
	}

	// 7. Build Merkle Tree
	entangledTrees := make([]*swarmconnector.TreeChunk, alpha)
	for i := 0; i < alpha; i++ {
		reader, err := os.Open(filepath.Join(dir, strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}
		fileState, _ := reader.Stat()
		addr, wait, err := sc.FileStore.Store(context.Background(), reader,
			fileState.Size(), false)
		if err != nil {
			t.Fatalf("Got Error: %v", err)
		}
		err = wait(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		et, err := swarmconnector.BuildCompleteTree(context.Background(), sc.LStore, storage.Reference(addr),
			swarmconnector.BuildTreeOptions{}, repair.NewMockRepair(sc.LStore))
		if err != nil {
			t.Fatal(err)
		}
		entangledTrees[i] = et
	}

	// 8. Assert that every node (tree and leaf) from the Merkle tree build in step 2.,
	// is able to be reconstructed from the leafs from the entangled trees.
	// We are cheating here by using the EntangledBlock structure to find the
	// correct parity indices. TODO
	var lp, rp *EntangledBlock
	var output, lpTreeData, rpTreeData, originData []byte
	var lpK, rpK int
	var lpTree, rpTree *swarmconnector.TreeChunk
	for j := 1; j <= len(flatTree); j++ {
		originData = flatTree[j-1].Data[swarmconnector.ChunkSizeOffset:]
		for i := 0; i < alpha; i++ {
			lp, rp, output, lpK, rpK = nil, nil, nil, 0, 0

			parityCtr := 1
			for k := 0; k < len(entangledBlocks); k++ {
				if int(entangledBlocks[k].Class) != i {
					continue
				}
				if entangledBlocks[k].LeftIndex == j {
					rp = entangledBlocks[k]
					rpK = parityCtr
				} else if entangledBlocks[k].RightIndex == j && entangledBlocks[k].LeftIndex > 0 {
					lp = entangledBlocks[k]
					lpK = parityCtr
				}

				parityCtr++
				if lp != nil && rp != nil {
					break // We found both parities, now we can XOR
				}
			}
			if lp == nil {
				t.Fatalf("Left parity was nil. Index %d", j)
			} else if rp == nil {
				t.Fatalf("Right parity was nil. Index %d", j)
			}

			// for n := 0; n < 15; n++ {
			// 	rpT, _ := entangledTrees[i].GetChildFromMem(n)
			// 	rpTD := rpT.Data[swarmconnector.ChunkSizeOffset:]
			// 	fmt.Printf("Data: %v\n", rpTD[:40])
			// }
			rpTree, err = entangledTrees[i].GetChildFromMem(rpK)
			if err != nil {
				t.Error(err)
			}

			rpTreeData = rpTree.Data[swarmconnector.ChunkSizeOffset:]
			assert.Equal(t, swarmconnector.RawChunkSize(rp.Data), swarmconnector.RawChunkSize(rpTreeData), "Size not equal. EntangleSlize size: %d, Tree size: %d", swarmconnector.RawChunkSize(rp.Data), swarmconnector.RawChunkSize(rpTreeData))
			assert.Equal(t, rp.Data, rpTreeData, "Data retrieved from RIGHT tree was incorrect.")

			// Special case for when the first parity was replaced. We use the first data block
			// and the second parity to repair the second data block.
			if lp != nil && lp.Replace == true {
				lpTree = flatTree[lp.LeftIndex-1]
			} else {
				lpTree, err = entangledTrees[i].GetChildFromMem(lpK)
				if err != nil {
					t.Error(err)
				}
				assert.Equal(t, lp.Data, lpTree.Data[swarmconnector.ChunkSizeOffset:], "Data retrieved from LEFT tree was incorrect.")
			}
			lpTreeData = lpTree.Data[swarmconnector.ChunkSizeOffset:]

			assert.NotNil(t, rpTree, "Right parity is nil. Index: %d. Class: %d, RP-index: %d", j, i, rpK)
			assert.NotNil(t, lpTree, "Left parity is nil. Index: %d. Class: %d, LP-index: %d", j, i, lpK)

			output = XORByteSlice(lpTreeData, rpTreeData)

			assert.Equal(t, swarmconnector.RawChunkSize(originData), swarmconnector.RawChunkSize(output), "Size not equal. Origin size: %d, Decoded size: %d", swarmconnector.RawChunkSize(flatTree[j-1].Data), swarmconnector.RawChunkSize(output))

			assert.Equal(t, originData, output[:len(originData)], "XOR value incorrect. Class: %d, Index: %d, Left Parity (L: %d, R: %d), Right Parity (L: %d, R: %d)",
				i, j, lp.LeftIndex, lp.RightIndex, rp.LeftIndex, rp.RightIndex)
		}
	}
}

// TestSwarmEntangler tests the entangling of a file encoded into a merkle tree.
// Also checks the size metadata of the chunks.
// The test does the following steps:
// 1. Create random data in memory
// 2. Build Merkle tree from original data
// 3. Flatten tree
// 4. Entangle the flatten tree
// 5. Close the lattice(s)
// 6. Put the entangled data into files (One per alpha)
// 7. Build Merkle tree from entangled data
// 8. Generates the size list needed for full reonstruction.
// 9. Assert that every node (tree and leaf) from the original Merkle tree built in step 2.,
// is able to be reconstructed from the leaves from the entangled trees.
func TestSwarmEntangler(t *testing.T) {
	alpha, s, p := 3, 5, 5
	chunkSize := chunk.DefaultSize // bytes
	inputSize := 1000 * chunkSize  // 22000 * 4k bytes (100 MB)

	tangler := NewEntangler(p, p, s, alpha, chunkSize)

	// Setup temp directory
	dir, err := ioutil.TempDir("", "test-entangler")
	defer os.RemoveAll(dir)

	if err != nil {
		t.Fatalf("Could not create temp directory: %v", err)
	}

	// 1. Create random data in memory
	addr, reader, getter, err := utils.GenerateRandomData(inputSize, storage.DefaultHash, dir)
	if err != nil {
		t.Fatal(err)
	}

	// 2. Build Merkle tree
	treeRoot, err := swarmconnector.BuildCompleteTree(reader.Context(), getter, storage.Reference(addr),
		swarmconnector.BuildTreeOptions{}, repair.NewMockRepair(getter))
	if err != nil {
		t.Fatal(err)
	}

	// 3. Flatten tree
	flatTree := treeRoot.FlattenTreeWindow(s, p)
	resultChan := make(chan *EntangledBlock)
	done := make(chan struct{})

	// Setup for step 6
	sc := swarmconnector.NewSwarmConnector(dir, "testing", dir)
	files := make([]*os.File, alpha)
	buffers := make([]*bufio.Writer, alpha)
	entangledBlocks := make([]*EntangledBlock, 0)
	for i := 0; i < alpha; i++ {
		files[i], err = os.Create(filepath.Join(dir, strconv.Itoa(i)))
		if err != nil {
			t.Error(err)
		}
		buffers[i] = bufio.NewWriter(files[i])
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer close(resultChan)
	ResultLoop:
		for {
			select {
			case block := <-resultChan:
				if block.LeftIndex < 1 {
					continue
				}
				if block.Replace {
					for i := 0; i < len(entangledBlocks); i++ {
						if entangledBlocks[i].LeftIndex == block.LeftIndex &&
							entangledBlocks[i].RightIndex == block.RightIndex {
							entangledBlocks[i].Data = block.Data
							entangledBlocks[i].Replace = true
							break
						}
					}
				} else {
					entangledBlocks = append(entangledBlocks, block)
				}

				assert.Equalf(t, chunkSize, len(block.Data), "Size of entangled chunk is incorrect. Class: %d, Parity (L: %d, R: %d), ", block.Class, block.LeftIndex, block.RightIndex)
			case <-done:
				break ResultLoop
			}
		}
		wg.Done()
	}()

	// 4. Entangle the tree
	for i := 0; i < len(flatTree); i++ {
		tangler.Entangle(flatTree[i].Data[swarmconnector.ChunkSizeOffset:], i+1, resultChan)
	}

	// 5. Wrap the lattice(s)
	tangler.WrapLattice(resultChan)
	done <- struct{}{}

	wg.Wait()

	// 6. Put the entangled data into files (One per alpha)
	for i := 0; i < alpha; i++ {
		for k := 0; k < len(entangledBlocks); k++ {
			if int(entangledBlocks[k].Class) != i {
				continue
			}
			_, err = buffers[i].Write(entangledBlocks[k].Data)
			if err != nil {
				t.Error(err)
			}

		}
		buffers[i].Flush()
		files[i].Close()
	}

	// 7. Build Merkle Tree
	entangledTrees := make([]*swarmconnector.TreeChunk, alpha)
	for i := 0; i < alpha; i++ {
		reader, err := os.Open(filepath.Join(dir, strconv.Itoa(i)))
		if err != nil {
			t.Fatal(err)
		}
		fileState, _ := reader.Stat()
		addr, wait, err := sc.FileStore.Store(context.Background(), reader,
			fileState.Size(), false)
		if err != nil {
			t.Fatal(err)
		}
		err = wait(context.Background())
		if err != nil {
			t.Error(err)
		}
		et, err := swarmconnector.BuildCompleteTree(context.Background(), sc.LStore, storage.Reference(addr),
			swarmconnector.BuildTreeOptions{}, repair.NewMockRepair(sc.LStore))
		if err != nil {
			t.Error(err)
		}
		entangledTrees[i] = et
	}

	sizeList, _ := swarmconnector.GenerateChunkMetadata(uint64(inputSize))

	// 9. Assert that every node (tree and leaf) from the Merkle tree build in step 2.,
	// is able to be reconstructed from the leaves from the entangled trees.
	// We are cheating here by using the EntangledBlock structure to find the
	// correct parity indices. TODO - Use Lattice.
	var lp, rp *EntangledBlock
	var output, lpTreeData, rpTreeData, originData []byte
	var lpK, rpK int
	var lpTree, rpTree *swarmconnector.TreeChunk
	for j := 1; j <= len(flatTree); j++ {
		originData = flatTree[j-1].Data
		for i := 0; i < alpha; i++ {
			lp, rp, output, lpK, rpK = nil, nil, nil, 0, 0

			parityCtr := 1
			for k := 0; k < len(entangledBlocks); k++ {
				if int(entangledBlocks[k].Class) != i {
					continue
				}
				if entangledBlocks[k].LeftIndex == j {
					rp = entangledBlocks[k]
					rpK = parityCtr
				} else if entangledBlocks[k].RightIndex == j && entangledBlocks[k].LeftIndex > 0 {
					lp = entangledBlocks[k]
					lpK = parityCtr
				}

				parityCtr++
				if lp != nil && rp != nil {
					break // We found both parities, now we can XOR
				}
			}
			if lp == nil {
				t.Fatalf("Left parity was nil. Index %d", j)
			} else if rp == nil {
				t.Fatalf("Right parity was nil. Index %d", j)
			}

			rpTree, err = entangledTrees[i].GetChildFromMem(rpK)
			if err != nil {
				t.Error(err)
			}

			rpTreeData = rpTree.Data[swarmconnector.ChunkSizeOffset:]

			// Special case for when the first parity was replaced. We use the first data block
			// and the second parity to repair the second data block.
			if lp != nil && lp.Replace == true {
				lpTree = flatTree[lp.LeftIndex-1]
			} else {
				lpTree, err = entangledTrees[i].GetChildFromMem(lpK)
				if err != nil {
					t.Error(err)
				}

			}
			lpTreeData = lpTree.Data[swarmconnector.ChunkSizeOffset:]

			assert.NotNil(t, rpTree, "Right parity is nil. Index: %d. Class: %d, RP-index: %d", j, i, rpK)
			assert.NotNil(t, lpTree, "Left parity is nil. Index: %d. Class: %d, LP-index: %d", j, i, lpK)

			sizeListShiftedIndex := flatTree[j-1].Index - 1 // We right-shifted the internal nodes, and need to get the expected size for them.

			output = make([]byte, sizeList[sizeListShiftedIndex].Length)
			binary.LittleEndian.PutUint64(output, sizeList[sizeListShiftedIndex].Size)

			copy(output[swarmconnector.ChunkSizeOffset:], XORByteSlice(lpTreeData, rpTreeData))

			assert.Equal(t, swarmconnector.RawChunkSize(originData), swarmconnector.RawChunkSize(output), "Size not equal. Origin size: %d, Decoded size: %d", swarmconnector.RawChunkSize(flatTree[j-1].Data), swarmconnector.RawChunkSize(output))

			assert.Equal(t, originData, output, "XOR value incorrect. Class: %d, Index: %d, Left Parity (L: %d, R: %d), Right Parity (L: %d, R: %d)",
				i, j, lp.LeftIndex, lp.RightIndex, rp.LeftIndex, rp.RightIndex)

		}
	}
}

// Avoid compiler optimizations.
var benchBlockResult []*EntangledBlock

func BenchmarkSwarmEntanglerEncoding(b *testing.B) {
	// Available file sizes in MB: 1, 2.5, 5, 7.5, 10, 25, 50, 75, 100, 250, 500, 750, 1000
	numChunksTest := []int{256, 640, 1280, 1920, 2560, 6400, 12800, 19200, 25600, 64000, 128000, 192000, 256000}
	testtag := chunk.NewTag(0, "test-tag", 0, false)

	var wait func(context.Context) error

	getters := make(map[int]storage.Getter)
	rootAddrs := make(map[int]storage.Address)
	for i, numChunks := range numChunksTest {
		data := make([]byte, chunk.DefaultSize*numChunksTest[i])
		reader := rand.New(rand.NewSource(time.Now().UnixNano()))
		n := 0
		for n < len(data) {
			num, _ := reader.Read(data[n:])
			n += num
		}
		bytesReader := bytes.NewReader(data)
		putGetter := storage.NewHasherStore(utils.NewMapChunkStore(), storage.MakeHashFunc(storage.DefaultHash), false, testtag)
		var err error
		var rootAddr storage.Address

		// 2. Build Merkle tree
		ctx := context.TODO()
		if usePyramid {
			rootAddr, wait, err = storage.PyramidSplit(ctx, bytesReader, putGetter, putGetter, testtag)
		} else {
			rootAddr, wait, err = storage.TreeSplit(ctx, bytesReader, int64(numChunks*chunk.DefaultSize), putGetter)
		}

		rootAddrs[numChunks] = rootAddr
		getters[numChunks] = putGetter

		if err = wait(ctx); err != nil {
			b.Error(err)
		}
	}

	var err error
	for _, numChunks := range numChunksTest {
		fmt.Printf("Size_" + strconv.Itoa(numChunks) + "\n")
		b.Run("Size_"+strconv.Itoa(numChunks), func(b *testing.B) {
			var blocks []*EntangledBlock
			for i := 0; i < b.N; i++ {
				blocks, err = entangleRandomfile(getters[numChunks], rootAddrs[numChunks])
				if err != nil {
					b.Error(err)
				}
			}
			benchBlockResult = blocks
		})
	}
}

const usePyramid = false

func entangleRandomfile(getter storage.Getter, rootAddr storage.Address) ([]*EntangledBlock, error) {
	s, p := 5, 5
	tangler := NewEntangler(p, p, s, 3, chunk.DefaultSize)

	treeRoot, err := swarmconnector.BuildCompleteTree(context.Background(), getter, storage.Reference(rootAddr),
		swarmconnector.BuildTreeOptions{}, repair.NewMockRepair(getter))
	if err != nil {
		return nil, err
	}

	// 3. Flatten tree
	flatTree := treeRoot.FlattenTreeWindow(s, p)

	resultChan := make(chan *EntangledBlock)
	done := make(chan struct{})
	entangledBlocks := make([]*EntangledBlock, 0)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer close(resultChan)
	ResultLoop:
		for {
			select {
			case block := <-resultChan:
				if block.LeftIndex < 1 {
					continue
				}
				if block.Replace {
					for i := 0; i < len(entangledBlocks); i++ {
						if entangledBlocks[i].LeftIndex == block.LeftIndex &&
							entangledBlocks[i].RightIndex == block.RightIndex {
							entangledBlocks[i].Data = block.Data
							entangledBlocks[i].Replace = true
							break
						}
					}
				} else {
					entangledBlocks = append(entangledBlocks, block)
				}
			case <-done:
				break ResultLoop
			}
		}
		wg.Done()
	}()

	// fmt.Printf("Len FlatTree: %v\n", len(flatTree))
	// 4. Entangle the tree
	for i := 0; i < len(flatTree); i++ {
		tangler.Entangle(flatTree[i].Data[swarmconnector.ChunkSizeOffset:], i+1, resultChan)
	}

	// 5. Wrap the lattice(s)
	tangler.WrapLattice(resultChan)
	done <- struct{}{}

	wg.Wait()

	return entangledBlocks, nil
}

// TestSwarmEntanglerWithLattice does the same as TestSwarmEntangler
// with the difference that the Lattice structure is used for repairs.
func TestSwarmEntanglerWithLattice(t *testing.T) {
	alpha, s, p := 3, 5, 5
	chunkSize := chunk.DefaultSize // bytes
	inputSize := 256 * chunkSize   // 22000 * 4k bytes (100 MB)

	tangler := NewEntangler(p, p, s, alpha, chunkSize)

	// Setup temp directory
	dir, err := ioutil.TempDir("", "test-entangler")
	defer os.RemoveAll(dir)

	if err != nil {
		t.Fatalf("Could not create temp directory. Error: %v", err.Error())
	}

	// 1. Create random data in memory
	addr, reader, getter, err := utils.GenerateRandomData(inputSize, storage.DefaultHash, dir)
	if err != nil {
		t.Fatalf("Got Error: %v", err)
	}

	// 2. Build Merkle tree
	treeRoot, err := swarmconnector.BuildCompleteTree(reader.Context(), getter, storage.Reference(addr),
		swarmconnector.BuildTreeOptions{}, repair.NewMockRepair(getter))
	if err != nil {
		t.Fatal(err.Error())
	}

	// 3. Flatten tree
	flatTree := treeRoot.FlattenTreeWindow(s, p)
	resultChan := make(chan *EntangledBlock)
	done := make(chan struct{})

	// Setup for step 6
	sc := swarmconnector.NewSwarmConnector(dir, "testing", dir)
	files := make([]*os.File, alpha)
	buffers := make([]*bufio.Writer, alpha)
	entangledBlocks := make([]*EntangledBlock, 0)
	for i := 0; i < alpha; i++ {
		files[i], err = os.Create(filepath.Join(dir, strconv.Itoa(i)))
		if err != nil {
			t.Error(err)
		}

		buffers[i] = bufio.NewWriter(files[i])
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer close(resultChan)
	ResultLoop:
		for {
			select {
			case block := <-resultChan:
				if block.LeftIndex < 1 {
					continue
				}
				if block.Replace {
					for i := 0; i < len(entangledBlocks); i++ {
						if entangledBlocks[i].LeftIndex == block.LeftIndex &&
							entangledBlocks[i].RightIndex == block.RightIndex {
							entangledBlocks[i].Data = block.Data
							entangledBlocks[i].Replace = true
							break
						}
					}
				} else {
					entangledBlocks = append(entangledBlocks, block)
				}
				assert.Equalf(t, chunkSize, len(block.Data), "Size of entangled chunk is incorrect. Class: %d, Parity (L: %d, R: %d), ", block.Class, block.LeftIndex, block.RightIndex)
			case <-done:
				break ResultLoop
			}
		}
		wg.Done()
	}()

	// 4. Entangle the tree
	for i := 0; i < len(flatTree); i++ {
		tangler.Entangle(flatTree[i].Data[swarmconnector.ChunkSizeOffset:], i+1, resultChan)
	}

	// 5. Wrap the lattice(s)
	tangler.WrapLattice(resultChan)
	done <- struct{}{}

	wg.Wait()

	// 6. Put the entangled data into files (One per alpha)
	for i := 0; i < alpha; i++ {
		ebSorted := make(map[int][]byte)
		for k := 0; k < len(entangledBlocks); k++ {
			if int(entangledBlocks[k].Class) != i {
				continue
			}
			ebSorted[entangledBlocks[k].LeftIndex-1] = entangledBlocks[k].Data
		}
		for k := 0; k < len(entangledBlocks); k++ {
			_, err = buffers[i].Write(ebSorted[k])
			if err != nil {
				t.Error(err)
			}

		}
		buffers[i].Flush()
		files[i].Close()
	}

	// 7. Build Merkle Tree
	entangledTrees := make([]*swarmconnector.TreeChunk, alpha)
	for i := 0; i < alpha; i++ {
		reader, err := os.Open(filepath.Join(dir, strconv.Itoa(i)))
		if err != nil {
			t.Fatalf("Got Error: %v", err)
		}
		fileState, _ := reader.Stat()
		addr, wait, err := sc.FileStore.Store(context.Background(), reader,
			fileState.Size(), false)
		if err != nil {
			t.Fatalf("Got Error: %v", err)
		}
		err = wait(context.Background())
		if err != nil {
			t.Error(err)
		}
		et, err := swarmconnector.BuildCompleteTree(context.Background(), sc.LStore, storage.Reference(addr),
			swarmconnector.BuildTreeOptions{}, repair.NewMockRepair(sc.LStore))
		if err != nil {
			t.Error(err)
		}
		// fmt.Printf("Number of children for entangled tree class: %d. Is: %d. Size: %d, TotChilds: %d\n", i, len(et.Children), et.SubtreeSize, et.SubtreeSize/chunk.DefaultSize)
		entangledTrees[i] = et
	}

	lattice := NewSwarmLattice(context.Background(), alpha, s, p, uint64(inputSize), getter,
		treeRoot.Key, [][]byte{
			entangledTrees[0].Key, entangledTrees[1].Key,
			entangledTrees[2].Key,
		}, chunk.DefaultSize)

	for i := 0; i < lattice.NumDataBlocks; i = i + 1 {
		b := lattice.Blocks[i]
		b.Identifier = flatTree[i].Key
		b.Data = flatTree[i].Data
	}

	// Repair block
	for i := 0; i < lattice.NumDataBlocks; i++ {
		// sizeListShiftedIndex := flatTree[i].Index - 1 // We right-shifted the internal nodes, and need to get the expected size for them.
		// shiftedOrigData := lattice.Blocks[sizeListShiftedIndex]
		data := lattice.Blocks[i]
		tmpOrig := make([]byte, len(data.Data))
		copy(tmpOrig, data.Data)
		// fmt.Printf("Lattice position: %v, Is data block: %v\n", data.Position, shiftedOrigData.Position)
		repPair := data.GetRepairPairs()
		for j := 0; j < alpha; j++ {
			lp := repPair[j].Left
			rp := repPair[j].Right
			data.Data = nil // Clear the data for testing

			if lp.IsParity {
				assert.Equal(t, lp.Right[0], rp.Left[0], "Parities are not connected.")
			} else {
				assert.Equal(t, lp.Right[j].Right[0], rp.Left[0], "Parities are not connected.")
			}
			assert.Equal(t, data.Position, rp.Position, "Position of left parity incorrect")

			rpT, err := entangledTrees[j].GetChildFromMem(rp.Position)
			if err != nil {
				t.Fatal(err.Error())
			}
			rp.Data = rpT.Data

			if lp.IsParity {
				lpT, err := entangledTrees[j].GetChildFromMem(lp.Position)
				if err != nil {
					t.Fatal(err.Error())
				}
				lp.Data = lpT.Data
			}

			assert.NotEqual(t, lp.Data, rp.Data, "Parity data is equal. Left: %v, LD: %v, Right: %v, RD: %v\n",
				lp, lp.Data[:20], rp, rp.Data[:20])

			err = data.Repair(lp, rp)
			assert.Nil(t, err, "Error!! %+v", err)

			//assert.Equal(t, len(tmpOrig), len(data.Data), "Length of reconstruct not equal.")

			assert.Equal(t, tmpOrig[:16], data.Data[:16], "Data not reconstructed properly. Index: %v", data.Position)
		}
	}
}

func TestEntangler(t *testing.T) {
	alpha, s, p := 3, 5, 5
	inputSize := 1000              // 25600 * 4k bytes (100 MB)
	chunkSize := chunk.DefaultSize // bytes
	tangler := NewEntangler(p, p, s, alpha, chunkSize)

	// Generate random data
	input := make([][]byte, inputSize)

	for i := 0; i < len(input); i++ {
		input[i] = testutil.RandomBytes(i, chunkSize)
	}

	resultChan := make(chan *EntangledBlock)
	done := make(chan struct{})

	// Setup
	outputs := make([][][]byte, alpha)
	for i := 0; i < alpha; i++ {
		outputs[i] = make([][]byte, inputSize+utils.Max(s, p))
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer close(resultChan)
	ResultLoop:
		for {
			select {
			case block := <-resultChan:
				outputs[block.Class][block.RightIndex] = block.Data
			case <-done:
				break ResultLoop
			}
		}
		wg.Done()
	}()

	for i := 0; i < len(input); i++ {
		tangler.Entangle(input[i], i+1, resultChan)
	}
	done <- struct{}{}

	wg.Wait()

	var nextIndex int

	for j := 1; j <= len(input); j++ {
		r, h, l := GetForwardNeighbours(j, s, p)
		for i := 0; i < alpha; i++ {
			switch i {
			case 0:
				nextIndex = h
			case 1:
				nextIndex = r
			case 2:
				nextIndex = l
			}
			assert.LessOrEqual(t, len(input[j-1]), len(outputs[i][j]), "Size of output must be greater or equal than input")

			if nextIndex < len(input) {
				output := XORByteSlice(outputs[i][j], outputs[i][nextIndex])
				assert.Equal(t, input[j-1], output, "XOR value incorrect. Class: %d, LeftP: %d, RightP: %d", i, j, nextIndex)
			}
		}
	}
}

func TestSetDatablocksToClose(t *testing.T) {
	type test struct {
		wrapPos  []int
		testnum  int
		maxIndex int
	}

	var tests []*test

	tests = append(tests, &test{
		maxIndex: 13,
		wrapPos:  []int{13, 12, 11, 10, 9, 8, 6},
		testnum:  1,
	})
	tests = append(tests, &test{
		maxIndex: 115,
		wrapPos:  []int{115, 114, 113, 112, 111},
		testnum:  2,
	})
	tests = append(tests, &test{
		maxIndex: 24,
		wrapPos:  []int{24, 23, 22, 21, 20, 19, 16},
		testnum:  3,
	})
	tests = append(tests, &test{
		maxIndex: 21,
		wrapPos:  []int{21, 20, 19, 18, 17, 16},
		testnum:  4,
	})

	for _, test := range tests {
		e := NewEntangler(5, 5, 5, 3, chunk.DefaultSize)
		e.NumDataBlocks = test.maxIndex
		e.setDatablocksToClose()
		assert.ElementsMatch(t, e.RightExtremeIndex, test.wrapPos, "Wrap positions did not match. Test number %d", test.testnum)
	}
}

func TestClosedEntanglement(t *testing.T) {
	alpha, s, p := 3, 5, 5
	inputSize := 1000              // 10 * 4k bytes
	chunkSize := chunk.DefaultSize // bytes
	tangler := NewEntangler(p, p, s, alpha, chunkSize)

	// Generate random data
	input := make([][]byte, inputSize)

	for i := 0; i < len(input); i++ {
		input[i] = testutil.RandomBytes(i, chunkSize)
	}

	resultChan := make(chan *EntangledBlock)
	done := make(chan struct{})

	// Setup
	entangledBlocks := make([]*EntangledBlock, 0)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer close(resultChan)
	ResultLoop:
		for {
			select {
			case block := <-resultChan:
				if block.Replace {
					for i := 0; i < len(entangledBlocks); i++ {
						if entangledBlocks[i].LeftIndex == block.LeftIndex &&
							entangledBlocks[i].RightIndex == block.RightIndex {
							entangledBlocks[i].Data = block.Data
							entangledBlocks[i].Replace = true
							break
						}
					}
				} else {
					entangledBlocks = append(entangledBlocks, block)
				}
			case <-done:
				break ResultLoop
			}
		}
		wg.Done()
	}()

	for i := 0; i < len(input); i++ {
		tangler.Entangle(input[i], i+1, resultChan)
	}
	tangler.WrapLattice(resultChan)
	done <- struct{}{}

	wg.Wait()

	var lp, rp *EntangledBlock
	var output []byte
	for j := 1; j <= len(input); j++ {
		for i := 0; i < alpha; i++ {
			lp, rp = nil, nil
			// Find left parity
			for k := 0; k < len(entangledBlocks); k++ {
				if int(entangledBlocks[k].Class) != i {
					continue
				}
				if entangledBlocks[k].LeftIndex == j {
					rp = entangledBlocks[k]
				} else if entangledBlocks[k].RightIndex == j && entangledBlocks[k].LeftIndex > 0 {
					lp = entangledBlocks[k]
				}
				if lp != nil && rp != nil {
					break // We found both parities, now we can XOR
				}
			}
			assert.NotNil(t, lp, "Left parity was nil. Index %d", j)
			assert.NotNil(t, rp, "Right parity was nil. Index %d", j)

			if lp.Replace == true {
				output = XORByteSlice(input[lp.LeftIndex-1], rp.Data)
			} else {
				output = XORByteSlice(lp.Data, rp.Data)
			}

			assert.Equal(t, input[j-1], output, "XOR value incorrect. Class: %d, Index: %d, Left Parity (L: %d, R: %d), Right Parity (L: %d, R: %d)",
				i, j, lp.LeftIndex, lp.RightIndex, rp.LeftIndex, rp.RightIndex)
		}
	}
}

func TestXOR(t *testing.T) {
	a := []byte{0xb9, 0x63}
	b := []byte{0x66, 0xcc}

	// Xor two parities
	c := XORByteSlice(a, b)

	// Compare hash
	cStr := fmt.Sprintf("%x", c)

	assert.Equal(t, "dfaf", cStr, "Hash should be equal")
}

func TestPadByteSlice(t *testing.T) {
	a := make([]byte, 50)
	acpy := make([]byte, 50)
	b := make([]byte, 20)
	bcpy := make([]byte, 20)

	c := make([]byte, 10)
	ccpy := make([]byte, 10)
	d := make([]byte, 70)
	dcpy := make([]byte, 70)

	lenA, lenB, lenC, lenD := len(a), len(b), len(c), len(d)
	rand.Read(a)
	rand.Read(b)
	rand.Read(c)
	rand.Read(d)

	copy(acpy, a)
	copy(bcpy, b)
	copy(ccpy, c)
	copy(dcpy, d)

	assert.Greater(t, len(a), len(b), "Length of slice A should be greater than B")
	assert.Greater(t, len(d), len(c), "Length of slice D should be greater than C")

	PadByteSlices(&a, &b, false)
	PadByteSlices(&c, &d, false)

	assert.Equal(t, lenA, len(b), "Length of slice A and B should be equal")
	assert.Equal(t, lenD, len(c), "Length of slice D and C should be equal")

	for i := 0; i < lenA; i++ {
		assert.Equal(t, a[i], acpy[i], "Byte values should be equal")
		if i < lenB {
			assert.Equal(t, b[i], bcpy[i], "Byte values should be equal")
		} else {
			assert.Equal(t, 0, int(a[i]^b[i]), "XOR of padded byte slice should be 0")
		}
	}

	for i := 0; i < lenD; i++ {
		assert.Equal(t, d[i], dcpy[i], "Byte values should be equal")
		if i < lenC {
			assert.Equal(t, c[i], ccpy[i], "Byte values should be equal")
		} else {
			assert.Equal(t, 0, int(d[i]^c[i]), "XOR of padded byte slice should be 0")
		}
	}
}
