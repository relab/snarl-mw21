package entangler

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/storage"
	"github.com/relab/snarl-mw21/repair"
	"github.com/relab/snarl-mw21/swarmconnector"
	"github.com/relab/snarl-mw21/utils"
	"github.com/stretchr/testify/assert"
)

func TestGetNeighbours(t *testing.T) {
	var tests = []struct {
		index      int
		direction  bool // True: Right, False: Left
		neighbours []int
	}{
		{128, true, []int{126, 127, 128, 129, 130, 131, 132, 133, 134, 135}},
		{128, false, []int{121, 122, 123, 124, 125, 126, 127, 128, 129, 130}},
		{1, true, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
		{1, false, []int{251, 256, 257, 255, 254, 258, 259, 1, 2, 3, 4, 5}},
		{257, true, []int{251, 256, 257, 255, 254, 258, 259, 1, 2, 3, 4, 5}},
		{257, false, []int{1, 4, 251, 252, 253, 254, 255, 256, 257, 258, 259, 5}},
	}
	ts := NewTestSetup(256*chunk.DefaultSize, 3, 5, 5)
	for i, test := range tests {
		lattice := NewSwarmLattice(context.TODO(), ts.Alpha, ts.S, ts.P, ts.Filesize, nil, nil, nil, chunk.DefaultSize)
		lattice.RunInit()

		block := lattice.Blocks[test.index-1]
		neighbours := lattice.GetNeighbours(block, test.direction)
		var neighbourIndexes []int = make([]int, len(neighbours))
		for j := 0; j < len(neighbours); j++ {
			neighbourIndexes[j] = neighbours[j].Position
		}
		assert.ElementsMatch(t, neighbourIndexes, test.neighbours, "Neighbours did not contain same elements.. Test %v. Elements: %v", i, neighbourIndexes)
	}

}
func TestLatticeRunInit(t *testing.T) {
	var tests = []struct {
		size         uint64
		numblocks    int
		alpha        int
		s            int
		p            int
		swarmlattice bool
	}{
		// Regular Lattice
		{7*chunk.DefaultSize + 3500, 9, 3, 5, 5, false},
		{25*chunk.DefaultSize + 1, 27, 3, 5, 5, false},
		{650 * chunk.DefaultSize, 657, 3, 5, 5, false},
		{5500*chunk.DefaultSize + 250, 5545, 3, 5, 5, false},

		// Swarm lattice
		{7*chunk.DefaultSize + 3500, 9, 3, 5, 5, true},
		{25*chunk.DefaultSize + 1, 27, 3, 5, 5, true},
		{650 * chunk.DefaultSize, 657, 3, 5, 5, true},
		{5500*chunk.DefaultSize + 250, 5545, 3, 5, 5, true},
		{128 * 128 * 2 * chunk.DefaultSize, 33027, 3, 5, 5, true},
	}

	a := []int{}
	h := func(start, end, inc int) []int {
		ret := make([]int, 0)
		i := start
		for ; i <= end; i += inc {
			ret = append(ret, utils.Min(i, end))
		}
		if i-inc < end {
			ret = append(ret, end)
		}
		return ret
	}
	g := func(start, end int) []int {
		return h(start, end, 1)
	}

	swarmHierNum := -1
	var swarmHierarchy = []struct {
		parents  map[int]int
		children map[int][]int
	}{
		{map[int]int{1: 9, 2: 9, 3: 9, 8: 9, 9: 0}, map[int][]int{1: a, 2: a, 3: a, 4: a, 9: g(1, 8)}},
		{map[int]int{1: 27, 2: 27, 26: 27, 27: 0}, map[int][]int{1: a, 2: a, 3: a, 4: a, 26: a, 27: g(1, 26)}},
		{map[int]int{1: 129, 5: 129, 150: 258, 129: 657, 256: 258, 656: 657, 657: 0, 381: 387}, map[int][]int{1: a, 5: a, 150: a,
			129: g(1, 128), 256: a, 258: g(130, 257), 259: a, 387: g(259, 386), 657: h(129, 656, 129), 656: g(646, 655)}},
		{map[int]int{}, map[int][]int{}},
		{map[int]int{1: 129, 5: 129, 150: 258, 129: 16513, 256: 258, 656: 774, 33027: 0, 16511: 16512, 16512: 16513, 33024: 33025, 33025: 33026, 33026: 33027, 16513: 33027},
			map[int][]int{1: a, 5: a, 150: a, 129: g(1, 128), 256: a, 258: g(130, 257), 259: a, 774: g(646, 773), 16511: a, 16512: g(16384, 16511), 16513: h(129, 16512, 129),
				33027: {16513, 33026}}},
	}

	// Test NewLattice
	for i, test := range tests {
		var lattice *Lattice
		if test.swarmlattice {
			lattice = NewSwarmLattice(context.TODO(), test.alpha, test.s, test.p, test.size, nil, nil, nil, chunk.DefaultSize)
			swarmHierNum++
		} else {
			lattice = NewLattice(context.TODO(), test.alpha, test.s, test.p, test.numblocks)
		}

		lattice.RunInit()

		assert.Equal(t, test.numblocks*(test.alpha+1), len(lattice.Blocks), "Number of blocks in lattice did not match. Test %v, Swarmlattice: %v", i, test.swarmlattice)
		assert.Equal(t, test.numblocks, lattice.NumDataBlocks, "Number of DATA blocks in lattice did not match. Test %v, Swarmlattice: %v", i, test.swarmlattice)

		for j := 0; j < lattice.NumDataBlocks; j++ {
			theBlock := lattice.GetBlock(j + 1)
			assert.Equal(t, false, theBlock.IsParity, "Data block marked as parity. Test %v, Swarmlattice: %v", i, test.swarmlattice)

			if test.swarmlattice {
				sh := swarmHierarchy[swarmHierNum]
				translatedBlock := lattice.GetBlock(theBlock.Position)
				if par, ok := sh.parents[theBlock.Position]; ok {
					if par == 0 {
						assert.Nil(t, theBlock.Parent, "Root should not have a parent. Numblocks: %v, i: %v", test.numblocks, i)
					} else {
						assert.Equal(t, par, translatedBlock.Parent.Position, "Wrong parent. Block: %v, Parent actual: %v, Parent expect: %v, Numblocks: %v, i: %v",
							theBlock.Position, translatedBlock.Parent.Position, par, test.numblocks, i)
					}
				}
				if childs, ok := sh.children[theBlock.Position]; ok {
					blockchilds := make([]int, len(translatedBlock.Children))
					for k := 0; k < len(translatedBlock.Children); k++ {
						blockchilds[k] = translatedBlock.Children[k].Position
					}
					assert.ElementsMatchf(t, childs, blockchilds, "Wrong children. Numblocks: %v, i: %v", test.numblocks, i)
				}
			}
		}

		for j := 0; j < lattice.NumDataBlocks; j++ {
			for k := 0; k < lattice.Alpha; k++ {
				leftParity := lattice.Blocks[j].Left[k]
				rightParity := lattice.Blocks[j].Right[k]

				assert.NotNil(t, leftParity, "Left parity is nil. Index %v, Test %v, Alpha %v. Block %v, Swarmlattice: %v", j, i, k, lattice.Blocks[j], test.swarmlattice)
				assert.NotNil(t, rightParity, "Right parity is nil Index %v, Test %v, Alpha %v, Swarmlattice: %v", j, i, k, test.swarmlattice)

				if lattice.NumDataBlocks > 10 {
					assert.NotEqual(t, leftParity, rightParity, "Parities must be different.")
				}

				assert.Equal(t, leftParity.Class, rightParity.Class, "Class not equal")
				assert.Equal(t, leftParity.Class, StrandClass(k), "Class not equal")

				assert.Equal(t, leftParity.Right[0], rightParity.Left[0], "Left and right parity are not connected to same data block. Index %v, Test %v, Swarmlattice: %v", j, i, test.swarmlattice)
				assert.Equal(t, leftParity.Right[0], lattice.Blocks[j], "Left and right parity are not connected to the correct data block. Index %v, Test %v, Swarmlattice: %v", j, i, test.swarmlattice)
			}
		}
	}
}

func TestCreateInternalNodeShift(t *testing.T) {
	var tests = []struct {
		length      int
		canonIndex  int
		inWindowCnt int
		testnum     int
	}{
		{chunk.DefaultSize / 2, 1, 0, 0},
		{chunk.DefaultSize, 1, 0, 1},                           // Maximum size for a single chunk
		{chunk.DefaultSize + (chunk.DefaultSize / 2), 3, 0, 2}, // One root. One full child, one partial child
		{chunk.DefaultSize * 2, 3, 0, 3},
		{chunk.DefaultSize * 3, 4, 0, 4},
		{chunk.DefaultSize * 127, 128, 0, 5},
		{chunk.DefaultSize * 128, 129, 0, 6},       // One root and 128 children.
		{chunk.DefaultSize*128 + 4064, 131, 24, 7}, // One root, one intermediary, 129 children.
		{chunk.DefaultSize*128 + 4095, 131, 24, 8}, // One root, one intermediary, 129 children.
		{chunk.DefaultSize * 129, 132, 25, 9},      // One root, two intermediary, 129 children.
		{chunk.DefaultSize * 257, 261, 0, 10},
		{chunk.DefaultSize * 128 * 128, 16513, 0, 11},                            // One root, 128 IM1, 16384 children.
		{chunk.DefaultSize*128*128 + 1337, 16515, 24, 12},                        // One root, 1 IM1, 128 IM2, 16385 children.
		{chunk.DefaultSize*128*128 + 4095, 16515, 24, 13},                        // One root, 1 IM1, 128 IM2, 16385 children.
		{chunk.DefaultSize*128*128 + 4096, 16516, 25, 14},                        // One root, 2 IM1, 128 IM2, 16385 children.
		{chunk.DefaultSize*128*128 + chunk.DefaultSize*127, 16642, 0, 15},        // One root, 2 IM1, 128 IM2, 16511 children.
		{chunk.DefaultSize*128*128 + chunk.DefaultSize*127 + 4095, 16643, 0, 16}, // One root, 2 IM1, 128 IM2, 16512 children.
	}

	dir, err := ioutil.TempDir("", "swarm-storage-")
	defer os.RemoveAll(dir)

	if err != nil {
		t.Fatalf("Could not create temp directory. Error: %v", err.Error())
	}

	for i, test := range tests {
		addr, reader, getter, err := utils.GenerateRandomData(test.length, storage.DefaultHash, dir)
		if err != nil {
			fmt.Printf("Got error on test %d. Error: %v", i, err)
		}
		treeRoot, err := swarmconnector.BuildCompleteTree(reader.Context(), getter, storage.Reference(addr),
			swarmconnector.BuildTreeOptions{EmptyLeaves: true}, repair.NewMockRepair(getter))
		if err != nil {
			t.Fatal(err.Error())
		}

		S, P := 5, 5
		flatTree := treeRoot.FlattenTreeWindow(S, P)
		ts := NewTestSetup(uint64(test.length), 3, 5, 5)
		lattice := NewSwarmLattice(context.TODO(), ts.Alpha, ts.S, ts.P, ts.Filesize, nil, nil, nil, chunk.DefaultSize)
		lattice.RunInit()

		for j := 0; j < len(flatTree); j++ {
			b := lattice.GetBlock(flatTree[j].Index)
			assert.Equal(t, j+1, b.Position)
		}
	}
}
