package swarmconnector

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/storage"
	"github.com/relab/snarl-mw21/repair"
	"github.com/relab/snarl-mw21/utils"
	"github.com/stretchr/testify/assert"
)

func TestGetChildFromMem(t *testing.T) {
	type test struct {
		length     int
		canonIndex int
		testnum    int
	}

	var tests []*test

	tests = append(tests, &test{
		length:     chunk.DefaultSize / 2,
		canonIndex: 1,
		testnum:    0,
	})
	// Maximum size for a single chunk
	tests = append(tests, &test{
		length:     chunk.DefaultSize,
		canonIndex: 1,
		testnum:    1,
	})
	// One root. One full child, one partial child
	tests = append(tests, &test{
		length:     chunk.DefaultSize + (chunk.DefaultSize / 2),
		canonIndex: 3,
		testnum:    2,
	})
	tests = append(tests, &test{
		length:     chunk.DefaultSize * 2,
		canonIndex: 3,
		testnum:    3,
	})
	tests = append(tests, &test{
		length:     chunk.DefaultSize * 3,
		canonIndex: 4,
		testnum:    4,
	})
	tests = append(tests, &test{
		length:     chunk.DefaultSize * 127,
		canonIndex: 128,
		testnum:    5,
	})
	// One root and 128 children.
	tests = append(tests, &test{
		length:     chunk.DefaultSize * 128,
		canonIndex: 129,
		testnum:    6,
	})
	// One root, one intermediary, 129 children.
	tests = append(tests, &test{
		length:     chunk.DefaultSize*128 + 4064,
		canonIndex: 131,
		testnum:    7,
	})
	// One root, one intermediary, 129 children.
	tests = append(tests, &test{
		length:     chunk.DefaultSize*128 + 4095,
		canonIndex: 131,
		testnum:    8,
	})
	// One root, two intermediary, 129 children.
	tests = append(tests, &test{
		length:     chunk.DefaultSize * 129,
		canonIndex: 132,
		testnum:    9,
	})
	tests = append(tests, &test{
		length:     chunk.DefaultSize * 257,
		canonIndex: 261,
		testnum:    10,
	})
	// One root, 128 intermediary, 16384 children.
	tests = append(tests, &test{
		length:     chunk.DefaultSize * 128 * 128,
		canonIndex: 16513,
		testnum:    11,
	})
	// One root, 1 IM1, 128 IM2, 16385 children.
	tests = append(tests, &test{
		length:     chunk.DefaultSize*128*128 + 1337,
		canonIndex: 16515,
		testnum:    12,
	})
	// One root, 1 IM1, 128 IM2, 16385 children.
	tests = append(tests, &test{
		length:     chunk.DefaultSize*128*128 + 4095,
		canonIndex: 16515,
		testnum:    13,
	})
	// One root, 2 IM1, 128 IM2, 16385 children.
	tests = append(tests, &test{
		length:     chunk.DefaultSize*128*128 + 4096,
		canonIndex: 16516,
		testnum:    14,
	})
	// One root, 2 IM1, 128 IM2, 16511 children.
	tests = append(tests, &test{
		length:     chunk.DefaultSize*128*128 + chunk.DefaultSize*127,
		canonIndex: 16642,
		testnum:    15,
	})
	// One root, 2 IM1, 128 IM2, 16512 children.
	tests = append(tests, &test{
		length:     chunk.DefaultSize*128*128 + chunk.DefaultSize*127 + 4095,
		canonIndex: 16643,
		testnum:    16,
	})
	// One root, 2 IM1, 129 IM2, 16512 children.
	tests = append(tests, &test{
		length:     chunk.DefaultSize*128*128 + chunk.DefaultSize*128,
		canonIndex: 16644,
		testnum:    17,
	})
	// One root, 2 IM1, 130 IM2, 16513 children.
	tests = append(tests, &test{
		length:     chunk.DefaultSize*128*128 + (chunk.DefaultSize * 129),
		canonIndex: 16646,
		testnum:    18,
	})
	// One root, 2 IM1, 256 IM2, 32768 children.
	tests = append(tests, &test{
		length:     chunk.DefaultSize * 128 * 128 * 2,
		canonIndex: 33027,
		testnum:    19,
	})
	// One root, 3 IM1, 259 IM2, 33025 children.
	tests = append(tests, &test{
		length:     (chunk.DefaultSize * 128 * 128) + (chunk.DefaultSize * 128 * 128) + (chunk.DefaultSize * 128 * 2) + chunk.DefaultSize,
		canonIndex: 33288,
		testnum:    20,
	})
	// One root, 5 IM1, 640 IM2, 81920 children.
	tests = append(tests, &test{
		length:     chunk.DefaultSize * 128 * 128 * 5,
		canonIndex: 82566,
		testnum:    21,
	})

	dir, err := ioutil.TempDir("", "swarm-storage-")
	defer os.RemoveAll(dir)

	if err != nil {
		t.Fatalf("Could not create temp directory. Error: %s", err.Error())
	}

	var nextIndex int = 1
	var testNum int = 0

	var walker func(*TreeChunk, *TreeChunk)
	walker = func(parent, root *TreeChunk) {
		for j := range parent.Children {
			walker(parent.Children[j], root)
		}

		// Only check for children.
		if len(parent.Children) == 0 {
			cld, err := root.GetChildFromMem(nextIndex)
			if err != nil {
				t.Fatalf("Error retrieving child. %s", err.Error())
			}
			// Check equal size
			assert.Equal(t, parent.SubtreeSize, cld.SubtreeSize, "Got %d, Expected %d. Incorrect subtreesize. Key %v. Test number %d. Data: %v, Length: %d", cld.SubtreeSize, parent.SubtreeSize, parent.Key[:16], testNum, parent.Data[:utils.Min(8, len(parent.Data))], len(parent.Data))
			// Check equal key
			assert.Equal(t, parent.Key, cld.Key, "Got %d, Expected %d. Incorrect key. Test number %d. Data: %v, Length: %d, Size: %d", cld.Key[:16], parent.Key[:16], testNum, parent.Data[:utils.Min(20, len(parent.Data))], len(parent.Data), parent.SubtreeSize)
			nextIndex++
		}
	}
	for i, test := range tests {
		addr, reader, getter, err := utils.GenerateRandomData(test.length, storage.DefaultHash, dir)
		if err != nil {
			fmt.Printf("Got error on test %d. Error: %v", i, err)
		}
		nextIndex = 1
		testNum = test.testnum
		treeRoot, err := BuildCompleteTree(reader.Context(), getter, storage.Reference(addr),
			BuildTreeOptions{EmptyLeaves: true}, repair.NewMockRepair(getter))
		if err != nil {
			t.Fatal(err.Error())
		}

		walker(treeRoot, treeRoot)
		assert.Equal(t, treeRoot.Index, test.canonIndex, "Got %d, Expected %d. Incorrect index for chunk with hash %v. Test number %d. Data: %v, Length: %d, Size: %d", treeRoot.Index, test.canonIndex, treeRoot.Key, testNum, treeRoot.Data, len(treeRoot.Data), treeRoot.SubtreeSize)

	}
}

func TestFlattenTree(t *testing.T) {
	var tests = []struct {
		length     int
		canonIndex int
		testnum    int
	}{
		{chunk.DefaultSize / 2, 1, 0},
		{chunk.DefaultSize, 1, 1},                           // Maximum size for a single chunk
		{chunk.DefaultSize + (chunk.DefaultSize / 2), 3, 2}, // One root. One full child, one partial child
		{chunk.DefaultSize * 2, 3, 3},
		{chunk.DefaultSize * 3, 4, 4},
		{chunk.DefaultSize * 127, 128, 5},
		{chunk.DefaultSize * 128, 129, 6},      // One root and 128 children.
		{chunk.DefaultSize*128 + 4064, 131, 7}, // One root, one intermediary, 129 children.
		{chunk.DefaultSize*128 + 4095, 131, 8}, // One root, one intermediary, 129 children.
		{chunk.DefaultSize * 129, 132, 9},      // One root, two intermediary, 129 children.
		{chunk.DefaultSize * 257, 261, 10},
		{chunk.DefaultSize * 128 * 128, 16513, 11},                            // One root, 128 IM1, 16384 children.
		{chunk.DefaultSize*128*128 + 1337, 16515, 12},                         // One root, 1 IM1, 128 IM2, 16385 children.
		{chunk.DefaultSize*128*128 + 4095, 16515, 13},                         // One root, 1 IM1, 128 IM2, 16385 children.
		{chunk.DefaultSize*128*128 + 4096, 16516, 14},                         // One root, 2 IM1, 128 IM2, 16385 children.
		{chunk.DefaultSize*128*128 + chunk.DefaultSize*127, 16642, 15},        // One root, 2 IM1, 128 IM2, 16511 children.
		{chunk.DefaultSize*128*128 + chunk.DefaultSize*127 + 4095, 16643, 16}, // One root, 2 IM1, 128 IM2, 16512 children.
		{chunk.DefaultSize*128*128 + chunk.DefaultSize*128, 16644, 17},        // One root, 2 IM1, 129 IM2, 16512 children.
		{chunk.DefaultSize*128*128 + (chunk.DefaultSize * 129), 16646, 18},    // One root, 2 IM1, 130 IM2, 16513 children.
		{chunk.DefaultSize * 128 * 128 * 2, 33027, 19},                        // One root, 2 IM1, 256 IM2, 32768 children.
		{chunk.DefaultSize * 33025, 33288, 20},                                // One root, 3 IM1, 259 IM2, 33025 children.
		{chunk.DefaultSize * 128 * 128 * 5, 82566, 21},                        // One root, 5 IM1, 640 IM2, 81920 children.
		{1048576, 259, 22},                                                    // 1 MB File
		{chunk.DefaultSize*256 + 10, 260, 23},                                 // 1 MB + 10 byte file
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
		treeRoot, err := BuildCompleteTree(reader.Context(), getter, storage.Reference(addr),
			BuildTreeOptions{EmptyLeaves: true}, repair.NewMockRepair(getter))
		if err != nil {
			t.Fatal(err.Error())
		}

		flatTree := treeRoot.FlattenTree()
		for i := 0; i < len(flatTree); i++ {
			assert.Equal(t, len(flatTree[i].Key), chunk.AddressLength, "Incorrect length of key")
			assert.Equal(t, i+1, flatTree[i].Index, "Tree chunks are not in canonical order")
		}

		assert.Equal(t, test.canonIndex, len(flatTree), "Flat tree is not correct size")
	}
}

func TestFlattenTreeDependency(t *testing.T) {
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
		{chunk.DefaultSize*128*128 + chunk.DefaultSize*128, 16644, 0, 17},        // One root, 2 IM1, 129 IM2, 16512 children.
		{chunk.DefaultSize*128*128 + (chunk.DefaultSize * 129), 16646, 0, 18},    // One root, 2 IM1, 130 IM2, 16513 children.
		{chunk.DefaultSize * 128 * 128 * 2, 33027, 0, 19},                        // One root, 2 IM1, 256 IM2, 32768 children.
		{chunk.DefaultSize * 33025, 33288, 0, 20},                                // One root, 3 IM1, 259 IM2, 33025 children.
		{chunk.DefaultSize * 128 * 128 * 5, 82566, 0, 21},                        // One root, 5 IM1, 640 IM2, 81920 children.
		{1048576, 259, 0, 22},                                                    // 1 MB File
		{chunk.DefaultSize*256 + 10, 260, 0, 23},                                 // 1 MB + 10 byte file
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
		treeRoot, err := BuildCompleteTree(reader.Context(), getter, storage.Reference(addr),
			BuildTreeOptions{EmptyLeaves: true}, repair.NewMockRepair(getter))
		if err != nil {
			t.Fatal(err.Error())
		}

		S, P := 5, 5
		flatTree := treeRoot.FlattenTreeWindow(S, P)
		entangleWindow := S * P
		assert.Equal(t, test.canonIndex, len(flatTree), "Flat tree is not correct size")
		// We check for dependencies by:
		// No internal node must be less than S*P positions closer on the left or right side to any of its children.
		internalNodes := make(map[int]*TreeChunk)
		for i := 0; i < len(flatTree); i++ {
			assert.Equal(t, len(flatTree[i].Key), chunk.AddressLength, "Incorrect length of key")
			if len(flatTree[i].Children) > 0 && i != treeRoot.Index-1 {
				internalNodes[i] = flatTree[i]
			}
		}
		inWindowCnt := 0
		for canonIndex, tc := range internalNodes {
			lowestChild := tc.Children[0].Index
			highestChild := tc.Children[len(tc.Children)-1].Index

			for i := utils.Max(canonIndex+1-entangleWindow, 0); i < utils.Min(canonIndex+1+entangleWindow+S, len(flatTree)-1); i++ {
				inWindow := flatTree[i].Index >= lowestChild && flatTree[i].Index <= highestChild
				if inWindow {
					inWindowCnt++
				}

			}
		}
		assert.Equal(t, test.inWindowCnt, inWindowCnt, "Number of dependencies is too high. (Or too low?). Testnum: %v", test.testnum)
	}
}

func TestString(t *testing.T) {
	var tests = []struct {
		length     int
		canonIndex int
		testnum    int
	}{
		{chunk.DefaultSize, 1, 1},
		{3 * chunk.DefaultSize, 4, 2},
		{128 * chunk.DefaultSize, 129, 3},
		{129 * chunk.DefaultSize, 132, 4},
		{16384 * chunk.DefaultSize, 16513, 5},
		{16385 * chunk.DefaultSize, 16516, 6},
		{chunk.DefaultSize*128*128 + chunk.DefaultSize*129, 16646, 7},
		{256 * chunk.DefaultSize, 259, 8},      // 1 MB
		{1280 * chunk.DefaultSize, 1291, 9},    // 5 MB
		{2560 * chunk.DefaultSize, 2581, 10},   // 10 MB
		{6400 * chunk.DefaultSize, 6451, 10},   // 25 MB
		{12800 * chunk.DefaultSize, 12901, 10}, // 50 MB
		{19200 * chunk.DefaultSize, 19353, 10}, // 75 MB
		{25600 * chunk.DefaultSize, 25803, 11}, // 100 MB
	}

	dir, err := ioutil.TempDir("", "swarm-storage-")
	defer os.RemoveAll(dir)

	if err != nil {
		t.Fatalf("Could not create temp directory. Error: %v", err.Error())
	}

	for _, test := range tests {
		addr, reader, getter, err := utils.GenerateRandomData(test.length, storage.DefaultHash, dir)
		if err != nil {
			fmt.Printf("Got error on test %d. Error: %v", test.testnum, err)
		}

		treeRoot, err := BuildCompleteTree(reader.Context(), getter, storage.Reference(addr),
			BuildTreeOptions{EmptyLeaves: true}, repair.NewMockRepair(getter))
		if err != nil {
			t.Fatal(err.Error())
		}

		output := fmt.Sprint(treeRoot)

		assert.Equal(t, test.canonIndex, strings.Count(output, ":"))
	}
}
