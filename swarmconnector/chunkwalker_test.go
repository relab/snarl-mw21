package swarmconnector

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/storage"
	"github.com/ethersphere/swarm/testutil"
	"github.com/relab/snarl-mw21/repair"
	"github.com/relab/snarl-mw21/utils"
	"github.com/stretchr/testify/assert"
)

func TestRemoveDecryptionKey(t *testing.T) {
	dir := t.TempDir()
	sc := NewSwarmConnector(dir, "testing", dir)

	testRuns := []int{sc.HashSize / 2, sc.HashSize, sc.HashSize + (sc.HashSize / 2), sc.HashSize * 2, sc.HashSize * 3}

	for i, test := range testRuns {
		input := testutil.RandomBytes(i, test)

		output := sc.removeDecryptionKeyFromChunkHash(input)

		assert.LessOrEqual(t, len(output), sc.HashSize, "Output is not correct length")
		assert.Subset(t, input, output, "Output is not a subset of input")
	}
}

func BenchmarkGetTreeIndexBySize(b *testing.B) {
	tests := []struct {
		chunkSize uint64
		index     int
		desc      string
	}{
		{chunkSize: 2048, index: 1, desc: "Half size of a single chunk"},
		{chunkSize: 4096, index: 1, desc: "Maximum size for a single chunk"},
		{chunkSize: 6144, index: 3, desc: "One root. One full child, one partial child"},
		{chunkSize: 8192, index: 3, desc: "One root. Two full children."},
		{chunkSize: 12288, index: 4, desc: "One root. Three full children."},
		{chunkSize: 520192, index: 128, desc: "One root and 127 children."},
		{chunkSize: 524288, index: 129, desc: "One root and 128 children."},
		{chunkSize: 528352, index: 131, desc: "One root, one intermediary, 129 children."},
		{chunkSize: 528383, index: 131, desc: "One root, one intermediary, 129 children."},
		{chunkSize: 528384, index: 132, desc: "One root, two intermediary, 129 children."},
		{chunkSize: 1052672, index: 261, desc: "One root, two intermediary, 257 children."},
		{chunkSize: 67108864, index: 16513, desc: "One root, 128 intermediary, 16384 children."},
		{chunkSize: 67110201, index: 16515, desc: "One root, 1 IM1, 128 IM2, 16385 children."},
		{chunkSize: 67112959, index: 16515, desc: "One root, 1 IM1, 128 IM2, 16385 children."},
		{chunkSize: 67112960, index: 16516, desc: "One root, 2 IM1, 128 IM2, 16385 children."},
		{chunkSize: 67629056, index: 16642, desc: "One root, 2 IM1, 128 IM2, 16511 children."},
		{chunkSize: 67633151, index: 16643, desc: "One root, 2 IM1, 128 IM2, 16512 children."},
		{chunkSize: 67633152, index: 16644, desc: "One root, 2 IM1, 129 IM2, 16512 children."},
		{chunkSize: 67637248, index: 16646, desc: "One root, 2 IM1, 130 IM2, 16513 children."},
		{chunkSize: 134217728, index: 33027, desc: "One root, 2 IM1, 256 IM2, 32768 children."},
		{chunkSize: 135270400, index: 33288, desc: "One root, 3 IM1, 259 IM2, 33025 children."},
		{chunkSize: 335544320, index: 82566, desc: "One root, 5 IM1, 640 IM2, 81920 children."},
		{chunkSize: 1048576, index: 259, desc: "1 MB File"},
		{chunkSize: 1048586, index: 260, desc: "1 MB + 10 byte file"},
		{chunkSize: 104857600, index: 25803, desc: "100 MB - 104 857 600"},
		{chunkSize: 1048576000, index: 258017, desc: "1000 MB - 104 857 6000 - 256 000 chunks  1 root, 16 IM1, 2000 IM2, 256000 children."},
	}
	for _, test := range tests {
		b.Run(test.desc, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = GetTreeIndexBySize(test.chunkSize)
			}
		})
	}
}

func TestGetTreeIndexBySize(t *testing.T) {
	tests := []struct {
		chunkSize uint64
		index     int
		desc      string
	}{
		{chunkSize: 2048, index: 1, desc: "Half size of a single chunk"},
		{chunkSize: 4096, index: 1, desc: "Maximum size for a single chunk"},
		{chunkSize: 6144, index: 3, desc: "One root. One full child, one partial child"},
		{chunkSize: 8192, index: 3, desc: "One root. Two full children."},
		{chunkSize: 12288, index: 4, desc: "One root. Three full children."},
		{chunkSize: 520192, index: 128, desc: "One root and 127 children."},
		{chunkSize: 524288, index: 129, desc: "One root and 128 children."},
		{chunkSize: 528352, index: 131, desc: "One root, one intermediary, 129 children."},
		{chunkSize: 528383, index: 131, desc: "One root, one intermediary, 129 children."},
		{chunkSize: 528384, index: 132, desc: "One root, two intermediary, 129 children."},
		{chunkSize: 1052672, index: 261, desc: "One root, two intermediary, 257 children."},
		{chunkSize: 67108864, index: 16513, desc: "One root, 128 intermediary, 16384 children."},
		{chunkSize: 67110201, index: 16515, desc: "One root, 1 IM1, 128 IM2, 16385 children."},
		{chunkSize: 67112959, index: 16515, desc: "One root, 1 IM1, 128 IM2, 16385 children."},
		{chunkSize: 67112960, index: 16516, desc: "One root, 2 IM1, 128 IM2, 16385 children."},
		{chunkSize: 67629056, index: 16642, desc: "One root, 2 IM1, 128 IM2, 16511 children."},
		{chunkSize: 67633151, index: 16643, desc: "One root, 2 IM1, 128 IM2, 16512 children."},
		{chunkSize: 67633152, index: 16644, desc: "One root, 2 IM1, 129 IM2, 16512 children."},
		{chunkSize: 67637248, index: 16646, desc: "One root, 2 IM1, 130 IM2, 16513 children."},
		{chunkSize: 134217728, index: 33027, desc: "One root, 2 IM1, 256 IM2, 32768 children."},
		{chunkSize: 135270400, index: 33288, desc: "One root, 3 IM1, 259 IM2, 33025 children."},
		{chunkSize: 335544320, index: 82566, desc: "One root, 5 IM1, 640 IM2, 81920 children."},
		{chunkSize: 1048576, index: 259, desc: "1 MB File"},
		{chunkSize: 1048586, index: 260, desc: "1 MB + 10 byte file"},
		{chunkSize: 104857600, index: 25803, desc: "100 MB - 104 857 600"},
		{chunkSize: 1048576000, index: 258017, desc: "1000 MB - 104 857 6000 - 256 000 chunks  1 root, 16 IM1, 2000 IM2, 256000 children."},
	}
	for _, test := range tests {
		gotIndex := GetTreeIndexBySize(test.chunkSize)
		if gotIndex != test.index {
			t.Errorf("GetTreeIndexBySize(%d) = %d, expected %d", test.chunkSize, gotIndex, test.index)
		}
	}
}

func TestGetTreeIndex(t *testing.T) {
	tests := []struct {
		length     int
		canonIndex int
		testnum    int
		desc       string
	}{
		{length: chunk.DefaultSize / 2, canonIndex: 1, testnum: 0, desc: "Half size of a single chunk"},
		{length: chunk.DefaultSize, canonIndex: 1, testnum: 1, desc: "Maximum size for a single chunk"},
		{length: chunk.DefaultSize + (chunk.DefaultSize / 2), canonIndex: 3, testnum: 2, desc: "One root. One full child, one partial child"},
		{length: chunk.DefaultSize * 2, canonIndex: 3, testnum: 3, desc: "One root. Two full children."},
		{length: chunk.DefaultSize * 3, canonIndex: 4, testnum: 4, desc: "One root. Three full children."},
		{length: chunk.DefaultSize * 127, canonIndex: 128, testnum: 5, desc: "One root and 127 children."},
		{length: chunk.DefaultSize * 128, canonIndex: 129, testnum: 6, desc: "One root and 128 children."},
		{length: chunk.DefaultSize*128 + 4064, canonIndex: 131, testnum: 7, desc: "One root, one intermediary, 129 children."},
		{length: chunk.DefaultSize*128 + 4095, canonIndex: 131, testnum: 8, desc: "One root, one intermediary, 129 children."},
		{length: chunk.DefaultSize * 129, canonIndex: 132, testnum: 9, desc: "One root, two intermediary, 129 children."},
		{length: chunk.DefaultSize * 257, canonIndex: 261, testnum: 10, desc: "One root, two intermediary, 257 children."},
		{length: chunk.DefaultSize * 128 * 128, canonIndex: 16513, testnum: 11, desc: "One root, 128 intermediary, 16384 children."},
		{length: chunk.DefaultSize*128*128 + 1337, canonIndex: 16515, testnum: 12, desc: "One root, 1 IM1, 128 IM2, 16385 children."},
		{length: chunk.DefaultSize*128*128 + 4095, canonIndex: 16515, testnum: 13, desc: "One root, 1 IM1, 128 IM2, 16385 children."},
		{length: chunk.DefaultSize*128*128 + 4096, canonIndex: 16516, testnum: 14, desc: "One root, 2 IM1, 128 IM2, 16385 children."},
		{length: chunk.DefaultSize*128*128 + chunk.DefaultSize*127, canonIndex: 16642, testnum: 15, desc: "One root, 2 IM1, 128 IM2, 16511 children."},
		{length: chunk.DefaultSize*128*128 + chunk.DefaultSize*127 + 4095, canonIndex: 16643, testnum: 16, desc: "One root, 2 IM1, 128 IM2, 16512 children."},
		{length: chunk.DefaultSize*128*128 + chunk.DefaultSize*128, canonIndex: 16644, testnum: 17, desc: "One root, 2 IM1, 129 IM2, 16512 children."},
		{length: chunk.DefaultSize*128*128 + (chunk.DefaultSize * 129), canonIndex: 16646, testnum: 18, desc: "One root, 2 IM1, 130 IM2, 16513 children."},
		{length: chunk.DefaultSize * 128 * 128 * 2, canonIndex: 33027, testnum: 19, desc: "One root, 2 IM1, 256 IM2, 32768 children."},
		{length: (chunk.DefaultSize * 128 * 128) + (chunk.DefaultSize * 128 * 128) + (chunk.DefaultSize * 128 * 2) + chunk.DefaultSize, canonIndex: 33288, testnum: 20, desc: "One root, 3 IM1, 259 IM2, 33025 children."},
		{length: chunk.DefaultSize * 128 * 128 * 5, canonIndex: 82566, testnum: 21, desc: "One root, 5 IM1, 640 IM2, 81920 children."},
		{length: 1048576, canonIndex: 259, testnum: 22, desc: "1 MB File"},
		{length: chunk.DefaultSize*256 + 10, canonIndex: 260, testnum: 23, desc: "1 MB + 10 byte file"},
		{length: 104857600, canonIndex: 25803, testnum: 24, desc: "100 MB - 104 857 600"},
		{length: 1048576000, canonIndex: 258017, testnum: 25, desc: "1000 MB - 104 857 6000 - 256 000 chunks  1 root, 16 IM1, 2000 IM2, 256000 children."},
	}

	dir := t.TempDir()

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			addr, reader, getter, err := utils.GenerateRandomData(test.length, storage.DefaultHash, dir)
			if err != nil {
				t.Errorf("Failed to generate random data for test %d. Error: %v", test.testnum, err)
			}

			chunkdata, _ := getter.Get(reader.Context(), storage.Reference(addr))
			rootChunk := chunk.NewChunk(addr, chunkdata)
			gotIndex := GetTreeIndex(rootChunk)

			assert.Equal(t, test.canonIndex, gotIndex, "Got %v, Expected %v. Canonical index did not match. Test number: %d", gotIndex, test.canonIndex, test.testnum)
		})
	}
}

func TestBuildCompleteTree(t *testing.T) {
	tests := []struct {
		length     int
		canonIndex int
		testnum    int
		desc       string
	}{
		{length: chunk.DefaultSize / 2, canonIndex: 1, testnum: 0, desc: "Half size of a single chunk"},
		{length: chunk.DefaultSize, canonIndex: 1, testnum: 1, desc: "Maximum size for a single chunk"},
		{length: chunk.DefaultSize + (chunk.DefaultSize / 2), canonIndex: 3, testnum: 2, desc: "One root. One full child, one partial child"},
		{length: chunk.DefaultSize * 2, canonIndex: 3, testnum: 3, desc: "One root. Two full children."},
		{length: chunk.DefaultSize * 3, canonIndex: 4, testnum: 4, desc: "One root. Three full children."},
		{length: chunk.DefaultSize * 127, canonIndex: 128, testnum: 5, desc: "One root and 127 children."},
		{length: chunk.DefaultSize * 128, canonIndex: 129, testnum: 6, desc: "One root and 128 children."},
		{length: chunk.DefaultSize*128 + 4064, canonIndex: 131, testnum: 7, desc: "One root, one intermediary, 129 children."},
		{length: chunk.DefaultSize*128 + 4095, canonIndex: 131, testnum: 8, desc: "One root, one intermediary, 129 children."},
		{length: chunk.DefaultSize * 129, canonIndex: 132, testnum: 9, desc: "One root, two intermediary, 129 children."},
		{length: chunk.DefaultSize * 257, canonIndex: 261, testnum: 10, desc: "One root, two intermediary, 257 children."},
		{length: chunk.DefaultSize * 128 * 128, canonIndex: 16513, testnum: 11, desc: "One root, 128 intermediary, 16384 children."},
		{length: chunk.DefaultSize*128*128 + 1337, canonIndex: 16515, testnum: 12, desc: "One root, 1 IM1, 128 IM2, 16385 children."},
		{length: chunk.DefaultSize*128*128 + 4095, canonIndex: 16515, testnum: 13, desc: "One root, 1 IM1, 128 IM2, 16385 children."},
		{length: chunk.DefaultSize*128*128 + 4096, canonIndex: 16516, testnum: 14, desc: "One root, 2 IM1, 128 IM2, 16385 children."},
		{length: chunk.DefaultSize*128*128 + chunk.DefaultSize*127, canonIndex: 16642, testnum: 15, desc: "One root, 2 IM1, 128 IM2, 16511 children."},
		{length: chunk.DefaultSize*128*128 + chunk.DefaultSize*127 + 4095, canonIndex: 16643, testnum: 16, desc: "One root, 2 IM1, 128 IM2, 16512 children."},
		{length: chunk.DefaultSize*128*128 + chunk.DefaultSize*128, canonIndex: 16644, testnum: 17, desc: "One root, 2 IM1, 129 IM2, 16512 children."},
		{length: chunk.DefaultSize*128*128 + (chunk.DefaultSize * 129), canonIndex: 16646, testnum: 18, desc: "One root, 2 IM1, 130 IM2, 16513 children."},
		{length: chunk.DefaultSize * 128 * 128 * 2, canonIndex: 33027, testnum: 19, desc: "One root, 2 IM1, 256 IM2, 32768 children."},
		{length: (chunk.DefaultSize * 128 * 128) + (chunk.DefaultSize * 128 * 128) + (chunk.DefaultSize * 128 * 2) + chunk.DefaultSize, canonIndex: 33288, testnum: 20, desc: "One root, 3 IM1, 259 IM2, 33025 children."},
		{length: chunk.DefaultSize * 128 * 128 * 5, canonIndex: 82566, testnum: 21, desc: "One root, 5 IM1, 640 IM2, 81920 children."},
		{length: 1048576, canonIndex: 259, testnum: 22, desc: "1 MB File"},
		{length: chunk.DefaultSize*256 + 10, canonIndex: 260, testnum: 23, desc: "1 MB + 10 byte file"},
		{length: 104857600, canonIndex: 25803, testnum: 24, desc: "100 MB - 104 857 600"},
		{length: 1048576000, canonIndex: 258017, testnum: 25, desc: "1000 MB - 104 857 6000 - 256 000 chunks  1 root, 16 IM1, 2000 IM2, 256000 children."},
	}
	dir := t.TempDir()

	var nextIndex int = 1
	var testNum int = 0

	var walker func(*TreeChunk)
	walker = func(parent *TreeChunk) {
		for j := range parent.Children {
			walker(parent.Children[j])
		}
		assert.Equal(t, nextIndex, parent.Index, "Got %d, Expected %d. Incorrect index for chunk with hash %v. Test number %d. Data: %v, Length: %d, Size: %d", parent.Index, nextIndex, parent.Key[:16], testNum, parent.Data[:utils.Min(len(parent.Data), 32)], len(parent.Data), parent.SubtreeSize)
		nextIndex++
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

		walker(treeRoot)
		assert.Equal(t, treeRoot.Index, test.canonIndex, "Got %d, Expected %d. Incorrect index for chunk with hash %v. Test number %d. Data: %v, Length: %d, Size: %d", treeRoot.Index, test.canonIndex, treeRoot.Key[:16], testNum, treeRoot.Data[:32], len(treeRoot.Data), treeRoot.SubtreeSize)
	}
}

func TestGetChildFromNet(t *testing.T) {
	tests := []struct {
		length     int
		canonIndex int
		testnum    int
		desc       string
	}{
		{length: chunk.DefaultSize / 2, canonIndex: 1, testnum: 0, desc: "Half size of a single chunk"},
		{length: chunk.DefaultSize, canonIndex: 1, testnum: 1, desc: "Maximum size for a single chunk"},
		{length: chunk.DefaultSize + (chunk.DefaultSize / 2), canonIndex: 3, testnum: 2, desc: "One root. One full child, one partial child"},
		{length: chunk.DefaultSize * 2, canonIndex: 3, testnum: 3, desc: "One root. Two full children."},
		{length: chunk.DefaultSize * 3, canonIndex: 4, testnum: 4, desc: "One root. Three full children."},
		{length: chunk.DefaultSize * 127, canonIndex: 128, testnum: 5, desc: "One root and 127 children."},
		{length: chunk.DefaultSize * 128, canonIndex: 129, testnum: 6, desc: "One root and 128 children."},
		{length: chunk.DefaultSize*128 + 4064, canonIndex: 131, testnum: 7, desc: "One root, one intermediary, 129 children."},
		{length: chunk.DefaultSize*128 + 4095, canonIndex: 131, testnum: 8, desc: "One root, one intermediary, 129 children."},
		{length: chunk.DefaultSize * 129, canonIndex: 132, testnum: 9, desc: "One root, two intermediary, 129 children."},
		{length: chunk.DefaultSize * 257, canonIndex: 261, testnum: 10, desc: "One root, two intermediary, 257 children."},
		{length: chunk.DefaultSize * 128 * 128, canonIndex: 16513, testnum: 11, desc: "One root, 128 intermediary, 16384 children."},
		{length: chunk.DefaultSize*128*128 + 1337, canonIndex: 16515, testnum: 12, desc: "One root, 1 IM1, 128 IM2, 16385 children."},
		{length: chunk.DefaultSize*128*128 + 4095, canonIndex: 16515, testnum: 13, desc: "One root, 1 IM1, 128 IM2, 16385 children."},
		{length: chunk.DefaultSize*128*128 + 4096, canonIndex: 16516, testnum: 14, desc: "One root, 2 IM1, 128 IM2, 16385 children."},
		{length: chunk.DefaultSize*128*128 + chunk.DefaultSize*127, canonIndex: 16642, testnum: 15, desc: "One root, 2 IM1, 128 IM2, 16511 children."},
		{length: chunk.DefaultSize*128*128 + chunk.DefaultSize*127 + 4095, canonIndex: 16643, testnum: 16, desc: "One root, 2 IM1, 128 IM2, 16512 children."},
		{length: chunk.DefaultSize*128*128 + chunk.DefaultSize*128, canonIndex: 16644, testnum: 17, desc: "One root, 2 IM1, 129 IM2, 16512 children."},
		{length: chunk.DefaultSize*128*128 + (chunk.DefaultSize * 129), canonIndex: 16646, testnum: 18, desc: "One root, 2 IM1, 130 IM2, 16513 children."},
		{length: chunk.DefaultSize * 128 * 128 * 2, canonIndex: 33027, testnum: 19, desc: "One root, 2 IM1, 256 IM2, 32768 children."},
		{length: (chunk.DefaultSize * 128 * 128) + (chunk.DefaultSize * 128 * 128) + (chunk.DefaultSize * 128 * 2) + chunk.DefaultSize, canonIndex: 33288, testnum: 20, desc: "One root, 3 IM1, 259 IM2, 33025 children."},
		{length: chunk.DefaultSize * 128 * 128 * 5, canonIndex: 82566, testnum: 21, desc: "One root, 5 IM1, 640 IM2, 81920 children."},
		{length: 1048576, canonIndex: 259, testnum: 22, desc: "1 MB File"},
		{length: chunk.DefaultSize*256 + 10, canonIndex: 260, testnum: 23, desc: "1 MB + 10 byte file"},
		{length: 104857600, canonIndex: 25803, testnum: 24, desc: "100 MB - 104 857 600"},
		{length: 1048576000, canonIndex: 258017, testnum: 25, desc: "1000 MB - 104 857 6000 - 256 000 chunks  1 root, 16 IM1, 2000 IM2, 256000 children."},
	}
	dir := t.TempDir()

	var nextIndex int = 1
	var maxIndex int
	var testNum int = 0

	var intervals []int = []int{1, 2, 4}

	var walker func(context.Context, storage.Getter, int, *TreeChunk, *TreeChunk)
	walker = func(ctx context.Context, getter storage.Getter, interval int,
		parent, root *TreeChunk) {
		for j := range parent.Children {
			numChildren := int(math.Ceil(float64(parent.SubtreeSize) / chunk.DefaultSize))
			nextInter := utils.Min(interval, numChildren)
			walker(ctx, getter, nextInter, parent.Children[j], root)
		}

		// Dont try to request children that are outside bounds.
		if nextIndex > maxIndex {
			return
		}

		// Only check for children.
		if len(parent.Children) == 0 {
			var indexes []int = make([]int, interval)
			var memChilds []*TreeChunk = make([]*TreeChunk, interval)
			for i := 0; i < interval; i++ {
				indexes[i] = nextIndex
				cld, err := root.GetChildFromMem(nextIndex)
				if cld != nil && err != nil {
					t.Fatalf("Error. %s", err.Error())
				}
				memChilds[i] = cld
				nextIndex++

				// Dont try to request children that are outside bounds.
				if nextIndex > maxIndex {
					nextIndex--
				}
			}

			netChilds, err := GetChildrenFromNet(ctx, getter, storage.Reference(root.Key), nil, indexes...)
			if err != nil {
				t.Fatalf("Error retrieving child. %s", err.Error())
			}

			assert.Equal(t, len(netChilds), len(memChilds), "Got %d, Expected %d. Length are not identical", len(netChilds), len(memChilds))

			for i := 0; i < len(netChilds); i++ {
				// Check we retrieved data
				assert.NotEqual(t, len(netChilds[i].Data), 0, "Data length was null. Key %v", netChilds[i].Key)
				// Check equal size
				assert.Equal(t, memChilds[i].SubtreeSize, netChilds[i].SubtreeSize,
					"Got %d, Expected %d. Incorrect subtreesize. Key %v. Test number %d. Data: %v, Length: %d",
					netChilds[i].SubtreeSize, memChilds[i].SubtreeSize, memChilds[i].Key[:16], testNum,
					netChilds[i].Data[:utils.Min(8, len(netChilds[i].Data))], len(netChilds[i].Data))
				// Check equal key
				assert.Equal(t, memChilds[i].Key, netChilds[i].Key,
					"Got %d, Expected %d. Incorrect key. Test number %d. Data: %v, Length: %d, Size: %d",
					netChilds[i].Key[:16], memChilds[i].Key[:16], testNum, netChilds[i].Data[:utils.Min(20,
						len(netChilds[i].Data))], len(netChilds[i].Data), netChilds[i].SubtreeSize)
			}
		}

	}
	for i, test := range tests {
		addr, reader, getter, err := utils.GenerateRandomData(test.length, storage.DefaultHash, dir)
		if err != nil {
			fmt.Printf("Got error on test %d. Error: %v", i, err)
		}

		testNum = test.testnum

		treeRoot, err := BuildCompleteTree(reader.Context(), getter, storage.Reference(addr),
			BuildTreeOptions{EmptyLeaves: true}, repair.NewMockRepair(getter))
		if err != nil {
			t.Fatal(err.Error())
		}

		numChildren := int(math.Ceil(float64(treeRoot.SubtreeSize) / chunk.DefaultSize))
		maxIndex = numChildren

		for i := 0; i < len(intervals); i++ {
			nextIndex = 1

			interval := utils.Min(intervals[i], numChildren)
			walker(reader.Context(), getter, interval, treeRoot, treeRoot)
		}

		assert.Equal(t, treeRoot.Index, test.canonIndex, "Got %d, Expected %d. Incorrect index for chunk with hash %v. Test number %d. Data: %v, Length: %d, Size: %d", treeRoot.Index, test.canonIndex, treeRoot.Key, testNum, treeRoot.Data, len(treeRoot.Data), treeRoot.SubtreeSize)
	}
}
