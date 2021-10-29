package swarmconnector

import (
	"testing"

	"github.com/ethersphere/swarm/chunk"
	"github.com/stretchr/testify/assert"
)

func TestGenerateChunkMetadataParentChildren(t *testing.T) {
	cd := uint64(chunk.DefaultSize)
	a := []int{}
	g := func(start, end int) []int {
		ret := make([]int, 0)
		for i := start; i <= end; i++ {
			ret = append(ret, i)
		}
		return ret
	}
	tests := []struct {
		maxIndex int
		size     uint64
		parents  map[int]int
		children map[int][]int
	}{
		{1, 100, map[int]int{1: 0}, map[int][]int{1: a}},
		{5, 4 * cd, map[int]int{1: 5, 2: 5, 3: 5, 4: 5}, map[int][]int{1: a, 2: a, 3: a, 4: a, 5: []int{1, 2, 3, 4}}},
		{160, 157 * cd, map[int]int{1: 129, 2: 129, 120: 129, 129: 160, 150: 159, 159: 160, 160: 0}, map[int][]int{
			1: a, 2: a, 120: a,
			129: g(1, 128), 150: a, 159: g(130, 158), 160: {159, 129},
		}},
		{259, 256 * cd, map[int]int{1: 129, 5: 129, 150: 258, 129: 259, 256: 258, 258: 259, 259: 0}, map[int][]int{
			1: a, 5: a, 150: a,
			129: g(1, 128), 256: a, 258: g(130, 257), 259: {129, 258},
		}},
	}

	for _, test := range tests {
		sizeList, err := GenerateChunkMetadata(test.size)
		assert.Nil(t, err, "Error not nil. %v", err)
		assert.Equal(t, test.maxIndex, len(sizeList), "Number of elements do not match.")
		for i := 0; i < len(sizeList); i++ {
			if par, ok := test.parents[i+1]; ok {
				assert.Equal(t, par, sizeList[i].Parent, "Wrong parent. MaxIndex: %v, i: %v", test.maxIndex, i)
			}
			if childs, ok := test.children[i+1]; ok {
				assert.ElementsMatchf(t, childs, sizeList[i].Children, "Wrong children. MaxIndex: %v, i: %v", test.maxIndex, i)
			}
		}
	}
}

func TestGenerateChunkMetadataSizeLength(t *testing.T) {
	cd := uint64(chunk.DefaultSize)
	im := uint64(128 * chunk.DefaultSize)

	type co struct {
		count     int
		chunksize uint64
	}
	chunks := func(chunkorder ...co) []uint64 {
		list := make([]uint64, 0)
		for i := 0; i < len(chunkorder); i++ {
			tmplist := make([]uint64, chunkorder[i].count)
			for j := 0; j < chunkorder[i].count; j++ {
				tmplist[j] = chunkorder[i].chunksize
			}
			list = append(list, tmplist...)
		}
		return list
	}

	tests := []struct {
		maxIndex int
		size     uint64
		testLeaf bool
		sizeList []uint64
	}{
		{1, 100, true, chunks(co{1, 100})},
		{1, 4093, true, chunks(co{1, 4093})},
		{3, chunk.DefaultSize + 1, true, []uint64{cd, 1, cd + 1}},
		{5, 4 * cd, true, chunks(co{4, cd}, co{1, 4 * cd})},
		{128, 127 * cd, true, chunks(co{127, cd}, co{1, 127 * cd})},
		{129, 128 * cd, true, chunks(co{128, cd}, co{1, 128 * cd})},
		{38, 150000, true, chunks(co{36, cd}, co{1, 2544}, co{1, 150000})},
		{132, 129 * cd, true, chunks(co{128, cd}, co{1, im}, co{2, cd}, co{1, 129 * cd})},
		{289, 285 * cd, true, chunks(co{128, cd}, co{1, im}, co{128, cd}, co{1, im}, co{29, cd}, co{1, 29 * cd}, co{1, 285 * cd})},
		{16513, 16384 * cd, false, chunks(co{128, im}, co{1, 16384 * cd})},
		{16515, 16384*cd + 1337, false, chunks(co{128, im}, co{1, 16384 * cd}, co{1, 1337}, co{1, 16384*cd + 1337})},
		{16515, 16384*cd + 4095, false, chunks(co{128, im}, co{1, 16384 * cd}, co{1, 4095}, co{1, 16384*cd + 4095})},
		{16516, 16385 * cd, false, chunks(co{128, im}, co{1, 16384 * cd}, co{1, 16385 * cd})},
		{16646, 16513 * cd, false, chunks(co{128, im}, co{1, 16384 * cd}, co{1, im}, co{1, 129 * cd}, co{1, 16513 * cd})},
		{25803, 25600 * cd, false, chunks(co{128, im}, co{1, 16384 * cd}, co{72, im}, co{1, 9216 * cd}, co{1, 25600 * cd})},
		{82566, cd * 128 * 128 * 5, false, chunks(co{128, im}, co{1, 16384 * cd}, co{128, im}, co{1, 16384 * cd}, co{128, im},
			co{1, 16384 * cd}, co{128, im}, co{1, 16384 * cd}, co{128, im}, co{1, 16384 * cd}, co{1, cd * 128 * 128 * 5})},
	}

	for _, test := range tests {
		sizeList, err := GenerateChunkMetadata(test.size)
		assert.Nil(t, err, "Error not nil. %v", err)
		assert.Equal(t, test.maxIndex, len(sizeList), "Number of elements do not match.")
		if test.testLeaf {
			newList := make([]uint64, 0)
			for i := 0; i < len(sizeList); i++ {
				newList = append(newList, sizeList[i].Size)
			}
			assert.Equal(t, test.sizeList, newList, "Elements do not match. Size: %d, MaxIndex: %d", test.size, test.maxIndex)
		} else {
			newList := make([]uint64, 0)
			for i := 0; i < len(sizeList); i++ {
				if sizeList[i].Size != cd {
					newList = append(newList, sizeList[i].Size)
				}
			}
			assert.Equal(t, test.sizeList, newList, "Elements do not match. Size: %d, MaxIndex: %d", test.size, test.maxIndex)
		}
	}
}

func TestGetDepthCanonicalIndex(t *testing.T) {
	tests := []struct {
		maxIndex int
		depth    int
	}{
		{1, 1},
		{118, 2},
		{267, 3},
		{6999, 3},
		{16513, 3},
		{16514, 4},
		{947215, 4},
		{8746985110, 6},
	}

	for _, test := range tests {
		depth := GetDepthCanonicalIndex(test.maxIndex)
		assert.Equal(t, test.depth, depth, "Depth do not match.")
	}
}

func TestGetLeavesCanonIndex(t *testing.T) {
	tests := []struct {
		maxIndex int
		leaves   int
	}{
		{1, 1},
		{118, 117},
		{267, 263},
		{129, 128},
		{131, 129},
		{132, 129},
		{133, 130},
		{16513, 16384},
		{16515, 16385},
		{16516, 16385},
		{16517, 16386},
		{16642, 16511},
		{16643, 16512},
		{16644, 16512},
		{16646, 16513},
		{16647, 16514},
		{16648, 16515},
		{16649, 16516},
		{16773, 16640},
		{33027, 32768},
		{33288, 33025},
		{82566, 81920},
		{2113664, 2097151},
		{2113665, 2097152},
		{2113667, 2097153},
		{2113668, 2097153},
	}

	for _, test := range tests {
		leaves := GetLeavesCanonIndex(test.maxIndex)
		assert.Equal(t, test.leaves, leaves, "Canon index %d, returns %d leaves. Should be %d.",
			test.maxIndex, leaves, test.leaves)
	}
}
