package entangler

import (
	"math"
	"sort"

	"github.com/relab/snarl-mw21/utils"
)

type Entangler struct {
	RightStrands      int
	P                 int // Right
	LeftStrands       int
	HorizontalStrands int
	S                 int // Horizontal
	Alpha             int
	ParityMemory      [][]byte
	LeftExtremeMemory [][]byte
	NumDataBlocks     int
	RightExtremeIndex []int
}

// (Usually) represents a parity
type EntangledBlock struct {
	Size       uint64
	Length     int
	Data       []byte
	LeftIndex  int
	RightIndex int
	Class      StrandClass
	Replace    bool // Replaces previous parity (Closed lattice)
}

func NewEntangler(rStrands, lStrands, hStrands, alpha, chunkSize int) *Entangler {
	totStrands := rStrands + lStrands + hStrands
	e := &Entangler{
		RightStrands: rStrands, P: rStrands, LeftStrands: lStrands,
		HorizontalStrands: hStrands, S: hStrands, Alpha: alpha,
		ParityMemory: make([][]byte, totStrands),
	}

	maxStrand := utils.Max(rStrands, hStrands)
	e.LeftExtremeMemory = make([][]byte, maxStrand)
	for i := 0; i < totStrands; i++ {
		e.ParityMemory[i] = make([]byte, chunkSize)
		if i < maxStrand {
			e.LeftExtremeMemory[i] = make([]byte, chunkSize)
		}
	}

	return e
}

func (e *Entangler) FeedLeftExtreme(datachunks ...[]byte) {
	for i := 0; i < len(datachunks); i++ {
		if datachunks[i] != nil {
			e.LeftExtremeMemory[i] = datachunks[i]
		}
	}
}

func (e *Entangler) Entangle(datachunk []byte, index int, result chan<- *EntangledBlock) {
	if index > e.NumDataBlocks {
		e.NumDataBlocks = index
	}

	if index <= len(e.LeftExtremeMemory) {
		e.LeftExtremeMemory[index-1] = datachunk
	}

	r, h, l := GetMemoryPosition(index, e.S, e.P)
	rBack, hBack, lBack := GetBackwardNeighbours(index, e.S, e.P)
	rParity := e.ParityMemory[r]
	hParity := e.ParityMemory[h]
	lParity := e.ParityMemory[l]

	result <- &EntangledBlock{
		Data: rParity, LeftIndex: rBack,
		RightIndex: index, Class: Right,
	}
	result <- &EntangledBlock{
		Data: hParity, LeftIndex: hBack,
		RightIndex: index, Class: Horizontal,
	}
	result <- &EntangledBlock{
		Data: lParity, LeftIndex: lBack,
		RightIndex: index, Class: Left,
	}

	rNext := XORByteSlice(datachunk, rParity)
	e.ParityMemory[r] = rNext
	hNext := XORByteSlice(datachunk, hParity)
	e.ParityMemory[h] = hNext
	lNext := XORByteSlice(datachunk, lParity)
	e.ParityMemory[l] = lNext
}

// WrapLattice wraps the lattice. Creating an edge/parity between the start and end of the lattice.
// Sends out new values for the new parities calculated.
// Also re-calculated the parity between the first and second data blocks.
func (e *Entangler) WrapLattice(result chan<- *EntangledBlock) {
	// Create the list of blocks that should be wrapped.
	if len(e.RightExtremeIndex) == 0 {
		e.setDatablocksToClose()
	}

	for i := 0; i < len(e.RightExtremeIndex); i++ {
		index := e.RightExtremeIndex[i]

		// We will use the already calculated parity to bind it to the start of the lattice.
		r, h, l := GetMemoryPosition(index, e.S, e.P)
		rFront, hFront, lFront := GetForwardNeighbours(index, e.S, e.P)
		rFirst, hFirst, lFirst := GetWrapPosition(index, e.S, e.P)

		if rFront > e.NumDataBlocks {
			// Link the last created parity to the first blocks of the lattice.
			result <- &EntangledBlock{
				Data: e.ParityMemory[r], LeftIndex: index,
				RightIndex: rFirst, Class: Right,
			}

			// Recalculate the parity between the first and second data blocks.
			rSecond, _, _ := GetForwardNeighbours(rFirst, e.S, e.P)
			rNext := XORByteSlice(e.LeftExtremeMemory[rFirst-1], e.ParityMemory[r])
			result <- &EntangledBlock{
				Data: rNext, LeftIndex: rFirst,
				RightIndex: rSecond, Class: Right, Replace: true,
			}
		}
		if hFront > e.NumDataBlocks {
			// Link the last created parity to the first blocks of the lattice.
			result <- &EntangledBlock{
				Data: e.ParityMemory[h], LeftIndex: index,
				RightIndex: hFirst, Class: Horizontal,
			}

			// Recalculate the parity between the first and second data blocks.
			_, hSecond, _ := GetForwardNeighbours(hFirst, e.S, e.P)
			hNext := XORByteSlice(e.LeftExtremeMemory[hFirst-1], e.ParityMemory[h])
			result <- &EntangledBlock{
				Data: hNext, LeftIndex: hFirst,
				RightIndex: hSecond, Class: Horizontal, Replace: true,
			}
		}
		if lFront > e.NumDataBlocks {
			// Link the last created parity to the first blocks of the lattice.
			result <- &EntangledBlock{
				Data: e.ParityMemory[l], LeftIndex: index,
				RightIndex: lFirst, Class: Left,
			}

			// Recalculate the parity between the first and second data blocks.
			_, _, lSecond := GetForwardNeighbours(lFirst, e.S, e.P)
			lNext := XORByteSlice(e.LeftExtremeMemory[lFirst-1], e.ParityMemory[l])
			result <- &EntangledBlock{
				Data: lNext, LeftIndex: lFirst,
				RightIndex: lSecond, Class: Left, Replace: true,
			}
		}
	}
}

func (e *Entangler) GetReplacedParityIndices() map[int]struct{} {
	replacedIndices := make(map[int]struct{})

	// Create the list of blocks that should be wrapped.
	if len(e.RightExtremeIndex) == 0 {
		e.setDatablocksToClose()
	}

	for i := 0; i < len(e.RightExtremeIndex); i++ {
		index := e.RightExtremeIndex[i]

		// We calculate to see if the given index is closed.
		rFront, hFront, lFront := GetForwardNeighbours(index, e.S, e.P)
		rFirst, hFirst, lFirst := GetWrapPosition(index, e.S, e.P)

		if rFront > e.NumDataBlocks {
			replacedIndices[rFirst] = struct{}{}
		}
		if hFront > e.NumDataBlocks {
			replacedIndices[hFirst] = struct{}{}
		}
		if lFront > e.NumDataBlocks {
			replacedIndices[lFirst] = struct{}{}
		}
	}

	return replacedIndices
}

func (e *Entangler) setDatablocksToClose() {
	e.RightExtremeIndex = make([]int, 0)
	lastIndex := e.NumDataBlocks
	var r, h, l, wraps int
	maxWraps := utils.Max(e.S, e.P) * e.Alpha
	for wraps < maxWraps && lastIndex > 0 {
		r, h, l = GetForwardNeighbours(lastIndex, e.S, e.P)
		didWrap := false
		if r > e.NumDataBlocks {
			wraps++
			didWrap = true
		}
		if h > e.NumDataBlocks {
			wraps++
			didWrap = true
		}
		if l > e.NumDataBlocks {
			wraps++
			didWrap = true
		}
		if wraps > 0 && didWrap {
			e.RightExtremeIndex = append(e.RightExtremeIndex, lastIndex)
		}
		lastIndex--
	}

	sort.Ints(e.RightExtremeIndex)
}

// XORByteSliceFast does as XORByteSlice, but is faster because it does not reserve temporary space in memory.
func XORByteSliceFast(a, b, out []byte) {
	if len(a) != len(b) {
		PadByteSlices(&a, &b, true) // Pad with zeros
	}
	if len(a) > len(out) {
		return
	}

	for i := 0; i < len(a); i++ {
		out[i] = a[i] ^ b[i]
	}
}

// XORByteSlice does the XOR function on each byte of the slice.
func XORByteSlice(a []byte, b []byte) []byte {
	if len(a) != len(b) {
		PadByteSlices(&a, &b, true) // Pad with zeros
	}
	buf := make([]byte, len(a))

	for i := range a {
		buf[i] = a[i] ^ b[i]
	}

	return buf
}

// PadByteSlices pads the shortest slice to make them equal length
// If [zeros] is true, the padding will be done with 0's, else it will be the content of the longest slice.
func PadByteSlices(a, b *[]byte, zeros bool) {
	lenA, lenB := len(*a), len(*b)
	if lenA == lenB {
		return
	} else if lenA < lenB {
		c := make([]byte, lenB)
		copy(c, *a)
		if !zeros {
			copy(c[lenA:], (*b)[lenA:])
		}
		*a = c
	} else {
		c := make([]byte, lenA)
		copy(c, *b)
		if !zeros {
			copy(c[lenB:], (*a)[lenB:])
		}
		*b = c
	}
}

// GetForwardNeighbours finds the index of the data block that is connected forwards
// Check is it top, center or bottom in the lattice
// 1 -> Top, 0 -> Bottom, else Center
func GetForwardNeighbours(index, S, P int) (r, h, l int) {
	nodePos := index % S

	if nodePos == 1 || nodePos == -4 {
		r = index + S + 1
		h = index + S
		l = index + (S * P) - int(math.Pow(float64(S-1), 2))
	} else if nodePos == 0 {
		r = index + (S * P) - int(math.Pow(float64(S), 2)-1)
		h = index + S
		l = index + S - 1
	} else {
		r = index + S + 1
		h = index + S
		l = index + (S - 1)
	}
	return
}

// GetBackwardNeighbours finds the index of the data block that is connected backwards
// TODO: Fix underflow naming errors on the nodes on the extreme of the lattice.
// Check is it top, center or bottom in the lattice
// 1 -> Top, 0 -> Bottom, else Center
func GetBackwardNeighbours(index, S, P int) (r, h, l int) {
	nodePos := index % S

	if nodePos == 1 || nodePos == -4 {
		r = index - (S * P) + int((math.Pow(float64(S), 2) - 1))
		h = index - S
		l = index - (S - 1)
	} else if nodePos == 0 {
		r = index - (S + 1)
		h = index - S
		l = index - (S * P) + int(math.Pow(float64(S-1), 2))
	} else {
		r = index - (S + 1)
		h = index - S
		l = index - (S - 1)
	}
	return
}

// GetMemoryPosition gets the position in the ParityMemory array where the parity is located
// For now this will recursively call the GetBackwardNeighbours function
func GetMemoryPosition(index, S, P int) (r, h, l int) {
	h = ((index - 1) % S) + S
	indx := index % (S * P)
	// r, l = indx, indx

	// for ; r > S; r, _, _ = GetBackwardNeighbours(r, S, P) {
	// }

	switch indx {
	case 0, 1, 7, 13, 19:
		r = 0
	case 2, 8, 14, 20, 21:
		r = 4
	case 3, 9, 15, 16, 22:
		r = 3
	case 4, 10, 11, 17, 23:
		r = 2
	case 5, 6, 12, 18, 24:
		r = 1
	}

	// for ; l > S; _, _, l = GetBackwardNeighbours(l, S, P) {
	// }

	switch indx {
	case 1, 10, 14, 18, 22:
		l = 11
	case 2, 6, 15, 19, 23:
		l = 12
	case 3, 7, 11, 20, 24:
		l = 13
	case 0, 4, 8, 12, 16:
		l = 14
	case 5, 9, 13, 17, 21:
		l = 10
	}

	return
}

// GetWrapPosition takes input the right-extreme of the lattice and returns the
// index of the datablock it would wrap around to when entangling.
func GetWrapPosition(index, S, P int) (r, h, l int) {
	h = ((index) % S)
	if h == 0 {
		h = S
	}
	indx := index % (S * P)
	if indx == 0 {
		indx = 25
	}
	r, l = indx, indx

	for ; r > S; r, _, _ = GetBackwardNeighbours(r, S, P) {
	}
	for ; l > S; _, _, l = GetBackwardNeighbours(l, S, P) {
	}

	return
}

// GetWrapPositionMaxLen returns the wrap position for the given index if it is
// truly at tne right-hand extreme of the lattice.
func GetWrapPositionMaxLen(index, S, P, maxIndex int) (r, h, l int) {
	rWrap, hWrap, lWrap := GetWrapPosition(index, S, P)
	rNext, hNext, lNext := GetForwardNeighbours(index, S, P)

	if rNext > maxIndex {
		r = rWrap
	} else {
		r = rNext
	}

	if hNext > maxIndex {
		h = hWrap
	} else {
		h = hNext
	}

	if lNext > maxIndex {
		l = lWrap
	} else {
		l = lNext
	}

	return
}
