package entangler

import (
	"context"
	"log"
	"sync"

	"github.com/ethersphere/swarm/storage"
	"github.com/relab/snarl-mw21/swarmconnector"
)

// S - Horizontal strands. P - Helical strands
type Lattice struct {
	Entangler
	Blocks            []*Block
	DataBlocks        []*Block
	Getter            storage.Getter
	MissingDataBlocks int
	ctx               context.Context
	DataRootID        []byte
	ParityRootID      [][]byte
	didInit           bool
	maxDatablockSize  int
	Size              uint64
	lock              sync.Mutex
	dlLock            sync.Mutex
	DownloadEvent     []chan int
	pendingDLs        int
	RecoverError      error
	internalNodeShift map[int]int // Shifts from TreeChunk Index to Lattice Position
}

func NewLattice(ctx context.Context, alpha, s, p int, numDataBlocks int) *Lattice {
	blocks := make([]*Block, 0, numDataBlocks*alpha)
	return &Lattice{
		Entangler: Entangler{
			Alpha:         alpha,
			S:             s,
			P:             p,
			NumDataBlocks: numDataBlocks,
		},
		MissingDataBlocks: numDataBlocks,
		Blocks:            blocks,
		ctx:               ctx,
		Size:              uint64(numDataBlocks),
		maxDatablockSize:  1 << 20,
	}
}

func NewSwarmLattice(ctx context.Context, alpha, s, p int, size uint64, getter storage.Getter,
	datarootid []byte, parityrootids [][]byte, maxDataSize int) *Lattice {
	l := &Lattice{
		Entangler: Entangler{
			Alpha: alpha,
			S:     s,
			P:     p,
		},
		Getter:           getter,
		ctx:              ctx,
		DataRootID:       datarootid,
		ParityRootID:     parityrootids,
		maxDatablockSize: maxDataSize,
		Size:             size,
	}

	// We initialize the lattice
	l.RunInit()

	return l
}

// GetBlock retrieves the correct block in the lattice given the blocks canonical index
// in the Merkle tree.
func (l *Lattice) GetBlock(canonIndex int) *Block {
	var blockPos int
	if shift, ok := l.internalNodeShift[canonIndex]; ok {
		blockPos = shift
	} else {
		blockPos = canonIndex
	}
	return l.Blocks[blockPos-1]
}

// GetTranslatedBlock returns the block in the Lattice that we translated into / from.
func (l *Lattice) GetTranslatedBlock(block *Block) *Block {
	if block == nil {
		return nil
	} else if block.IsParity {
		return block
	}
	return l.GetBlock(block.Position)
}

func (l *Lattice) pendingDLchange(diff int) {
	l.dlLock.Lock()
	defer l.dlLock.Unlock()
	l.pendingDLs += diff

	// For now we just care if there are nothing pending.
	if l.pendingDLs == 0 {
		for i := 0; i < len(l.DownloadEvent); i++ {
			l.DownloadEvent[i] <- l.pendingDLs
			close(l.DownloadEvent[i])
		}
		l.DownloadEvent = nil
	}
}

func (l *Lattice) WaitForNoPendingDL() (bool, <-chan int) {
	l.dlLock.Lock()
	defer l.dlLock.Unlock()
	if l.pendingDLs == 0 {
		return false, nil
	}
	c := make(chan int)
	if l.DownloadEvent == nil {
		l.DownloadEvent = []chan int{c}
	} else {
		l.DownloadEvent = append(l.DownloadEvent, c)
	}

	return true, c
}

func (l *Lattice) GetNeighbours(block *Block, direction bool) []*Block {
	neighbours := make([]*Block, 1, 2*l.Alpha)
	neighbours[0] = block
	added := make(map[int]struct{})
	added[block.Position] = struct{}{}
	var iter func([]*Block, bool)
	iter = func(src []*Block, dir bool) {
		var n *Block
		for i := 0; i < len(src); i++ {
			if dir {
				n = src[i].Right[0]
			} else {
				n = src[i].Left[0]
			}

			if _, ok := added[n.Position]; !ok {
				neighbours = append(neighbours, n)
				added[n.Position] = struct{}{}
				if dir {
					defer iter(n.Left, false)
				} else {
					defer iter(n.Right, true)
				}
			}
		}
	}

	if direction { // To the right
		iter(block.Right, true)
	} else { // To the left
		iter(block.Left, false)
	}

	return neighbours
}

func (l *Lattice) ResetRepairStatus(nolock ...bool) {
	if nolock == nil {
		l.lock.Lock()
		defer l.lock.Unlock()
	}
	for i := 0; i < len(l.Blocks); i++ {
		b := l.Blocks[i]
		b.lock.Lock()
		if b.RepairStatus == RepairFailed || b.RepairStatus == RepairPending {
			b.RepairStatus = NoRepair
		}
		b.lock.Unlock()
	}
}

func (l *Lattice) ResetMendingStatus(nolock ...bool) {
	if nolock == nil {
		l.lock.Lock()
		defer l.lock.Unlock()
	}
	for i := 0; i < l.NumDataBlocks; i++ {
		b := l.Blocks[i]
		if b.IsMending {
			b.IsMending = false
		}
	}
}

// RunInit creates the entire Lattice structure in memory for the given size and configuration.
func (l *Lattice) RunInit() {
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.didInit {
		return
	}

	sizeList, err := swarmconnector.GenerateChunkMetadata(l.Size)
	if err != nil {
		log.Fatal(err)
	}
	if l.NumDataBlocks == 0 {
		l.NumDataBlocks = len(sizeList)
	}

	l.MissingDataBlocks = l.NumDataBlocks
	l.Blocks = make([]*Block, 0, l.NumDataBlocks*l.Alpha)

	// Create datablocks
	for i := 0; i < l.NumDataBlocks; i++ {
		b := &Block{
			EntangledBlock: EntangledBlock{},
			Position:       i + 1, IsParity: false,
			Left:  make([]*Block, l.Alpha),
			Right: make([]*Block, l.Alpha),
		}
		l.Blocks = append(l.Blocks, b)
	}

	l.createInternalNodeShift(sizeList)

	// Create parities
	replacedIndices := l.GetReplacedParityIndices()

	// Setup temporary storage for connecting blocks
	next := make([]int, l.Alpha)
	wrap := make([]int, l.Alpha)
	var newWrap bool

	for i := 0; i < l.NumDataBlocks; i++ {
		var position = i + 1
		newWrap = false
		next[1], next[0], next[2] = GetForwardNeighbours(position, l.S, l.P)

		for k := 0; k < l.Alpha; k++ {
			b := &Block{
				EntangledBlock: EntangledBlock{
					Class: StrandClass(k),
				},
				Position: position, IsParity: true,
			}
			if _, ok := replacedIndices[position]; ok {
				b.Replace = true
			}

			// Connect to left data block
			leftData := l.Blocks[i]
			b.Left = []*Block{leftData}
			b.LeftIndex = leftData.Position
			leftData.Right[k] = b

			// Connect to right data block
			nxt := next[k]
			if nxt > l.NumDataBlocks {
				if !newWrap {
					wrap[1], wrap[0], wrap[2] = GetWrapPosition(position, l.S, l.P)
					newWrap = true
				}
				nxt = wrap[k]
			}

			if nxt--; nxt >= len(l.Blocks) || nxt < 0 {
				log.Fatal("Error in lattice construction.")
			}

			rightData := l.Blocks[nxt]
			b.Right = []*Block{rightData}
			b.RightIndex = rightData.Position
			rightData.Left[k] = b

			l.Blocks = append(l.Blocks, b)
		}
	}
	l.didInit = true
}

func (l *Lattice) createInternalNodeShift(sizeList []swarmconnector.ChunkMetadata) {
	// Add links to parents and children.
	l.internalNodeShift = make(map[int]int)
	internalNodes := make(map[int]swarmconnector.ChunkMetadata)
	internalNodesOrder := make([]int, 0)
	for i := 0; i < len(sizeList); i++ {
		b, s := l.Blocks[i], sizeList[i]
		lsc := len(s.Children)
		if s.Parent != 0 {
			b.Parent = l.Blocks[s.Parent-1]
		}
		if lsc > 0 {
			b.Children = make([]*Block, lsc)
			for j := 0; j < lsc; j++ {
				b.Children[j] = l.Blocks[s.Children[j]-1]
			}
			if s.Parent != 0 { // We do not shift the root
				internalNodes[i] = s
				internalNodesOrder = append(internalNodesOrder, i)
			}
		}
		b.Size = s.Size
		b.Length = s.Length
	}

	windowSize := l.S * l.P

	for i := 0; i < len(internalNodesOrder); i++ {
		canInd := internalNodesOrder[i]
		im := internalNodes[canInd]
		lowestChild := im.Children[0]
		highestChild := im.Children[len(im.Children)-1]
		for j := windowSize; j < l.NumDataBlocks; j += windowSize + l.S {
			inWindow := j+1 > lowestChild-windowSize && j+1 < highestChild+windowSize
			if !inWindow && len(sizeList[j].Children) == 0 {
				// Check if we already added this shift.
				if _, ok := l.internalNodeShift[canInd+1]; ok {
					continue
				}
				if _, ok := l.internalNodeShift[j+1]; ok {
					continue
				}
				l.internalNodeShift[canInd+1] = j + 1
				l.internalNodeShift[j+1] = canInd + 1
				l.translateBlocks(canInd, j)
				break
			}
		}
	}
}

func (l *Lattice) translateBlocks(a, b int) {
	l.Blocks[a].Size, l.Blocks[b].Size = l.Blocks[b].Size, l.Blocks[a].Size
	l.Blocks[a].Length, l.Blocks[b].Length = l.Blocks[b].Length, l.Blocks[a].Length
	l.Blocks[a].Parent, l.Blocks[b].Parent = l.Blocks[b].Parent, l.Blocks[a].Parent
	l.Blocks[a].Children, l.Blocks[b].Children = l.Blocks[b].Children, l.Blocks[a].Children
}
