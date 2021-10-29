package entangler

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/relab/snarl-mw21/swarmconnector"
)

type timePeriod struct {
	StartTime int64
	EndTime   int64
}

type RepairPair struct {
	Left  *Block
	Right *Block
}

type Block struct {
	EntangledBlock
	Left           []*Block
	Right          []*Block
	Position       int
	IsParity       bool
	Identifier     []byte
	DownloadStatus DownloadStatus
	RepairStatus   RepairStatus
	IsUnavailable  bool
	IsMending      bool
	DownloadTime   timePeriod
	RepairTime     timePeriod
	lock           sync.Mutex
	//lock         deadlock.Mutex
	Parent       *Block   // The dependant parent in the Merkle Tree
	Children     []*Block // Those that depend on this block in the Merkle Tree
	ChangeStatus []chan BlockStatus
	RepairPairs  []*RepairPair
}

func (b *Block) SetMending(set bool) {
	b.IsMending = set
}

func (b *Block) InternalNodePendingRepair() bool {
	return b.DownloadStatus == DownloadFailed && b.RepairStatus == NoRepair && !b.IsMending && len(b.Children) > 0 && !b.IsParity
}

func (b *Block) ParityShouldRepair() bool {
	b.lock.Lock()
	defer b.lock.Unlock()
	return !(b.HasData(true) || b.RepairStatus == RepairFailed || b.RepairStatus == RepairPending)
}

// GetRepairPairs returns a slice of all possible pairs of blocks that can be used to repair the current block.
// If the block is a Parity, the first element in the slice will always be in the left direction.
// If the block is a Data, the order will be: Horizontal, Right, Left.
func (b *Block) GetRepairPairs() []*RepairPair {
	if b.RepairPairs != nil {
		return b.RepairPairs
	}
	var repPair []*RepairPair
	if b.IsParity {
		l := b.Left[0].Left[b.Class]
		if l.Replace {
			l = l.Left[0]
		}
		if !b.Replace {
			repPair = make([]*RepairPair, 2)
			repPair[1] = &RepairPair{Left: b.Right[0], Right: b.Right[0].Right[b.Class]}
		} else {
			repPair = make([]*RepairPair, 1)
		}
		repPair[0] = &RepairPair{Left: l, Right: b.Left[0]}
	} else {
		repPair = make([]*RepairPair, len(b.Left))
		for i := 0; i < len(b.Left); i++ {
			l := b.Left[i]
			r := b.Right[i]
			if l.Replace {
				l = l.Left[0]
			}
			repPair[i] = &RepairPair{Left: l, Right: r}
			if r.Replace {
				repPair = append(repPair, &RepairPair{Left: r.Right[0], Right: r.Right[0].Right[i]})
			}
		}

	}
	b.RepairPairs = repPair
	return repPair
}

func (b *Block) Repair(v, w *Block, nolock ...bool) error {
	if !v.HasData() || !w.HasData() {
		return errors.New("Missing data")
	} else if !b.RepairSuccess(XORByteSlice(v.Data, w.Data), nolock...) {
		return errors.New("Block already have data")
	}
	return nil
}

func (b *Block) DownloadPending(nolock ...bool) bool {
	return b.SetData(nil, time.Now().UnixNano(), 0, DownloadPending, NoRepair, nolock...)
}

func (b *Block) DownloadFailed(nolock ...bool) bool {
	return b.SetData(nil, 0, time.Now().UnixNano(), DownloadFailed, NoRepair, nolock...)
}

func (b *Block) DownloadSuccess(data []byte, nolock ...bool) bool {
	return b.SetData(data, 0, time.Now().UnixNano(), DownloadSuccess, NoRepair, nolock...)
}
func (b *Block) RepairPending(nolock ...bool) bool {
	if nolock == nil {
		b.lock.Lock()
		defer b.lock.Unlock()
	}
	if b.RepairStatus == RepairPending {
		return false
	}
	return b.SetData(nil, time.Now().UnixNano(), 0, NoDownload, RepairPending, true)
}

func (b *Block) RepairFailed(nolock ...bool) bool {
	return b.SetData(nil, 0, time.Now().UnixNano(), NoDownload, RepairFailed, nolock...)
}

func (b *Block) RepairSuccess(data []byte, nolock ...bool) bool {
	return b.SetData(data, 0, time.Now().UnixNano(), NoDownload, RepairSuccess, nolock...)
}

// SetData sets metadata for the lattice block. Recommended to be called from helper functions Download* and Repair*. Returns true if changed
func (b *Block) SetData(data []byte, start, end int64, retrieveStatus DownloadStatus, repairStatus RepairStatus, nolock ...bool) bool {
	if nolock == nil {
		b.lock.Lock()
		defer b.lock.Unlock()
	}
	if b.HasData(true) { // If it has data, we shouldn't need to do any changes.
		return false
	}

	if data != nil {
		b.Data = make([]byte, len(data))
		copy(b.Data, data)

		if b.Length != 0 {
			binary.LittleEndian.PutUint64(b.Data, b.Size)
			b.Data = b.Data[:b.Length]
		}
	}
	if retrieveStatus != NoDownload {
		b.DownloadStatus = retrieveStatus
		if start != 0 {
			b.DownloadTime.StartTime = start
		}
		if end != 0 {
			b.DownloadTime.EndTime = end
		}
	}
	if repairStatus != NoRepair {
		b.RepairStatus = repairStatus
		if start != 0 {
			b.RepairTime.StartTime = start
		}
		if end != 0 {
			b.RepairTime.EndTime = end
		}
	}

	// Print log entry
	LogPrint("%t,%d,%d,%d,%t,%d,%d,%v,%d,%d,%v\n", b.IsParity, b.Position,
		b.LeftPos(0), b.RightPos(0), b.HasData(true), b.DownloadTime.StartTime,
		b.DownloadTime.EndTime, b.DownloadStatus, b.RepairTime.StartTime,
		b.RepairTime.EndTime, b.RepairStatus)

	// Signal any listeners
	for _, notifyChan := range b.ChangeStatus {
		notifyChan <- Set(b.DownloadStatus, b.RepairStatus)
	}
	b.ChangeStatus = nil

	return true
}

type BlockStatus uint8

func Set(ds DownloadStatus, rs RepairStatus) (bs BlockStatus) {
	return BlockStatus(rs)<<2 + BlockStatus(ds)
}

func (bs BlockStatus) HasData() bool {
	ds := DownloadStatus(bs & 0b0011)
	rs := RepairStatus(bs & 0b1100 >> 2)
	return ds == DownloadSuccess || rs == RepairSuccess
}

//go:generate stringer -type=DownloadStatus
type DownloadStatus int

const (
	NoDownload      DownloadStatus = iota // Did not attempt to download yet
	DownloadPending                       // Download pending
	DownloadSuccess                       // Download finished and HasData() = true
	DownloadFailed                        // Download finished and HasData() = false
)

//go:generate stringer -type=RepairStatus
type RepairStatus int

const (
	NoRepair      RepairStatus = iota // Did not attempt to repair
	RepairPending                     // We started the repair process for this block.
	RepairSuccess                     // HasData() = true [Download initially failed or was never attempted]
	RepairFailed                      // HasData() = false [Download initially failed or was never attempted]
)

//go:generate stringer -type=StrandClass
type StrandClass int

const (
	Horizontal StrandClass = iota
	Right
	Left
)

func (b *Block) LeftPos(class int) int {
	if b == nil {
		return -1 // Fatal error.
	}
	if b.IsParity && b.LeftIndex > 0 {
		return b.LeftIndex
	} else if len(b.Left) > class && b.Left[class] != nil {
		return b.Left[class].Position
	}
	return 0
}

func (b *Block) RightPos(class int) int {
	if b == nil {
		return -1 // Fatal error.
	}
	if b.IsParity && b.RightIndex > 0 {
		return b.RightIndex
	} else if len(b.Right) > class && b.Right[class] != nil {
		return b.Right[class].Position
	}
	return 0
}

func (b *Block) String() string {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.LockfreeString()
}

func (b *Block) LockfreeString() string {
	if b.IsParity {
		return fmt.Sprintf("Parity %v_%v, %v, %v, %v, D: %t, R: %v",
			b.LeftPos(0), b.RightPos(0), b.Class, b.DownloadStatus, b.RepairStatus, len(b.Data) > swarmconnector.ChunkSizeOffset, b.Replace)
	}
	return fmt.Sprintf("Data %d, %v, %v, D: %t",
		b.Position, b.DownloadStatus, b.RepairStatus, len(b.Data) > swarmconnector.ChunkSizeOffset)
}

func (b *Block) HasData(nolock ...bool) bool {
	if b == nil {
		return false
	}
	if nolock == nil {
		b.lock.Lock()
		defer b.lock.Unlock()
	}
	return len(b.Data) > swarmconnector.ChunkSizeOffset
}

func (b *Block) ShouldWaitDL(nolock ...bool) bool {
	if nolock == nil {
		b.lock.Lock()
	}
	if b.DownloadStatus == DownloadPending {
		if nolock == nil {
			b.lock.Unlock()
		}
		return true
	}
	if b.DownloadStatus == NoDownload && b.Parent != nil {
		if nolock == nil {
			b.lock.Unlock()
		}
		return b.Parent.ShouldWaitDL()
	}
	if nolock == nil {
		b.lock.Unlock()
	}
	return false
}

func (b *Block) SubToStatusChange(nolock ...bool) <-chan BlockStatus {
	if nolock == nil {
		b.lock.Lock()
		defer b.lock.Unlock()
	}
	c := make(chan BlockStatus)
	if b.ChangeStatus == nil {
		b.ChangeStatus = make([]chan BlockStatus, 0)
	}
	b.ChangeStatus = append(b.ChangeStatus, c)
	return c
}
