package entangler

import (
	"context"
	"errors"
	"fmt"

	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/storage"
	"github.com/relab/snarl-mw21/swarmconnector"
	"github.com/relab/snarl-mw21/utils"
)

func DebugPrint(format string, a ...interface{}) (int, error) {
	return utils.DebugPrint(format, a...)
}

func LogPrint(format string, a ...interface{}) (int, error) {
	return utils.LogPrint(format, a...)
}

// RepairChunk is called by BuildCompleteTree whenever a data block is faulty.
func (l *Lattice) RepairChunk(index int) ([]byte, error) {
	if len(l.ParityRootID) < l.Alpha {
		return nil, fmt.Errorf("Missing parity root ID. Can not repair. Supply the same ID multiple time if this is intended.")
	}

	// Find block
	b := l.GetBlock(index)

	raceChan := make(chan bool)
	go func() {
		for {
			b.lock.Lock()
			if b.HasData(true) {
				b.lock.Unlock()
				raceChan <- false
				close(raceChan)
				return
			}
			bChan := b.SubToStatusChange(true)
			b.lock.Unlock()
			<-bChan
		}
	}()

	go func() {
		l.lock.Lock()
		if b.HasData() {
			l.lock.Unlock()
			return
		}

		raceChan <- true
	}()

	if <-raceChan { // Second go-func did l.lock.Lock().
		defer l.lock.Unlock()
	} else { // First go-func detected that b already has data.
		return b.Data, nil
	}

	if l.RecoverError != nil {
		return nil, l.RecoverError
	}

	data, err := l.repairBlock(b)
	if err != nil {
		b.RepairFailed()
		l.RecoverError = err
	}

	// Clear mending status before returning
	l.ResetMendingStatus(true)

	return data, err
}

func (l *Lattice) repairBlock(b *Block) ([]byte, error) {
	if b.IsMending {
		return nil, fmt.Errorf("Block is already mending.")
	}
	b.SetMending(true)

	if l.repairDataDLAdjacent(b) {
		return b.Data, nil
	}
	oldHasDataCnt := -1
	for {
		data, err := l.repairDataRepAdjacent(b)
		l.ResetRepairStatus(true)
		if err == nil {
			return data, nil
		}

		hasDataCnt := 0
		for j := 0; j < len(l.Blocks); j++ {
			theBlock := l.Blocks[j]
			if theBlock.HasData() {
				hasDataCnt++
			} else if !theBlock.IsParity {
				if theBlock.InternalNodePendingRepair() {
					// If repair of internal node was successful, we try to repair the originator block again. (l.WaitForNoPendingDL is called later on in the stack)
					if _, err := l.repairBlock(theBlock); err == nil {
						hasDataCnt = oldHasDataCnt - 1
						break
					}
				}
			}
		}
		if hasDataCnt == oldHasDataCnt {
			if b.HasData() {
				return b.Data, nil
			}

			return nil, err // Fatal error
		}
		oldHasDataCnt = hasDataCnt
	}
}

// repairDataRepAdjacent attempts to repair either of the datas parity pair.
func (l *Lattice) repairDataRepAdjacent(b *Block) ([]byte, error) {

	pairs := b.GetRepairPairs()
	for i := 0; i < len(pairs); i++ {
		if b.HasData() {
			return b.Data, nil
		}
		repPair := pairs[i]

		l.repairParity(repPair.Right, true)
		l.repairParity(repPair.Left, false)

		if b.Repair(repPair.Left, repPair.Right) == nil {
			return b.Data, nil
		}
	}
	if b.HasData() {
		return b.Data, nil
	}
	return nil, chunk.ErrChunkNotFound
}

// repairDataDLAdjacent attempts to repair a vertex using either of its edge-pairs.
// attempts to download the edges, but not to repair them.
func (l *Lattice) repairDataDLAdjacent(block *Block) bool {
	if block.HasData() {
		return true
	}

	repPairs := block.GetRepairPairs()
	for i := 0; i < len(repPairs); i++ {
		repPair := repPairs[i]
		blockChan := make(chan *Block, 2)
		defer close(blockChan)
		go l.downloadBlock(repPair.Left, blockChan)
		go l.downloadBlock(repPair.Right, blockChan)

		if block.Repair(<-blockChan, <-blockChan) == nil {
			return true
		}
	}
	return false
}

func (l *Lattice) downloadBlock(block *Block, resultChan chan<- *Block) {
	if !block.HasData() {
		if block.IsParity {
			if err := l.GetParity(block); err != nil {
				DebugPrint("downloadBlock. %v, error downloading: %v.\n", block, err)
			}
		} else {
			if shouldWait, statuschan := l.WaitForNoPendingDL(); shouldWait {
				<-statuschan
			}
		}
	}
	resultChan <- block
}

func (l *Lattice) replacedParityRepair(b *Block) bool {
	right := b.Right[0]
	rightDat := make([]byte, chunk.DefaultSize)
	for {
		if right.Position == b.Position {
			return b.RepairSuccess(rightDat)
		}
		if shouldWait, statuschan := l.WaitForNoPendingDL(); shouldWait {
			<-statuschan
		}
		if !right.HasData() {
			return false
		}
		rightDat = XORByteSlice(rightDat, right.Data)
		right = right.Right[b.Class].Right[0]
	}
}

// repairParity - Need connected data and party block.
func (l *Lattice) repairParity(block *Block, goRight bool) {
	if shouldWait, statuschan := l.WaitForNoPendingDL(); shouldWait {
		<-statuschan
	}
	if !block.IsParity {
		if block.RepairPending() && !l.repairDataDLAdjacent(block) {
			_, _ = l.repairDataRepAdjacent(block)
		}
		return
	} else if !block.ParityShouldRepair() {
		return
	}

	block.RepairPending()

	if block.Replace && l.replacedParityRepair(block) {
		return
	}

	allRepPair := block.GetRepairPairs() // Either 1 or 2 possible pairs.
	var repPair *RepairPair
	if goRight && !block.Replace {
		repPair = allRepPair[1] // Use only second element
	} else {
		repPair = allRepPair[0] // Use only first element
	}

	blockChan := make(chan *Block, 2)
	defer close(blockChan)
	go l.downloadBlock(repPair.Left, blockChan)
	go l.downloadBlock(repPair.Right, blockChan)

	if block.Repair(<-blockChan, <-blockChan) != nil {
		l.repairParity(repPair.Right, true)
		l.repairParity(repPair.Left, false)

		if block.Repair(repPair.Left, repPair.Right) != nil {
			block.RepairFailed()
		}
	}
}

func (l *Lattice) RepairAll() {
	l.lock.Lock()
	defer l.lock.Unlock()
	oldHasDataCnt := -1
	for {
		hasDataCnt := 0
		for i := 0; i < len(l.Blocks); i++ {
			b := l.Blocks[i]
			if b.HasData(true) {
				hasDataCnt++
				continue
			}
			if b.IsParity {
				l.repairParity(b, true)
			} else {
				l.repairBlock(b)
			}
			if b.HasData(true) {
				hasDataCnt++
			}
		}
		if oldHasDataCnt == hasDataCnt {
			return
		}
		oldHasDataCnt = hasDataCnt
	}
}

func (l *Lattice) GetIdentifier(block *Block) []byte {
	if block.Identifier != nil {
		return block.Identifier
	}
	if block.IsParity && block.HasData() {
		id, err := utils.GetAddrOfRawData(block.Data, storage.MakeHashFunc(storage.DefaultHash)())
		if err != nil {
			block.Identifier = id
			return id
		}
	}
	return nil
}

func (l *Lattice) GetChunk(addr []byte, index int) ([]byte, error) {
	b := l.GetBlock(index)
	if b.HasData() {
		return b.Data, nil
	}
	l.pendingDLchange(1)
	b.DownloadPending()
	b.Identifier = addr
	data, err := l.Getter.Get(l.ctx, addr)
	if err != nil {
		b.DownloadFailed()
	} else {
		b.DownloadSuccess(data)
	}
	l.pendingDLchange(-1)
	return data, err
}

func (l *Lattice) GetLeaf(rootaddr []byte, leafindex int) ([]byte, error) {
	return l.Getter.Get(context.WithValue(l.ctx, swarmconnector.Leafchunkid, leafindex), rootaddr)
}

func (l *Lattice) GetParity(b *Block) error {
	b.lock.Lock()
	if b.HasData(true) {
		b.lock.Unlock()
		return nil
	} else if b.DownloadStatus == DownloadPending {
		c := b.SubToStatusChange(true) // Maintain lock to ensure that the block is unchanged until we get the channel.
		b.lock.Unlock()
		<-c
		if b.HasData() {
			return nil
		}
		return chunk.ErrChunkNotFound
	} else {
		b.DownloadPending(true)
		b.lock.Unlock()
	}
	data, err := l.Getter.Get(context.WithValue(l.ctx, swarmconnector.Leafchunkid, b.Position), l.ParityRootID[b.Class])
	if err != nil {
		b.DownloadFailed()
	} else {
		b.DownloadSuccess(data)
	}
	return err
}

func (l *Lattice) GetRootIndex() int {
	return l.NumDataBlocks
}

// XORBlocks Figures out which is the related block between a and b and attempts to repair it.
// At least one of them needs to be a parity block.
func (l *Lattice) XORBlocks(a *Block, b *Block) (*Block, error) {
	// Case 0: Missing data
	if !a.HasData() || !b.HasData() {
		return nil, errors.New("missing data")
	}

	var bytedata []byte
	// Case 1: Both is data (Invalid case)
	if !a.IsParity && !b.IsParity {
		return nil, errors.New("at least one block must be parity")
	}

	// Case 2: Both are Parity
	if a.IsParity && b.IsParity {
		if a.Right[0] == b.Left[0] && !a.Replace {
			bytedata = XORByteSlice(a.Data, b.Data)
			a.Right[0].RepairSuccess(bytedata)
			return a.Right[0], nil
		} else if a.Left[0] == b.Right[0] && !b.Replace {
			bytedata = XORByteSlice(a.Data, b.Data)
			a.Left[0].RepairSuccess(bytedata)
			return a.Left[0], nil
		} else {
			return nil, errors.New("blocks are not connected")
		}
	}

	// Case 3: One is Parity, one is Data
	var data, parity *Block
	if !a.IsParity {
		data, parity = a, b
	} else {
		data, parity = b, a
	}

	if len(data.Right) > int(parity.Class) && data.Right[parity.Class] == parity { // Reconstruct left parity
		bytedata = XORByteSlice(data.Data, parity.Data)
		data.Left[parity.Class].RepairSuccess(bytedata)
		return data.Left[parity.Class], nil
	} else if len(data.Left) > int(parity.Class) && data.Left[parity.Class] == parity { // Reconstruct right parity
		bytedata = XORByteSlice(data.Data, parity.Data)
		data.Right[parity.Class].RepairSuccess(bytedata)
		return data.Right[parity.Class], nil
	} else if len(data.Right) > int(parity.Class) && data.Right[parity.Class].Replace && data.Right[parity.Class].Right[0] == parity.Left[0] { // Repair 2nd column data
		bytedata = XORByteSlice(data.Data, parity.Data)
		parity.Left[0].RepairSuccess(bytedata)
		return parity.Left[0], nil
	}
	return nil, errors.New("blocks are not connected")
}
