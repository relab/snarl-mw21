package swarmconnector

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/storage"
	"github.com/relab/snarl-mw21/utils"
)

// MemoryGetter retrieves chunks from memory using the same interface as Swarms Get method.
// It's constructor accepts a list of failed chunks - which can be used for testing purposes.

type MemoryGetter struct {
	storage.Getter
	getLimit       chan struct{}
	dataChunks     *TreeChunk
	dataMap        map[string]*TreeChunk
	parityChunks   []*TreeChunk
	failedChunks   map[string]BlockFailure
	failedChildren []map[int]BlockFailure
}

func NewMemoryGetter(dataChunks *TreeChunk, parityChunks []*TreeChunk, failedChunks map[string]BlockFailure,
	failedChildren []map[int]BlockFailure) *MemoryGetter {
	dataMap := make(map[string]*TreeChunk)
	var walker func(*TreeChunk)
	walker = func(c *TreeChunk) {
		for j := range c.Children {
			walker(c.Children[j])
		}
		key := fmt.Sprintf("%x", c.Key)
		dataMap[key] = c
	}
	walker(dataChunks)

	return &MemoryGetter{
		getLimit:       make(chan struct{}, getLimit),
		dataChunks:     dataChunks,
		parityChunks:   parityChunks,
		failedChunks:   failedChunks,
		failedChildren: failedChildren,
		dataMap:        dataMap,
	}
}

// Get on the embedded type SnarlGetter first attempts to see if the requested chunk
// is available in local storage. If it is not available it will issue a request to
// the local swarm node and sync the snarl database before returning the value.
// It accepts a parameter in context in order to request a specific child leaf that
// we do not know the address of.
func (gc *MemoryGetter) Get(ctx context.Context, ref storage.Reference) (chunkdata storage.ChunkData, err error) {
	strRef := fmt.Sprintf("%x", ref)
	if fail, ok := gc.failedChunks[strRef]; ok {
		if fail.Class == Unavailable {
			if fail.Delay > 0 {
				time.Sleep(fail.Delay)
			}
			return nil, chunk.ErrChunkNotFound
		} else if fail.Class == Delay {
			time.Sleep(fail.Delay)
		} else if fail.Class == Corrupt {
			return utils.GenerateRandomBytes(chunk.DefaultSize, time.Now().UnixNano()), nil
		}
	} else {
		for i := 0; i < len(gc.parityChunks); i++ {
			if bytes.Equal(ref, gc.parityChunks[i].Key) {
				if chunkid, ok := ctx.Value(Leafchunkid).(int); ok {
					tc, err := gc.parityChunks[i].GetChildFromMemWithFail(chunkid, gc.failedChildren[i])
					if err != nil {
						return nil, err
					}
					return tc.Data, nil
				}
				return nil, chunk.ErrChunkInvalid
			}
		}
	}
	return gc.dataMap[strRef].Data, nil
}

type BlockFailure struct {
	Index int
	Delay time.Duration
	Class FailType
}

type FailType int

const (
	Delay = iota
	Unavailable
	Corrupt
)
