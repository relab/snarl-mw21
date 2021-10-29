package swarmconnector

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethersphere/swarm/api"
	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/storage"
	"github.com/ethersphere/swarm/storage/localstore"
	"github.com/relab/snarl-mw21/repair"
	"github.com/relab/snarl-mw21/utils"
	"github.com/zloylos/grsync"
	"golang.org/x/sync/semaphore"
)

const defaultLDBCapacity = 5000000 // capacity for LevelDB, by default 5*10^6*4096 bytes == 20GB
var SyncDB func()

type DB struct {
	*localstore.DB
}

type SwarmConnector struct {
	Getter         *SnarlGetter
	Putter         *SnarlPutter
	HashSize       int
	Hasher         storage.SwarmHash
	LStore         *DB
	FileStore      *storage.FileStore
	Ctx            context.Context
	Tags           *chunk.Tags
	Swarmapi       *api.API
	swarmChunkPath string
	SnarlChunkPath string
}

func NewSwarmConnector(ChunkDBPath, bzzKey, SnarlDBPath string) *SwarmConnector {
	if ChunkDBPath != SnarlDBPath {
		SyncDB = func() { syncLocalDB(ChunkDBPath, SnarlDBPath) }
		if !utils.GLOBAL_Benchmark {
			SyncDB()
		} else {
			os.RemoveAll(SnarlDBPath)
		}
	} else {
		SyncDB = func() {}
	}

	var err error
	tags := chunk.NewTags()
	bzzKeyByte := common.FromHex(bzzKey)
	lStore2, err := localstore.New(SnarlDBPath, bzzKeyByte, &localstore.Options{
		MockStore: nil,
		Capacity:  defaultLDBCapacity,
		Tags:      tags,
	})

	if err != nil {
		fmt.Printf("could not create localstore... Error: %+v\n", err)
		return nil
	}
	lStore := &DB{lStore2}

	ctx := context.Background()
	tag, _ := tags.GetFromContext(ctx)
	getter := NewSnarlGetter(lStore2, tag, "http://localhost:8500")
	putter := NewSnarlPutter(lStore2, tag, "http://localhost:8500")

	fileStore := storage.NewFileStore(lStore2, lStore2, storage.NewFileStoreParams(), tags)
	hasher := storage.MakeHashFunc(storage.DefaultHash)()
	return &SwarmConnector{
		LStore:         lStore,
		Tags:           tags,
		FileStore:      fileStore,
		Ctx:            ctx,
		HashSize:       hasher.Size(),
		Hasher:         hasher,
		Swarmapi:       api.NewAPI(fileStore, nil, nil, nil, nil, tags),
		Getter:         getter,
		swarmChunkPath: ChunkDBPath,
		SnarlChunkPath: SnarlDBPath,
		Putter:         putter,
	}
}

func (sc *SwarmConnector) BuildMultiTrees(addr []byte) ([]*TreeChunk, error) {
	// Check if its a manifest we are looking at
	manifestlist, err := sc.Swarmapi.GetManifestList(sc.Ctx, nil, addr, "")

	if err == nil {
		treechunks := make([]*TreeChunk, 0)
		for i := 0; i < len(manifestlist.Entries); i++ {
			rootAddr, err := hexutil.Decode("0x" + manifestlist.Entries[i].Hash)
			if err != nil {
				continue
			}
			rootchunks, err := sc.BuildMultiTrees(rootAddr)
			if err != nil {
				return nil, err
			}
			treechunks = append(treechunks, rootchunks...)
		}
		return treechunks, errors.New("Manifestlist")
	}

	tree, err := BuildCompleteTree(sc.Ctx, sc.LStore, addr, BuildTreeOptions{}, repair.NewMockRepair(sc.LStore))

	return []*TreeChunk{tree}, err
}

// GetChunk retrieves chunks from the localstore.
func (sc *SwarmConnector) GetChunk(addr []byte) (storage.ChunkData, error) {
	return sc.LStore.Get(sc.Ctx, addr)
}

func (sc *SwarmConnector) removeDecryptionKeyFromChunkHash(ref []byte) []byte {
	return utils.RemoveDecryptionKeyFromChunkHash(ref, sc.HashSize)
}

var semSync = semaphore.NewWeighted(1)

func syncLocalDB(src, dest string) {
	if !semSync.TryAcquire(1) {
		return
	}
	defer semSync.Release(1)

	task := grsync.NewTask(src, dest,
		grsync.RsyncOptions{Recursive: true, Update: false,
			Existing: false, IgnoreExisting: false, Delete: true, Verbose: true, Quiet: false},
	)

	err := task.Run()
	var retry int = 10
	for err != nil && retry > 0 {
		time.Sleep(10 * time.Millisecond)
		err = task.Run()
		retry--
	}
}
