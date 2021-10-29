package swarmconnector

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/storage"
	"github.com/relab/snarl-mw21/utils"
)

type SnarlGetter struct {
	storage.Getter
	chunkstore chunk.Store
	endpoint   string
	getLimit   chan struct{}
}

const Leafchunkid int = 0
const getLimit int = 55 // Higher than 200 causes errors

func NewSnarlGetter(chunkStore storage.ChunkStore, tag *chunk.Tag, endpoint string) *SnarlGetter {
	return &SnarlGetter{storage.NewHasherStore(chunkStore,
		storage.MakeHashFunc(storage.DefaultHash), false, tag), chunkStore, endpoint, make(chan struct{},
		getLimit)}
}

func (gc *SnarlGetter) Download(ctx context.Context, ref storage.Reference) (storage.ChunkData, error) {
	uri := fmt.Sprintf("%v/%v/%x", gc.endpoint, "bzz-raw:", ref)

	gc.acquire()
	defer gc.release()
	resp, err := http.Get(uri)

	if err != nil {
		return nil, err
	}

	// Sync database between swarm and snarl.
	SyncDB()

	defer resp.Body.Close()
	filedata, err := ioutil.ReadAll(resp.Body)

	return filedata, err
}

// Get on the embedded type SnarlGetter first attempts to see if the requested chunk
// is available in local storage. If it is not available it will issue a request to
// the local swarm node and sync the snarl database before returning the value.
// It accepts a parameter in context in order to request a specific child leaf that
// we do not know the address of.
func (gc *SnarlGetter) Get(ctx context.Context, ref storage.Reference) (chunkdata storage.ChunkData, err error) {
	var uri string
	var chunkid int
	var ok bool
	strRef := fmt.Sprintf("%x", ref)
	if chunkid, ok = ctx.Value(Leafchunkid).(int); ok {
		uri = fmt.Sprintf("%v/%v/%v/%v", gc.endpoint, "bzz-chunk:", strRef, chunkid)
	} else {
		uri = fmt.Sprintf("%v/%v/%v", gc.endpoint, "bzz-chunk:", strRef)
	}

	// First attempt to see if its available at the local storage.
	chunkdata, err = gc.Getter.Get(ctx, ref)

	// It's in local storage - Check if a specific child is requested else return the chunk.
	if err == nil {
		if !ok {
			return
		}
		tcs, _ := GetChildrenFromNet(ctx, gc.Getter, ref, nil, chunkid)

		chunkdata = tcs[0].Data
		return
	}

	// We request the chunk from the local node. Semaphore limits concurrency
	gc.acquire()
	defer gc.release()
	resp, err := http.Get(uri)

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, chunk.ErrChunkNotFound
	}

	chunkdata, err = ioutil.ReadAll(resp.Body)

	if len(chunkdata) == 0 || chunkdata.Size() == 0 {
		return nil, chunk.ErrChunkNotFound
	}

	// Sync database between swarm and snarl.
	var addr []byte
	if chunkid > 0 {
		addr, _ = utils.GetAddrOfRawData(chunkdata, storage.MakeHashFunc(storage.DefaultHash)())
	} else {
		addr = ref
	}

	if _, err = gc.chunkstore.Put(ctx, chunk.ModePutRequest, chunk.NewChunk(addr, chunkdata)); err != nil {
		return nil, err
	}

	return
}

func (gc *SnarlGetter) acquire() {
	gc.getLimit <- struct{}{}
}

func (gc *SnarlGetter) release() {
	<-gc.getLimit
}

func (sc *SnarlGetter) GetChunks(ctx context.Context, refs ...storage.Reference) ([]storage.ChunkData, []error) {
	chunkdata := make([]storage.ChunkData, len(refs))
	err := make([]error, len(refs))

	for i := 0; i < len(refs); i++ {
		chunkdata[i], err[i] = sc.Get(ctx, refs[i])
	}

	return chunkdata, err
}
