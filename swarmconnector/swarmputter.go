package swarmconnector

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/storage"
)

type SnarlPutter struct {
	storage.Putter
	Chunkstore chunk.Store
	endpoint   string
	putLimit   chan struct{}
}

const putLimit int = 20

func NewSnarlPutter(chunkStore storage.ChunkStore, tag *chunk.Tag, endpoint string) *SnarlPutter {
	return &SnarlPutter{storage.NewHasherStore(chunkStore,
		storage.MakeHashFunc(storage.DefaultHash), false, tag), chunkStore, endpoint, make(chan struct{}, putLimit)}
}

func (sp *SnarlPutter) UploadChunk(addr, data []byte) ([]byte, error) {
	uri := fmt.Sprintf("%v/%v/", sp.endpoint, "bzz-raw:")
	reader := bytes.NewReader(data)
	fmt.Printf("Data len: %v, reader len: %v, Size: %v\n", len(data), reader.Len(), reader.Size())
	sp.putLimit <- struct{}{}
	resp, err := http.Post(uri, "application/x-www-form-urlencoded", reader)

	<-sp.putLimit

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	dat, err := ioutil.ReadAll(resp.Body)
	fmt.Printf("Response: %v. Complete: %+v\n", string(dat), resp)
	return dat, err
}

func (sp *SnarlPutter) UploadFile(data io.Reader) ([]byte, *chunk.Tag, error) {
	uri := fmt.Sprintf("%v/%v/", sp.endpoint, "bzz:")
	sp.putLimit <- struct{}{}
	resp, err := http.Post(uri, "text/plain", data)
	<-sp.putLimit

	if err != nil {
		return nil, nil, err
	}

	defer resp.Body.Close()
	tag, err := sp.GetChunkTag(resp.Header.Get("x-swarm-tag"))
	if err != nil {
		return nil, nil, err
	}

	respByte, err := ioutil.ReadAll(resp.Body)
	return respByte, tag, err
}

func (sp *SnarlPutter) GetChunkTag(id string) (*chunk.Tag, error) {
	var uri string
	if _, err := strconv.ParseUint(id, 10, 32); err == nil {
		uri = fmt.Sprintf("%v/%v/?Id=%v", sp.endpoint, "bzz-tag:", id)
	} else {
		uri = fmt.Sprintf("%v/%v/%v", sp.endpoint, "bzz-tag:", id)
	}

	sp.putLimit <- struct{}{}
	resp, err := http.Get(uri)
	<-sp.putLimit

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http status error: %s", resp.Status)
	}

	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	chunkTag := &chunk.Tag{}
	err = json.Unmarshal(data, chunkTag)

	return chunkTag, err
}
