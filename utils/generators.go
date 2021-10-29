package utils

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"time"

	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/storage"
)

const usePyramid = false

// GenerateRandomData generates [size] random bytes
func GenerateRandomData(size int, hasher, dir string) (rootAddr storage.Address, reader *storage.LazyChunkReader, getter storage.Getter, err error) {
	var data io.Reader
	input := GenerateRandomBytes(size, time.Now().UnixNano())
	data = bytes.NewReader(input)

	testtag := chunk.NewTag(0, "test-tag", 0, false)

	putGetter := storage.NewHasherStore(NewMapChunkStore(), storage.MakeHashFunc(hasher), false, testtag)

	//var addr storage.Address
	var wait func(context.Context) error

	ctx := context.TODO()

	if usePyramid {
		rootAddr, wait, err = storage.PyramidSplit(ctx, data, putGetter, putGetter, testtag)
	} else {
		rootAddr, wait, err = storage.TreeSplit(ctx, data, int64(size), putGetter)
	}

	if err != nil {
		return
	}

	err = wait(ctx)
	if err != nil {
		return
	}

	reader = storage.TreeJoin(ctx, rootAddr, putGetter, 0)

	return rootAddr, reader, putGetter, nil
}

func GenerateRandomBytes(count int, seed int64) []byte {
	ret := make([]byte, count)
	reader := rand.New(rand.NewSource(seed))
	n := 0
	for n < count {
		num, _ := reader.Read(ret[n:])
		n += num
	}
	return ret
}
