package utils

import (
	"context"
	"sync"

	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/storage"
)

type MapChunkStore struct {
	chunks map[string]storage.Chunk
	mu     sync.RWMutex
}

func NewMapChunkStore() *MapChunkStore {
	return &MapChunkStore{
		chunks: make(map[string]storage.Chunk),
	}
}

// Put Implementation of Put
func (m *MapChunkStore) Put(_ context.Context, _ chunk.ModePut, chs ...storage.Chunk) ([]bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	exist := make([]bool, len(chs))
	for i, ch := range chs {
		addr := ch.Address().Hex()
		_, exist[i] = m.chunks[addr]
		m.chunks[addr] = ch
	}
	return exist, nil
}

func (m *MapChunkStore) Get(_ context.Context, _ chunk.ModeGet, ref storage.Address) (storage.Chunk, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	chunk := m.chunks[ref.Hex()]
	if chunk == nil {
		return nil, storage.ErrChunkNotFound
	}
	return chunk, nil
}

func (m *MapChunkStore) GetMulti(_ context.Context, _ chunk.ModeGet, refs ...storage.Address) (chunks []storage.Chunk, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, ref := range refs {
		chunk := m.chunks[ref.Hex()]
		if chunk == nil {
			return nil, storage.ErrChunkNotFound
		}
		chunks = append(chunks, chunk)
	}
	return chunks, nil
}

// Need to implement Has from SyncChunkStore
func (m *MapChunkStore) Has(ctx context.Context, ref storage.Address) (has bool, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, has = m.chunks[ref.Hex()]
	return has, nil
}

func (m *MapChunkStore) HasMulti(ctx context.Context, refs ...storage.Address) (have []bool, err error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	have = make([]bool, len(refs))
	for i, ref := range refs {
		_, have[i] = m.chunks[ref.Hex()]
	}
	return have, nil
}

func (m *MapChunkStore) Set(ctx context.Context, mode chunk.ModeSet, addrs ...chunk.Address) (err error) {
	return nil
}

func (m *MapChunkStore) LastPullSubscriptionBinID(bin uint8) (id uint64, err error) {
	return 0, nil
}

func (m *MapChunkStore) SubscribePull(ctx context.Context, bin uint8, since, until uint64) (c <-chan chunk.Descriptor, stop func()) {
	return nil, nil
}

func (m *MapChunkStore) Close() error {
	return nil
}
