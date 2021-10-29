package utils

import (
	"crypto/sha256"
	"errors"
	"io"
	"os"

	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/storage"
)

func RemoveDecryptionKeyFromChunkHash(ref []byte, hashSize int) []byte {
	return ref[0:Min(len(ref), hashSize)]
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func GetAddrOfRawData(data []byte, hasher storage.SwarmHash) ([]byte, error) {
	if l := len(data); l < 9 || l > chunk.DefaultSize+8 {
		return nil, errors.New("invalid chunk size")
	}

	hasher.Reset()
	hasher.SetSpanBytes(data[:8])
	hasher.Write(data[8:])
	hash := hasher.Sum(nil)

	return hash, nil
}

func GetHashOfFile(filepath string) ([]byte, error) {
	file, err := os.Open(filepath)

	if err != nil {
		return nil, err
	}
	defer file.Close()
	hasher := sha256.New()

	if _, err := io.Copy(hasher, file); err != nil {
		return nil, err
	}

	return hasher.Sum(nil), nil
}
