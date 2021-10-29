package cmd

import (
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

var ChunkDBPath, SnarlDBPath, bzzKey, ipcPath string
var minNumPeers int

var rootCmd = &cobra.Command{
	Use: "snarl",
}

func Execute() {
	rootCmd.PersistentFlags().StringVarP(&ChunkDBPath, "chunkdbpath", "", "", "Physical location of chunks.")
	rootCmd.PersistentFlags().StringVarP(&bzzKey, "bzzkey", "", "", "Bzzkey of account that uploaded content.")
	rootCmd.PersistentFlags().StringVarP(&SnarlDBPath, "snarldbpath", "", "", "Physical location of Snarl chunks.")
	rootCmd.PersistentFlags().StringVarP(&ipcPath, "ipcpath", "", "", "Ethereum Inter-process Communications file")
	rootCmd.PersistentFlags().IntVarP(&minNumPeers, "numPeers", "", 9, "Minimum number of peers connected")

	_ = rootCmd.Execute()
}

func waitConnectionToPeers(minNumPeers int) error {
	client, _ := rpc.DialIPC(context.Background(), ipcPath)
	retryLeft := 3000 // Try 300 * 100ms = 300 seconds
	var peers []*p2p.PeerInfo
	_ = client.Call(&peers, "admin_peers")

	for len(peers) < minNumPeers && retryLeft > 0 {
		time.Sleep(100 * time.Millisecond)
		_ = client.Call(&peers, "admin_peers")
		retryLeft--
	}

	fmt.Printf("Connected to %v peers. Retries left: %v\n", len(peers), retryLeft)
	if retryLeft == 0 {
		return errors.New("Could not connect.")
	}
	return nil
}
