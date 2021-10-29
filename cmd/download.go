package cmd

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethersphere/swarm/api"
	"github.com/ethersphere/swarm/chunk"
	"github.com/relab/snarl-mw21/entangler"
	"github.com/relab/snarl-mw21/swarmconnector"
	"github.com/relab/snarl-mw21/utils"
	"github.com/spf13/cobra"
)

var doRepair bool

var downloadCmd = &cobra.Command{
	Use:   "download [swarm hashes]",
	Short: "Download and repair a file from Swarm",
	Long:  "Downloads and if neccessary repairs and uploads the file to Swarm",
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			log.Fatalf("Must specify swarm hash.")
		}
		swarmhashes := strings.Split(args[0], ",")
		if len(swarmhashes) == 1 {
			downloadFile(0, swarmhashes[0:], alpha, s, p, false)
			return
		}
		for i := 1; i < len(swarmhashes); i++ {
			if !strings.HasPrefix(swarmhashes[i], "0x") {
				swarmhashes[i] = "0x" + swarmhashes[i]
			}
		}
		size, _ := strconv.ParseInt(swarmhashes[0], 16, 64)
		downloadFile(uint64(size), swarmhashes[1:], alpha, s, p, false)
	},
}

func init() {
	downloadCmd.Flags().IntVarP(&alpha, "alpha", "a", 3, "Parities per data block.")
	downloadCmd.Flags().IntVarP(&p, "p", "p", 5, "Helical strands.")
	downloadCmd.Flags().IntVarP(&s, "s", "s", 5, "Horizontal strands.")
	downloadCmd.Flags().BoolVarP(&closelattice, "close", "c", true, "Closed Lattice")
	downloadCmd.Flags().BoolVarP(&utils.GLOBAL_Benchmark, "benchmark", "b", false, "Run in benchmark mode.")
	downloadCmd.Flags().BoolVarP(&doRepair, "dorepair", "u", true, "Re-upload repaired chunks to Swarm")
	downloadCmd.Flags().StringVarP(&utils.GLOBAL_ExpectedOutput, "hashoutput", "", "", "Expected hash output in benchmark.")
	downloadCmd.Flags().IntVarP(&utils.GLOBAL_Failrate, "failrate", "", 0, "Random failure rate during tests")
	downloadCmd.Flags().IntVarP(&utils.GLOBAL_Failednodes, "failednodes", "", 0, "Network nodes failed")

	rootCmd.AddCommand(downloadCmd)
}

// downloadFile
// Params: size - [in hex] number of bytes of original file.
func downloadFile(size uint64, swarmHashes []string, alpha, s, p int, doRepair bool) error {
	// 1. Setup the swarmconnector
	sc := swarmconnector.NewSwarmConnector(ChunkDBPath, bzzKey, SnarlDBPath)

	dataAddr, err := hexutil.Decode(swarmHashes[0])
	if err != nil {
		log.Fatalf(err.Error())
	}
	parityAddrs := make([][]byte, len(swarmHashes)-1)
	for i := 1; i < len(swarmHashes); i++ {
		parityAddrs[i-1], err = hexutil.Decode(swarmHashes[i])
		if err != nil {
			log.Fatalf(err.Error())
		}
	}
	dir, err := ioutil.TempDir("", "downloaded-files")
	if err != nil {
		return err // ??
	}

	// 2. Ensure we are connected to enough peers
	if err := waitConnectionToPeers(minNumPeers); err != nil {
		log.Fatal(err)
	}

	t := time.Now().UnixNano()

	// 2. Try to retrieve normally.
	if size == 0 && regularDownload(sc, dataAddr, dir) == nil {
		if utils.GLOBAL_Benchmark {
			fmt.Printf("Download complete.\n")
			fmt.Printf("%d,%d\n", t, time.Now().UnixNano())
			if utils.GLOBAL_ExpectedOutput != "" {
				hashInput, _ := hex.DecodeString(utils.GLOBAL_ExpectedOutput)
				hashOutput, _ := utils.GetHashOfFile(dir + "/download")
				hashEqual := bytes.Equal(hashInput, hashOutput)
				if !hashEqual {
					fmt.Printf("HASHES NOT EQUAL. Input: %x, Output: %x\n", hashInput, hashOutput)
				} else {
					fmt.Printf("Hashes Equal. Hash: %x\n", hashInput)
				}
			}
		}
		return nil
	}

	if size == 0 {
		return nil // Error. Need to know size (For now ... Can do some tricks)
	}

	var filename string = "/download"

	lattice := entangler.NewSwarmLattice(sc.Ctx, alpha, s, p, size, sc.Getter, dataAddr, parityAddrs, chunk.DefaultSize)
	tc, err := swarmconnector.BuildCompleteTree(sc.Ctx, sc.Getter, dataAddr, swarmconnector.BuildTreeOptions{}, lattice)

	if err != nil {
		if utils.GLOBAL_Benchmark {
			datablocks, parityblocks := 0, 0
			for i := 0; i < len(lattice.Blocks); i++ {
				b := lattice.Blocks[i]
				if b.HasData() {
					if b.DownloadStatus == entangler.DownloadSuccess {
						if b.IsParity {
							parityblocks++
						} else {
							datablocks++
						}
					}
					fmt.Printf("%t,%d,%d,%d,%t,%d,%d,%t\n", b.IsParity, b.Position,
						b.LeftPos(0), b.RightPos(0), b.HasData(), b.DownloadTime.StartTime,
						b.DownloadTime.EndTime, b.DownloadStatus == entangler.DownloadSuccess)
				}
			}
			fmt.Printf("Download FAILED. Datablocks: %d/%d, Parityblocks: %d/%d\n", datablocks, lattice.NumDataBlocks, parityblocks, len(lattice.Blocks)-lattice.NumDataBlocks)
			fmt.Println(err.Error())
		}
		log.Fatalf(err.Error())
	}

	dataChunks := make([][]byte, 0, tc.Index)
	var walker func(*swarmconnector.TreeChunk)
	walker = func(c *swarmconnector.TreeChunk) {
		for j := range c.Children {
			walker(c.Children[j])
		}
		if c.SubtreeSize <= uint64(len(c.Data)) {
			dataChunks = append(dataChunks, c.Data[swarmconnector.ChunkSizeOffset:])
		}
	}
	walker(tc)

	if err := RebuildFile(dir+filename, dataChunks...); err == nil {
		if utils.GLOBAL_Benchmark {
			datablocks, parityblocks := 0, 0
			for i := 0; i < len(lattice.Blocks); i++ {
				b := lattice.Blocks[i]
				if b.HasData() {
					if b.DownloadStatus == entangler.DownloadSuccess {
						if b.IsParity {
							parityblocks++
						} else {
							datablocks++
						}
					}
					fmt.Printf("%t,%d,%d,%d,%t,%d,%d,%t\n", b.IsParity, b.Position,
						b.LeftPos(0), b.RightPos(0), b.HasData(), b.DownloadTime.StartTime,
						b.DownloadTime.EndTime, b.DownloadStatus == entangler.DownloadSuccess)
				}
			}
			fmt.Printf("Download complete. Datablocks: %d/%d, Parityblocks: %d/%d\n", datablocks, lattice.NumDataBlocks, parityblocks, len(lattice.Blocks)-lattice.NumDataBlocks)
			fmt.Printf("%d,%d\n", t, time.Now().UnixNano())
			if utils.GLOBAL_ExpectedOutput != "" {
				hashInput, _ := hex.DecodeString(utils.GLOBAL_ExpectedOutput)
				hashOutput, _ := utils.GetHashOfFile(dir + filename)
				hashEqual := bytes.Equal(hashInput, hashOutput)
				if !hashEqual {
					fmt.Printf("HASHES NOT EQUAL. Input: %x, Output: %x\n", hashInput, hashOutput)
				} else {
					fmt.Printf("Hashes Equal. Hash: %x\n", hashInput)
				}
			}
		}
		fmt.Printf("Output file with repairs: %v\n", dir+filename)
	} else {
		fmt.Printf("Error downloading file. %+v\n", err)
	}

	return nil
}

func RebuildFile(filePath string, Chunks ...[]byte) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	var buffer *bufio.Writer = bufio.NewWriter(file)

	for i := 0; i < len(Chunks); i++ {
		buffer.Write(Chunks[i])
	}
	buffer.Flush()
	return file.Close()
}

func regularDownload(sc *swarmconnector.SwarmConnector, dataAddr []byte, dir string) error {
	var filename string
	var err error
	var data []byte
	data, err = sc.Getter.Download(sc.Ctx, dataAddr)
	if err == nil {
		var manifestList api.ManifestList
		manifestList, err = sc.Swarmapi.GetManifestList(sc.Ctx, nil, dataAddr, "")
		if err == nil {
			if manifestList.Entries[0].Path != "/" {
				filename = manifestList.Entries[0].Path
			}

			dataAddr, _ = hexutil.Decode("0x" + manifestList.Entries[0].Hash)
			data, err = sc.Getter.Download(sc.Ctx, dataAddr)
		}
		if filename == "" {
			filename = "/download"
		}

		if RebuildFile(dir+filename, data) == nil {
			fmt.Printf("Output file without any failure: %v\n", dir+"/download")
			return nil
		}
		fmt.Printf("Error downloading file.") // Can this happen ?)
	}
	return err
}
