package cmd

import (
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/storage"
	"github.com/relab/snarl-mw21/entangler"
	"github.com/relab/snarl-mw21/repair"
	"github.com/relab/snarl-mw21/swarmconnector"
	"github.com/relab/snarl-mw21/utils"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

// p - helical
// s - horizontal
// alpha - parities pr data
var alpha, s, p int
var doUpload, closelattice, listChunks bool
var usePyramid bool = false

var entangleCmd = &cobra.Command{
	Use:   "entangle [swarm hash or path]",
	Short: "Entangles a file",
	Long:  "Entangles a file using the given parameters",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		err := entangle(args[0], alpha, s, p)
		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)
		}
	},
}

func init() {
	entangleCmd.Flags().IntVarP(&alpha, "alpha", "a", 3, "Parities per data block.")
	entangleCmd.Flags().IntVarP(&p, "p", "p", 5, "Helical strands.")
	entangleCmd.Flags().IntVarP(&s, "s", "s", 5, "Horizontal strands.")
	entangleCmd.Flags().BoolVarP(&closelattice, "close", "c", true, "Closed Lattice")
	entangleCmd.Flags().BoolVarP(&listChunks, "listchunks", "l", true, "Just list all the chunks addresses. No entangling.")

	entangleCmd.Flags().BoolVarP(&doUpload, "doupload", "u", true, "Upload entangled file to Swarm")

	rootCmd.AddCommand(entangleCmd)
}

func entangle(hashorpath string, alpha, s, p int) error {
	var err error
	var path string
	dataAddr, err := hexutil.Decode(hashorpath)
	if err != nil {
		path, err = entangleFile(hashorpath, alpha, s, p)
	} else {
		path, err = entangleSwarmfile(dataAddr, alpha, s, p)
	}

	if err != nil {
		log.Fatal(err.Error())
	}
	if doUpload {
		for i := 0; i < alpha; i++ {
			path := filepath.Join(path, strconv.Itoa(i))
			manifestHash, contentHash, tagHash, err := uploadFile(path)
			if err != nil {
				fmt.Printf("Could not upload file. Error: %v\n", err.Error())
			} else {
				os.Remove(path)
				fmt.Printf("Uploaded parity to Swarm. Manifest hash: %v, Tag hash: %v, Content hash: %v. Class: %d\n", string(manifestHash), tagHash, string(contentHash), i)
			}
		}
	} else {
		fmt.Printf("Entangled files located at: %v\n", path)
	}

	return err
}

func entangleFile(path string, alpha, s, p int) (string, error) {
	reader, err := os.Open(path)
	if err != nil {
		log.Fatalf("Could not open file. %v", err)
	}
	testtag := chunk.NewTag(0, "test-tag", 0, false)

	putGetter := storage.NewHasherStore(utils.NewMapChunkStore(), storage.MakeHashFunc(storage.DefaultHash), false, testtag)

	//var addr storage.Address
	var wait func(context.Context) error
	var rootAddr chunk.Address

	ctx := context.TODO()
	// fileinfo, _ := reader.Stat()
	// yeah := make([]byte, fileinfo.Size())
	// reader.Read(yeah)
	// hash, _ := utils.GetAddrOfRawData(yeah, storage.MakeHashFunc(storage.DefaultHash)())
	// fmt.Printf("HASH %x. Data: %v\n", hash, yeah)
	// return "", nil
	if usePyramid {
		rootAddr, wait, err = storage.PyramidSplit(ctx, reader, putGetter, putGetter, testtag)
	} else {
		fileinfo, _ := reader.Stat()
		rootAddr, wait, err = storage.TreeSplit(ctx, reader, fileinfo.Size(), putGetter)
	}
	fmt.Println(rootAddr)
	if err = wait(ctx); err != nil {
		return "", err
	}

	treeRoot, err := swarmconnector.BuildCompleteTree(ctx, putGetter, storage.Reference(rootAddr),
		swarmconnector.BuildTreeOptions{}, repair.NewMockRepair(putGetter))
	if err != nil {
		return "", err
	}

	// Flatten the tree in canonical order.
	flatTree := treeRoot.FlattenTreeWindow(s, p)

	dataChunks := make([][]byte, treeRoot.Index)
	for i := 0; i < len(flatTree); i++ {
		if listChunks {
			fmt.Printf("%x\n", flatTree[i].Key)
		}
		dataChunks[i] = flatTree[i].Data[swarmconnector.ChunkSizeOffset:]
	}
	if listChunks {
		return "", errors.New("Just listed all keys.")
	}
	return handleEntangleBlocks(dataChunks, alpha, s, p)
}

func entangleSwarmfile(swarmhash []byte, alpha, s, p int) (string, error) {
	var err error
	sc := swarmconnector.NewSwarmConnector(ChunkDBPath, bzzKey, ChunkDBPath)
	var trees []*swarmconnector.TreeChunk
	trees, err = sc.BuildMultiTrees(swarmhash)
	if err != nil {
		return "", err
	}

	// Flatten the tree in canonical order.
	flatTree := trees[0].FlattenTreeWindow(s, p)

	dataChunks := make([][]byte, trees[0].Index)
	for i := 0; i < len(flatTree); i++ {
		dataChunks[i] = flatTree[i].Data[swarmconnector.ChunkSizeOffset:]
	}

	return handleEntangleBlocks(dataChunks, alpha, s, p)
}

func handleEntangleBlocks(data [][]byte, alpha, s, p int) (string, error) {
	tangler := entangler.NewEntangler(p, p, s, alpha, chunk.DefaultSize)
	result := make(chan *entangler.EntangledBlock)
	done := make(chan struct{})

	dir, err := ioutil.TempDir("", "entangled-files")
	if err != nil {
		return "", err
	}

	files := make([]*os.File, alpha)
	buffers := make([]*bufio.Writer, alpha)
	entangledBlocks := make([]*entangler.EntangledBlock, 0, len(data))

	// Setup
	for i := 0; i < alpha; i++ {
		files[i], err = os.Create(filepath.Join(dir, strconv.Itoa(i)))
		buffers[i] = bufio.NewWriter(files[i])
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer close(result)
	ResultLoop:
		for {
			select {
			case block := <-result:
				if block.LeftIndex < 1 {
					continue
				}
				if block.Replace {
					for i := 0; i < len(entangledBlocks); i++ {
						if entangledBlocks[i].LeftIndex == block.LeftIndex &&
							entangledBlocks[i].RightIndex == block.RightIndex {
							entangledBlocks[i].Data = block.Data
							entangledBlocks[i].Replace = true
							break
						}
					}
				} else {
					entangledBlocks = append(entangledBlocks, block)
				}
			case <-done:
				break ResultLoop
			}
		}
		wg.Done()
	}()
	for i, j := 0, 1; i < len(data); i, j = i+1, j+1 {
		tangler.Entangle(data[i], j, result)
	}

	tangler.WrapLattice(result)
	done <- struct{}{}
	wg.Wait()
	for i := 0; i < alpha; i++ {
		ebSorted := make(map[int][]byte)
		for k := 0; k < len(entangledBlocks); k++ {
			if int(entangledBlocks[k].Class) != i {
				continue
			}
			ebSorted[entangledBlocks[k].LeftIndex-1] = entangledBlocks[k].Data
		}
		for k := 0; k < len(entangledBlocks); k++ {
			buffers[i].Write(ebSorted[k])
		}
		buffers[i].Flush()
		files[i].Close()
	}

	return dir, nil
}
