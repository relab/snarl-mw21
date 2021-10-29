package cmd

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethersphere/swarm/chunk"
	"github.com/ethersphere/swarm/storage"
	"github.com/relab/snarl-mw21/swarmconnector"
	"github.com/relab/snarl-mw21/utils"
	"github.com/spf13/cobra"
	"golang.org/x/net/context"
)

var verifyUpload, verbose, simulateUpload bool

var uploadCmd = &cobra.Command{
	Use:   "upload [path]",
	Short: "Upload a file to Swarm",
	Long:  "Upload a file to Swarm",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		if verifyUpload {
			_, err := verifyUploadSynced(args[0])

			if err != nil {
				fmt.Printf("Err: %v\n", err)
			}
			return
		}
		var manifestHash, contentHash, tagHash []byte
		var err error
		if simulateUpload {
			contentHash, err = getContentHashForFile(args[0])
		} else {
			manifestHash, contentHash, tagHash, err = uploadFile(args[0])
		}

		if err != nil {
			fmt.Println(err.Error())
			os.Exit(1)

		}
		fmt.Printf("Uploaded file to Swarm. Manifest hash: %v, Tag hash: %064x, Content hash: %064x\n", string(manifestHash), tagHash, contentHash)
	},
}

func init() {
	uploadCmd.Flags().BoolVarP(&verifyUpload, "verifyupload", "v", false, "Just verify that the upload was successfully synced in the network.")
	uploadCmd.Flags().BoolVarP(&verbose, "verbose", "", false, "Verbose syncing printing")
	uploadCmd.Flags().BoolVarP(&simulateUpload, "simulate", "", false, "Simulate upload")
	rootCmd.AddCommand(uploadCmd)
}

func verifyUploadSynced(hash string) (bool, error) {
	sc := swarmconnector.NewSwarmConnector(ChunkDBPath, bzzKey, SnarlDBPath)
	tag, err := sc.Putter.GetChunkTag(hash)

	if err != nil {
		return false, err
	}

	fmt.Printf("%+v", tag)

	return true, nil
}

// uploadFile returns ContentHash, (error).
func getContentHashForFile(filepath string) ([]byte, error) {
	reader, err := os.Open(filepath)
	if err != nil {
		log.Fatalf("Could not open file. %v", err)
	}
	defer reader.Close()

	testtag := chunk.NewTag(0, "test-tag", 0, false)

	putGetter := storage.NewHasherStore(utils.NewMapChunkStore(), storage.MakeHashFunc(storage.DefaultHash), false, testtag)

	var wait func(context.Context) error
	var rootAddr chunk.Address

	ctx := context.TODO()

	if usePyramid {
		rootAddr, wait, err = storage.PyramidSplit(ctx, reader, putGetter, putGetter, testtag)
	} else {
		fileinfo, _ := reader.Stat()
		rootAddr, wait, err = storage.TreeSplit(ctx, reader, fileinfo.Size(), putGetter)
	}

	if err = wait(ctx); err != nil {
		return nil, err
	}

	return rootAddr, nil
}

// uploadFile returns ManifestHash, ContentHash, TagHash, (error).
func uploadFile(filepath string) ([]byte, []byte, []byte, error) {
	sc := swarmconnector.NewSwarmConnector(ChunkDBPath, bzzKey, SnarlDBPath)

	// 2. Ensure we are connected to enough peers
	if err := waitConnectionToPeers(minNumPeers); err != nil {
		return nil, nil, nil, err
	}

	file, err := os.Open(filepath)

	if err != nil {
		return nil, nil, nil, err
	}
	defer file.Close()

	manifestHash, tag, err := sc.Putter.UploadFile(file)
	if err != nil {
		return nil, nil, nil, err
	}

	// Ensure that the syncing is completed before we continue.
	seen, total, err := tag.Status(chunk.StateSeen)
	if total-seen > 0 {
		waitForSyncing(sc.Putter, tag.Address.String())
	}

	tmphash, _ := hexutil.Decode("0x" + string(manifestHash))
	manifestList, err := sc.Swarmapi.GetManifestList(sc.Ctx, nil, tmphash, "")
	if err == nil {
		contenthash, _ := hexutil.Decode("0x" + manifestList.Entries[0].Hash)
		return manifestHash, contenthash, tag.Address, nil
	}

	// Could not retrieve the manifest for some reason.. Trying to figure out the contentHash by ourselves.
	testtag := chunk.NewTag(0, "test-tag", 0, false)
	putGetter := storage.NewHasherStore(utils.NewMapChunkStore(), storage.MakeHashFunc(storage.DefaultHash), false, testtag)

	fileinfo, err := file.Stat()
	if err != nil {
		return manifestHash, nil, tag.Address, err
	}
	rootAddr, wait, err := storage.TreeSplit(sc.Ctx, file, fileinfo.Size(), putGetter)
	if err = wait(sc.Ctx); err != nil {
		return manifestHash, nil, tag.Address, err
	}
	return manifestHash, rootAddr, tag.Address, nil
}

// waitForSyncing waits for up to 5 minutes for syncing to complete after an upload.
func waitForSyncing(putter *swarmconnector.SnarlPutter, hash string) error {

	for i := 0; i < 1500; i++ {
		time.Sleep(200 * time.Millisecond)
		tag, err := putter.GetChunkTag(hash)
		if err != nil {
			return err
		}

		count, total, err := tag.Status(chunk.StateSynced)
		if verbose {
			fmt.Printf("Count: %v, Total: %v. i: %v, addr: %v\n", count, total, i, hash)
		}

		if err != nil {
			return err
		}
		if count == total {
			return nil
		}
	}
	return errors.New("Syncing timed out.")
}
