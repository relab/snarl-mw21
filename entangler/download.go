package entangler

type DownloadRequest struct {
	Result chan *Block
	Block  *Block
}

type DownloadResponse struct {
	*DownloadRequest
	Value []byte
}
