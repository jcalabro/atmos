package xrpc

import (
	"context"
	"io"
)

// BlobRef describes an uploaded blob.
type BlobRef struct {
	Ref      CIDLink `json:"ref"`
	MimeType string  `json:"mimeType"`
	Size     int64   `json:"size"`
}

// CIDLink is a CID reference using the $link JSON convention.
type CIDLink struct {
	Link string `json:"$link"`
}

// uploadBlobResp wraps the server response for uploadBlob.
type uploadBlobResp struct {
	Blob BlobRef `json:"blob"`
}

// UploadBlob uploads a blob to the server.
func (c *Client) UploadBlob(ctx context.Context, contentType string, r io.Reader) (*BlobRef, error) {
	var out uploadBlobResp
	err := c.Do(ctx, "POST", "com.atproto.repo.uploadBlob", contentType, nil, r, &out)
	if err != nil {
		return nil, err
	}
	return &out.Blob, nil
}
