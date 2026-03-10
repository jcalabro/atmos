package repo

import (
	"bytes"
	"os"
	"testing"

	atmos "github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/crypto"
	"github.com/jcalabro/atmos/mst"
)

var benchRepoCAR []byte

func init() {
	var err error
	benchRepoCAR, err = os.ReadFile("testdata/repo_slice.car")
	if err != nil {
		panic(err)
	}
}

func BenchmarkLoadFromCAR(b *testing.B) {
	for b.Loop() {
		_, _, _ = LoadFromCAR(bytes.NewReader(benchRepoCAR))
	}
}

func BenchmarkCommitSign_P256(b *testing.B) {
	key, _ := crypto.GenerateP256()
	c := &Commit{
		DID:     "did:plc:testuser1234567890abcde",
		Version: 3,
		Data:    cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root")),
		Rev:     "3jqfcqzm3fo2j",
	}
	b.ResetTimer()
	for b.Loop() {
		c.Sig = nil
		_ = c.Sign(key)
	}
}

func BenchmarkCommitSign_K256(b *testing.B) {
	key, _ := crypto.GenerateK256()
	c := &Commit{
		DID:     "did:plc:testuser1234567890abcde",
		Version: 3,
		Data:    cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root")),
		Rev:     "3jqfcqzm3fo2j",
	}
	b.ResetTimer()
	for b.Loop() {
		c.Sig = nil
		_ = c.Sign(key)
	}
}

func BenchmarkCommitVerify_P256(b *testing.B) {
	key, _ := crypto.GenerateP256()
	c := &Commit{
		DID:     "did:plc:testuser1234567890abcde",
		Version: 3,
		Data:    cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root")),
		Rev:     "3jqfcqzm3fo2j",
	}
	_ = c.Sign(key)
	pub := key.PublicKey()
	b.ResetTimer()
	for b.Loop() {
		_ = c.VerifySignature(pub)
	}
}

func BenchmarkCommitVerify_K256(b *testing.B) {
	key, _ := crypto.GenerateK256()
	c := &Commit{
		DID:     "did:plc:testuser1234567890abcde",
		Version: 3,
		Data:    cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root")),
		Rev:     "3jqfcqzm3fo2j",
	}
	_ = c.Sign(key)
	pub := key.PublicKey()
	b.ResetTimer()
	for b.Loop() {
		_ = c.VerifySignature(pub)
	}
}

func BenchmarkCommitEncode(b *testing.B) {
	key, _ := crypto.GenerateP256()
	c := &Commit{
		DID:     "did:plc:testuser1234567890abcde",
		Version: 3,
		Data:    cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root")),
		Rev:     "3jqfcqzm3fo2j",
	}
	_ = c.Sign(key)
	b.ResetTimer()
	for b.Loop() {
		_, _ = encodeCommit(c)
	}
}

func BenchmarkCommitDecode(b *testing.B) {
	key, _ := crypto.GenerateP256()
	c := &Commit{
		DID:     "did:plc:testuser1234567890abcde",
		Version: 3,
		Data:    cbor.ComputeCID(cbor.CodecDagCBOR, []byte("root")),
		Rev:     "3jqfcqzm3fo2j",
	}
	_ = c.Sign(key)
	data, _ := encodeCommit(c)
	b.ResetTimer()
	for b.Loop() {
		_, _ = decodeCommit(data)
	}
}

func BenchmarkRepoCreate(b *testing.B) {
	record := map[string]any{
		"$type":     "app.bsky.feed.post",
		"text":      "hello world",
		"createdAt": "2024-01-15T12:00:00.000Z",
	}
	clock := atmos.NewTIDClock(0)
	b.ResetTimer()
	for b.Loop() {
		store := mst.NewMemBlockStore()
		r := &Repo{
			DID:   "did:plc:testuser1234567890abcde",
			Clock: clock,
			Store: store,
			Tree:  mst.NewTree(store),
		}
		_ = r.Create("app.bsky.feed.post", "3jqfcqzm3fo2j", record)
	}
}

func BenchmarkExportCAR(b *testing.B) {
	key, _ := crypto.GenerateP256()
	store := mst.NewMemBlockStore()
	r := &Repo{
		DID:   "did:plc:testuser1234567890abcde",
		Clock: atmos.NewTIDClock(0),
		Store: store,
		Tree:  mst.NewTree(store),
	}
	for i := range 50 {
		clock := atmos.NewTIDClock(uint(i))
		_ = r.Create("app.bsky.feed.post", clock.Next().String(), map[string]any{
			"$type": "app.bsky.feed.post",
			"text":  "post",
		})
	}
	var buf bytes.Buffer
	b.ResetTimer()
	for b.Loop() {
		buf.Reset()
		_ = r.ExportCAR(&buf, key)
	}
}
