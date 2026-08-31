package lextypes

import (
	"testing"

	"github.com/jcalabro/atmos/cbor"
	"github.com/stretchr/testify/require"
)

func TestLexBlob_UnmarshalCBOR_RejectsMalformedInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "duplicate key",
			data: appendAll(
				cbor.AppendMapHeader(nil, 2),
				cbor.AppendText(nil, "x"), cbor.AppendInt(nil, 1),
				cbor.AppendText(nil, "x"), cbor.AppendInt(nil, 2),
			),
		},
		{
			name: "unsorted keys",
			data: appendAll(
				cbor.AppendMapHeader(nil, 2),
				cbor.AppendText(nil, "y"), cbor.AppendInt(nil, 1),
				cbor.AppendText(nil, "x"), cbor.AppendInt(nil, 2),
			),
		},
		{
			name: "trailing bytes",
			data: append(appendAll(
				cbor.AppendMapHeader(nil, 1),
				cbor.AppendText(nil, "x"), cbor.AppendInt(nil, 1),
			), 0x00),
		},
		{
			name: "invalid utf-8 key",
			data: appendAll(
				cbor.AppendMapHeader(nil, 1),
				[]byte{0x62, 0xff, 0xfe}, cbor.AppendInt(nil, 1),
			),
		},
		{
			name: "unknown map count overflow",
			data: appendAll(
				cbor.AppendMapHeader(nil, 1),
				cbor.AppendText(nil, "x"),
				[]byte{0xbb, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x61, 'y', 0x01},
			),
		},
		{
			name: "unknown unsupported tag",
			data: appendAll(
				cbor.AppendMapHeader(nil, 1),
				cbor.AppendText(nil, "x"), []byte{0xc1, 0x01},
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var blob LexBlob
			require.Error(t, blob.UnmarshalCBOR(tt.data))
		})
	}
}

func appendAll(parts ...[]byte) []byte {
	var out []byte
	for _, part := range parts {
		out = append(out, part...)
	}
	return out
}
