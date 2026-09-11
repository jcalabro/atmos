package client

import (
	"encoding/json"
	"testing"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/gt"
	"github.com/stretchr/testify/require"
)

func TestValidateListedRecordsEnforcesValuesFilterAndCodec(t *testing.T) {
	t.Parallel()
	record := `{"$type":"com.example.post","text":"hello"}`
	dagCID := mustRecordCID(t, record)
	rawCID := cbor.ComputeCID(cbor.CodecRaw, []byte("record")).String()
	valid := comatproto.SpaceListRecords_Record{
		Collection: "com.example.post", Rkey: "one", CID: dagCID,
		Value: json.RawMessage(record),
	}
	require.NoError(t, validateListedRecords([]comatproto.SpaceListRecords_Record{valid}, "com.example.post", false))

	tests := []struct {
		name    string
		record  comatproto.SpaceListRecords_Record
		filter  atmos.NSID
		exclude bool
	}{
		{name: "wrong filter", record: valid, filter: "com.example.other"},
		{name: "missing included value", record: comatproto.SpaceListRecords_Record{Collection: valid.Collection, Rkey: valid.Rkey, CID: dagCID}},
		{name: "value despite exclusion", record: valid, filter: "com.example.post", exclude: true},
		{name: "wrong CID codec", record: comatproto.SpaceListRecords_Record{Collection: valid.Collection, Rkey: valid.Rkey, CID: rawCID, Value: valid.Value}},
		{name: "CID value mismatch", record: comatproto.SpaceListRecords_Record{Collection: valid.Collection, Rkey: valid.Rkey, CID: dagCID, Value: json.RawMessage(`{"$type":"com.example.post","text":"different"}`)}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Error(t, validateListedRecords([]comatproto.SpaceListRecords_Record{test.record}, test.filter, test.exclude))
		})
	}
}

func TestValidateRepoOpsEnforcesOperationShapeAndValues(t *testing.T) {
	t.Parallel()
	record := `{"$type":"com.example.post"}`
	dagCID := mustRecordCID(t, record)
	base := comatproto.SpaceListRepoOps_OpEntry{
		Collection: "com.example.post", Rkey: "one", Rev: "3jzfcijpj2z2a",
		CID: gt.Some(dagCID), Value: json.RawMessage(record),
	}
	require.NoError(t, validateRepoOps(&comatproto.SpaceListRepoOps_Output{Ops: []comatproto.SpaceListRepoOps_OpEntry{base}}, false))

	t.Run("neither CID", func(t *testing.T) {
		op := base
		op.CID = gt.None[string]()
		op.Value = nil
		require.Error(t, validateRepoOps(&comatproto.SpaceListRepoOps_Output{Ops: []comatproto.SpaceListRepoOps_OpEntry{op}}, false))
	})
	t.Run("delete value", func(t *testing.T) {
		op := base
		op.CID = gt.None[string]()
		op.Prev = gt.Some(dagCID)
		require.Error(t, validateRepoOps(&comatproto.SpaceListRepoOps_Output{Ops: []comatproto.SpaceListRepoOps_OpEntry{op}}, false))
	})
	t.Run("excluded value", func(t *testing.T) {
		require.Error(t, validateRepoOps(&comatproto.SpaceListRepoOps_Output{Ops: []comatproto.SpaceListRepoOps_OpEntry{base}}, true))
	})
	t.Run("wrong value type", func(t *testing.T) {
		op := base
		op.Value = json.RawMessage(`{"$type":"com.example.other"}`)
		require.Error(t, validateRepoOps(&comatproto.SpaceListRepoOps_Output{Ops: []comatproto.SpaceListRepoOps_OpEntry{op}}, false))
	})
	t.Run("CID value mismatch", func(t *testing.T) {
		op := base
		op.Value = json.RawMessage(`{"$type":"com.example.post","changed":true}`)
		require.ErrorContains(t, validateRepoOps(&comatproto.SpaceListRepoOps_Output{Ops: []comatproto.SpaceListRepoOps_OpEntry{op}}, false), "CID mismatch")
	})
}

func mustRecordCID(t testing.TB, record string) string {
	t.Helper()
	value, err := cbor.FromJSON([]byte(record))
	require.NoError(t, err)
	encoded, err := cbor.Marshal(value)
	require.NoError(t, err)
	return cbor.ComputeCID(cbor.CodecDagCBOR, encoded).String()
}
