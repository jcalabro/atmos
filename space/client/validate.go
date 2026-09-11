package client

import (
	"encoding/json"
	"fmt"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/api/comatproto"
	"github.com/jcalabro/atmos/cbor"
	spaces "github.com/jcalabro/atmos/space"
	"github.com/jcalabro/gt"
)

func validateSpace(space atmos.SpaceRef) error {
	if err := space.Validate(); err != nil {
		return fmt.Errorf("space client: invalid space: %w", err)
	}
	return nil
}

func validateRecordCoordinates(space atmos.SpaceRef, collection atmos.NSID, rkey atmos.RecordKey, allowEmptyRKey bool) error {
	if err := validateSpace(space); err != nil {
		return err
	}
	if err := collection.Validate(); err != nil {
		return fmt.Errorf("space client: invalid collection: %w", err)
	}
	if rkey == "" && allowEmptyRKey {
		return nil
	}
	if err := rkey.Validate(); err != nil {
		return fmt.Errorf("space client: invalid record key: %w", err)
	}
	return nil
}

func validateRecordInput(space atmos.SpaceRef, collection atmos.NSID, rkey atmos.RecordKey, record json.RawMessage, allowEmptyRKey bool) error {
	if err := validateRecordCoordinates(space, collection, rkey, allowEmptyRKey); err != nil {
		return err
	}
	return validateRecordValue(record, collection)
}

func validationOption(mode ValidationMode) (gt.Option[bool], error) {
	switch mode {
	case ValidateKnown:
		return gt.None[bool](), nil
	case ValidateRequired:
		return gt.Some(true), nil
	case ValidateSkipped:
		return gt.Some(false), nil
	default:
		return gt.None[bool](), fmt.Errorf("space client: invalid validation mode %d", mode)
	}
}

func validateRecordValue(record json.RawMessage, collection atmos.NSID) error {
	_, err := canonicalRecordCID(record, collection)
	return err
}

func canonicalRecordCID(record json.RawMessage, collection atmos.NSID) (cbor.CID, error) {
	if len(record) == 0 {
		return cbor.CID{}, fmt.Errorf("space client: record value is required")
	}
	decoded, err := cbor.FromJSON(record)
	if err != nil {
		return cbor.CID{}, fmt.Errorf("space client: record is not valid AT Protocol data: %w", err)
	}
	object, ok := decoded.(map[string]any)
	if !ok {
		return cbor.CID{}, fmt.Errorf("space client: record must be a JSON object")
	}
	typeValue, ok := object["$type"].(string)
	if !ok || typeValue != string(collection) {
		return cbor.CID{}, fmt.Errorf("space client: record $type %q does not match collection %q", typeValue, collection)
	}
	canonical, err := cbor.Marshal(decoded)
	if err != nil {
		return cbor.CID{}, fmt.Errorf("space client: encode canonical record: %w", err)
	}
	return cbor.ComputeCID(cbor.CodecDagCBOR, canonical), nil
}

func validateRecordCID(record json.RawMessage, collection atmos.NSID, rawCID string) error {
	expected, err := parseDAGCBORCID(rawCID)
	if err != nil {
		return err
	}
	actual, err := canonicalRecordCID(record, collection)
	if err != nil {
		return err
	}
	if !actual.Equal(expected) {
		return fmt.Errorf("space client: record value CID mismatch: expected %s, got %s", expected, actual)
	}
	return nil
}

func validateRecordOutput(rawURI, rawCID string, expectedSpace atmos.SpaceRef, expectedAuthor atmos.DID, expectedCollection atmos.NSID, expectedRKey atmos.RecordKey) error {
	uri, err := atmos.ParseSpaceURI(rawURI)
	if err != nil {
		return fmt.Errorf("space client: invalid record URI response: %w", err)
	}
	if uri.Space() != expectedSpace || uri.Author() != expectedAuthor || uri.Collection() != expectedCollection || uri.RecordKey() != expectedRKey {
		return fmt.Errorf("space client: record response coordinates do not match request")
	}
	if err := validateDAGCBORCID(rawCID); err != nil {
		return fmt.Errorf("space client: invalid record CID response: %w", err)
	}
	return nil
}

func validateWriteOutput(rawURI, rawCID string, status gt.Option[string], space atmos.SpaceRef, author atmos.DID, collection atmos.NSID, requestedRKey atmos.RecordKey) error {
	uri, err := atmos.ParseSpaceURI(rawURI)
	if err != nil {
		return fmt.Errorf("space client: invalid write URI response: %w", err)
	}
	if uri.Space() != space || uri.Author() != author || uri.Collection() != collection || (requestedRKey != "" && uri.RecordKey() != requestedRKey) {
		return fmt.Errorf("space client: write response coordinates do not match request")
	}
	if err := validateDAGCBORCID(rawCID); err != nil {
		return fmt.Errorf("space client: invalid write CID response: %w", err)
	}
	if status.HasVal() && status.Val() != "valid" && status.Val() != "unknown" {
		return fmt.Errorf("space client: invalid validation status %q", status.Val())
	}
	return nil
}

func validateListedRecords(records []comatproto.SpaceListRecords_Record, expectedCollection atmos.NSID, excludeValues bool) error {
	for i := range records {
		collection := atmos.NSID(records[i].Collection)
		if err := collection.Validate(); err != nil {
			return fmt.Errorf("space client: invalid records[%d].collection: %w", i, err)
		}
		if expectedCollection != "" && collection != expectedCollection {
			return fmt.Errorf("space client: records[%d].collection does not match requested filter", i)
		}
		if err := atmos.RecordKey(records[i].Rkey).Validate(); err != nil {
			return fmt.Errorf("space client: invalid records[%d].rkey: %w", i, err)
		}
		if err := validateDAGCBORCID(records[i].CID); err != nil {
			return fmt.Errorf("space client: invalid records[%d].cid: %w", i, err)
		}
		if excludeValues {
			if len(records[i].Value) != 0 {
				return fmt.Errorf("space client: records[%d].value was present despite excludeValues", i)
			}
		} else if err := validateRecordCID(records[i].Value, collection, records[i].CID); err != nil {
			return fmt.Errorf("space client: invalid records[%d].value: %w", i, err)
		}
	}
	return nil
}

func validateRepoOps(out *comatproto.SpaceListRepoOps_Output, excludeValues bool) error {
	for i := range out.Ops {
		op := &out.Ops[i]
		if err := atmos.NSID(op.Collection).Validate(); err != nil {
			return fmt.Errorf("space client: invalid ops[%d].collection: %w", i, err)
		}
		if err := atmos.RecordKey(op.Rkey).Validate(); err != nil {
			return fmt.Errorf("space client: invalid ops[%d].rkey: %w", i, err)
		}
		if err := atmos.TID(op.Rev).Validate(); err != nil {
			return fmt.Errorf("space client: invalid ops[%d].rev: %w", i, err)
		}
		for name, value := range map[string]gt.Option[string]{"cid": op.CID, "prev": op.Prev} {
			if value.HasVal() {
				if err := validateDAGCBORCID(value.Val()); err != nil {
					return fmt.Errorf("space client: invalid ops[%d].%s: %w", i, name, err)
				}
			}
		}
		if !op.CID.HasVal() && !op.Prev.HasVal() {
			return fmt.Errorf("space client: ops[%d] has neither current nor previous CID", i)
		}
		if !op.CID.HasVal() && len(op.Value) != 0 {
			return fmt.Errorf("space client: delete ops[%d] must not carry a value", i)
		}
		if excludeValues && len(op.Value) != 0 {
			return fmt.Errorf("space client: ops[%d].value was present despite excludeValues", i)
		}
		if len(op.Value) != 0 {
			if err := validateRecordCID(op.Value, atmos.NSID(op.Collection), op.CID.Val()); err != nil {
				return fmt.Errorf("space client: invalid ops[%d].value: %w", i, err)
			}
		}
	}
	if out.Commit.HasVal() {
		if _, err := rawCommit(out.Commit.Val()).Validate(); err != nil {
			return fmt.Errorf("space client: invalid terminal commit: %w", err)
		}
	}
	return nil
}

func validateDAGCBORCID(raw string) error {
	_, err := parseDAGCBORCID(raw)
	return err
}

func parseDAGCBORCID(raw string) (cbor.CID, error) {
	cid, err := cbor.ParseCIDString(raw)
	if err != nil {
		return cbor.CID{}, err
	}
	if cid.Codec() != cbor.CodecDagCBOR {
		return cbor.CID{}, fmt.Errorf("CID codec %d is not DAG-CBOR", cid.Codec())
	}
	return cid, nil
}

func convertWrite(space atmos.SpaceRef, write Write) (comatproto.SpaceApplyWrites_Input_Writes, error) {
	allowEmpty := write.Action == WriteCreate
	if err := validateRecordCoordinates(space, write.Collection, write.RKey, allowEmpty); err != nil {
		return comatproto.SpaceApplyWrites_Input_Writes{}, err
	}
	switch write.Action {
	case WriteCreate:
		if err := validateRecordInput(space, write.Collection, write.RKey, write.Value, true); err != nil {
			return comatproto.SpaceApplyWrites_Input_Writes{}, err
		}
		value := comatproto.SpaceApplyWrites_Create{Collection: string(write.Collection), Value: write.Value}
		if write.RKey != "" {
			value.Rkey = gt.Some(string(write.RKey))
		}
		return comatproto.SpaceApplyWrites_Input_Writes{SpaceApplyWrites_Create: gt.SomeRef(value)}, nil
	case WriteUpdate:
		if err := validateRecordInput(space, write.Collection, write.RKey, write.Value, false); err != nil {
			return comatproto.SpaceApplyWrites_Input_Writes{}, err
		}
		value := comatproto.SpaceApplyWrites_Update{Collection: string(write.Collection), Rkey: string(write.RKey), Value: write.Value}
		return comatproto.SpaceApplyWrites_Input_Writes{SpaceApplyWrites_Update: gt.SomeRef(value)}, nil
	case WriteDelete:
		if len(write.Value) != 0 {
			return comatproto.SpaceApplyWrites_Input_Writes{}, fmt.Errorf("delete operation must not carry a value")
		}
		value := comatproto.SpaceApplyWrites_Delete{Collection: string(write.Collection), Rkey: string(write.RKey)}
		return comatproto.SpaceApplyWrites_Input_Writes{SpaceApplyWrites_Delete: gt.SomeRef(value)}, nil
	default:
		return comatproto.SpaceApplyWrites_Input_Writes{}, fmt.Errorf("invalid write action %d", write.Action)
	}
}

func validateBatchResults(results []comatproto.SpaceApplyWrites_Output_Results, writes []Write, space atmos.SpaceRef, author atmos.DID) error {
	for i := range results {
		result := results[i]
		switch writes[i].Action {
		case WriteCreate:
			if !result.SpaceApplyWrites_CreateResult.HasVal() {
				return fmt.Errorf("space client: batch result %d does not match create operation", i)
			}
			value := result.SpaceApplyWrites_CreateResult.Val()
			if err := validateWriteOutput(value.URI, value.CID, value.ValidationStatus, space, author, writes[i].Collection, writes[i].RKey); err != nil {
				return fmt.Errorf("space client: invalid batch create result %d: %w", i, err)
			}
			if err := validateRecordCID(writes[i].Value, writes[i].Collection, value.CID); err != nil {
				return fmt.Errorf("space client: invalid batch create result %d: %w", i, err)
			}
		case WriteUpdate:
			if !result.SpaceApplyWrites_UpdateResult.HasVal() {
				return fmt.Errorf("space client: batch result %d does not match update operation", i)
			}
			value := result.SpaceApplyWrites_UpdateResult.Val()
			if err := validateWriteOutput(value.URI, value.CID, value.ValidationStatus, space, author, writes[i].Collection, writes[i].RKey); err != nil {
				return fmt.Errorf("space client: invalid batch update result %d: %w", i, err)
			}
			if err := validateRecordCID(writes[i].Value, writes[i].Collection, value.CID); err != nil {
				return fmt.Errorf("space client: invalid batch update result %d: %w", i, err)
			}
		case WriteDelete:
			if !result.SpaceApplyWrites_DeleteResult.HasVal() {
				return fmt.Errorf("space client: batch result %d does not match delete operation", i)
			}
		}
	}
	return nil
}

var _ = spaces.CommitVersion
