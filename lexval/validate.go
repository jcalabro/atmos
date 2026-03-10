// Package lexval validates data against ATProto Lexicon schemas.
package lexval

import (
	"github.com/jcalabro/atmos/lexicon"
)

// ValidateRecord validates a record against the schema identified by collection NSID.
// data is typically from cbor.Unmarshal or encoding/json.
func ValidateRecord(cat *lexicon.Catalog, collection string, data map[string]any) error {
	s := cat.Schema(collection)
	if s == nil {
		return &ValidationError{Path: "record", Message: "unknown schema: " + collection}
	}
	def := s.Defs["main"]
	if def == nil {
		return &ValidationError{Path: "record", Message: "schema has no main def: " + collection}
	}
	if def.Type != "record" {
		return &ValidationError{Path: "record", Message: "main def is not a record: " + def.Type}
	}
	if def.Record == nil {
		return &ValidationError{Path: "record", Message: "record def has no record object: " + collection}
	}

	// Check $type matches collection if present.
	if t, ok := data["$type"].(string); ok && t != collection {
		return &ValidationError{Path: "record.$type", Message: "expected " + collection + ", got " + t}
	}

	var errs []*ValidationError
	p := &path{seg: "record"}
	validateObjectInner(cat, s.ID, p, def.Record.Properties, def.Record.Required, def.Record.Nullable, data, &errs)
	return joinErrs(errs)
}

// ValidateValue validates a single value against a Field schema.
func ValidateValue(cat *lexicon.Catalog, contextNSID string, field *lexicon.Field, val any) error {
	var errs []*ValidationError
	p := &path{seg: "value"}
	validateField(cat, contextNSID, p, field, val, false, &errs)
	return joinErrs(errs)
}

// ValidateObject validates a map against an Object schema.
func ValidateObject(cat *lexicon.Catalog, contextNSID string, obj *lexicon.Object, data map[string]any) error {
	var errs []*ValidationError
	p := &path{seg: "object"}
	validateObjectInner(cat, contextNSID, p, obj.Properties, obj.Required, obj.Nullable, data, &errs)
	return joinErrs(errs)
}

func joinErrs(errs []*ValidationError) error {
	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	default:
		return multiError(errs)
	}
}

func validateField(cat *lexicon.Catalog, nsid string, p *path, f *lexicon.Field, val any, nullable bool, errs *[]*ValidationError) {
	if val == nil {
		if nullable {
			return
		}
		addErr(errs, p, "value is required (got null)")
		return
	}

	switch f.Type {
	case "string":
		validateString(p, f, val, errs)
	case "integer":
		validateInteger(p, f, val, errs)
	case "boolean":
		validateBoolean(p, f, val, errs)
	case "bytes":
		validateBytes(p, f, val, errs)
	case "cid-link":
		validateCIDLink(p, val, errs)
	case "blob":
		validateBlob(p, f, val, errs)
	case "array":
		validateArray(cat, nsid, p, f, val, errs)
	case "object":
		validateFieldObject(cat, nsid, p, f, val, errs)
	case "ref":
		validateRef(cat, nsid, p, f.Ref, val, errs)
	case "union":
		validateUnion(cat, nsid, p, f.Refs, f.Closed, val, errs)
	case "unknown":
		// accept anything
	default:
		addErr(errs, p, "unknown field type: "+f.Type)
	}
}
