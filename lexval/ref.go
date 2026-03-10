package lexval

import (
	"github.com/jcalabro/atmos/lexicon"
)

func validateRef(cat *lexicon.Catalog, nsid string, p *path, ref string, val any, errs *[]*ValidationError) {
	targetNSID, defName := lexicon.SplitRef(nsid, ref)
	s := cat.Schema(targetNSID)
	if s == nil {
		addErr(errs, p, "unresolved ref: schema "+targetNSID+" not found")
		return
	}
	def := s.Defs[defName]
	if def == nil {
		addErr(errs, p, "unresolved ref: def "+defName+" not found in "+targetNSID)
		return
	}

	validateDef(cat, targetNSID, p, def, val, errs)
}

func validateDef(cat *lexicon.Catalog, nsid string, p *path, def *lexicon.Def, val any, errs *[]*ValidationError) {
	switch def.Type {
	case "object":
		m, ok := val.(map[string]any)
		if !ok {
			addErr(errs, p, "expected object for ref")
			return
		}
		validateObjectInner(cat, nsid, p, def.Properties, def.Required, def.Nullable, m, errs)

	case "record":
		if def.Record == nil {
			addErr(errs, p, "record def has no record object")
			return
		}
		m, ok := val.(map[string]any)
		if !ok {
			addErr(errs, p, "expected object for record ref")
			return
		}
		validateObjectInner(cat, nsid, p, def.Record.Properties, def.Record.Required, def.Record.Nullable, m, errs)

	case "string":
		// Build a temporary Field from the def's string constraints.
		f := &lexicon.Field{
			Type:         "string",
			Format:       def.Format,
			MaxLength:    def.MaxLength,
			MinLength:    def.MinLength,
			MaxGraphemes: def.MaxGraphemes,
			MinGraphemes: def.MinGraphemes,
			Enum:         def.Enum,
			Const:        def.Const,
		}
		validateString(p, f, val, errs)

	case "integer":
		f := &lexicon.Field{
			Type:    "integer",
			Minimum: def.Minimum,
			Maximum: def.Maximum,
			Enum:    def.Enum,
			Const:   def.Const,
		}
		validateInteger(p, f, val, errs)

	case "union":
		validateUnion(cat, nsid, p, def.Refs, def.Closed, val, errs)

	case "token":
		if _, ok := val.(string); !ok {
			addErr(errs, p, "expected string for token")
		}

	case "array":
		f := &lexicon.Field{
			Type:      "array",
			Items:     def.Items,
			MaxLength: def.MaxLength,
			MinLength: def.MinLength,
		}
		validateArray(cat, nsid, p, f, val, errs)

	case "boolean":
		validateBoolean(p, &lexicon.Field{Type: "boolean", Const: def.Const}, val, errs)

	case "bytes":
		f := &lexicon.Field{
			Type:      "bytes",
			MaxLength: def.MaxLength,
			MinLength: def.MinLength,
		}
		validateBytes(p, f, val, errs)

	case "cid-link":
		validateCIDLink(p, val, errs)

	case "blob":
		f := &lexicon.Field{
			Type:    "blob",
			Accept:  nil, // Def doesn't have Accept/MaxSize; blob defs are rare.
			MaxSize: 0,
		}
		validateBlob(p, f, val, errs)

	case "ref":
		validateRef(cat, nsid, p, def.Ref, val, errs)

	default:
		addErr(errs, p, "unsupported def type for ref: "+def.Type)
	}
}
