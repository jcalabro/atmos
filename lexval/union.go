package lexval

import (
	"fmt"

	"github.com/jcalabro/atmos/lexicon"
)

func validateUnion(cat *lexicon.Catalog, nsid string, p *path, refs []string, closed bool, val any, errs *[]*ValidationError) {
	m, ok := val.(map[string]any)
	if !ok {
		addErr(errs, p, fmt.Sprintf("expected union object, got %T", val))
		return
	}

	typeVal, ok := m["$type"]
	if !ok {
		addErr(errs, p, "union missing $type")
		return
	}
	typeName, ok := typeVal.(string)
	if !ok {
		addErr(errs, p, fmt.Sprintf("union $type expected string, got %T", typeVal))
		return
	}

	// Find matching ref.
	for _, ref := range refs {
		targetNSID, defName := lexicon.SplitRef(nsid, ref)

		// Match: $type is the full NSID for main defs, or NSID#defName for non-main.
		var fullRef string
		if defName == "main" {
			fullRef = targetNSID
		} else {
			fullRef = targetNSID + "#" + defName
		}

		if typeName == fullRef {
			s := cat.Schema(targetNSID)
			if s == nil {
				addErr(errs, p, "unresolved union ref: schema "+targetNSID+" not found")
				return
			}
			def := s.Defs[defName]
			if def == nil {
				addErr(errs, p, "unresolved union ref: def "+defName+" not found in "+targetNSID)
				return
			}
			validateDef(cat, targetNSID, p, def, val, errs)
			return
		}
	}

	// No match.
	if closed {
		addErr(errs, p, fmt.Sprintf("union $type %q not in closed union", typeName))
	}
	// Open union: accept unknown types.
}
