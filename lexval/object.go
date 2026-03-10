package lexval

import (
	"fmt"

	"github.com/jcalabro/atmos/lexicon"
)

func validateFieldObject(cat *lexicon.Catalog, nsid string, p *path, f *lexicon.Field, val any, errs *[]*ValidationError) {
	m, ok := val.(map[string]any)
	if !ok {
		addErr(errs, p, fmt.Sprintf("expected object, got %T", val))
		return
	}
	validateObjectInner(cat, nsid, p, f.Properties, f.Required, f.Nullable, m, errs)
}

func validateObjectInner(cat *lexicon.Catalog, nsid string, p *path, props map[string]*lexicon.Field, required, nullable []string, data map[string]any, errs *[]*ValidationError) {
	reqSet := make(map[string]bool, len(required))
	for _, r := range required {
		reqSet[r] = true
	}
	nullSet := make(map[string]bool, len(nullable))
	for _, n := range nullable {
		nullSet[n] = true
	}

	// Check required fields exist.
	for _, r := range required {
		v, exists := data[r]
		if !exists {
			addErr(errs, p.field(r), "required field missing")
			continue
		}
		if v == nil && !nullSet[r] {
			addErr(errs, p.field(r), "required field is null")
		}
	}

	// Validate each property that has a schema.
	for name, field := range props {
		v, exists := data[name]
		if !exists {
			continue
		}
		validateField(cat, nsid, p.field(name), field, v, nullSet[name], errs)
	}

	// Unknown keys: silently accepted (forward compatibility).
}
