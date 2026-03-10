package lexval

import (
	"fmt"

	"github.com/jcalabro/atmos/lexicon"
)

func validateArray(cat *lexicon.Catalog, nsid string, p *path, f *lexicon.Field, val any, errs *[]*ValidationError) {
	arr, ok := val.([]any)
	if !ok {
		addErr(errs, p, fmt.Sprintf("expected array, got %T", val))
		return
	}

	if f.MaxLength > 0 && len(arr) > f.MaxLength {
		addErr(errs, p, fmt.Sprintf("array length %d exceeds maxLength %d", len(arr), f.MaxLength))
	}

	if f.MinLength > 0 && len(arr) < f.MinLength {
		addErr(errs, p, fmt.Sprintf("array length %d below minLength %d", len(arr), f.MinLength))
	}

	if f.Items != nil {
		for i, elem := range arr {
			validateField(cat, nsid, p.index(i), f.Items, elem, false, errs)
		}
	}
}
