package lexval

import (
	"fmt"
	"math"
	"strconv"

	"github.com/jcalabro/atmos/lexicon"
)

func validateInteger(p *path, f *lexicon.Field, val any, errs *[]*ValidationError) {
	var n int64
	switch v := val.(type) {
	case int64:
		n = v
	case int:
		n = int64(v)
	case float64:
		if v != math.Trunc(v) || v > math.MaxInt64 || v < math.MinInt64 {
			addErr(errs, p, fmt.Sprintf("float64 %v is not a valid integer", v))
			return
		}
		n = int64(v)
	default:
		addErr(errs, p, fmt.Sprintf("expected integer, got %T", val))
		return
	}

	if f.Const != nil {
		var cv int64
		switch c := f.Const.(type) {
		case float64:
			cv = int64(c)
		case int64:
			cv = c
		case int:
			cv = int64(c)
		}
		if n != cv {
			addErr(errs, p, fmt.Sprintf("expected const %d", cv))
		}
	}

	if f.Minimum != nil && n < *f.Minimum {
		addErr(errs, p, fmt.Sprintf("value %d below minimum %d", n, *f.Minimum))
	}

	if f.Maximum != nil && n > *f.Maximum {
		addErr(errs, p, fmt.Sprintf("value %d exceeds maximum %d", n, *f.Maximum))
	}

	if len(f.Enum) > 0 {
		found := false
		for _, e := range f.Enum {
			if ev, err := strconv.ParseInt(e, 10, 64); err == nil && n == ev {
				found = true
				break
			}
		}
		if !found {
			addErr(errs, p, fmt.Sprintf("value %d not in enum", n))
		}
	}
}
