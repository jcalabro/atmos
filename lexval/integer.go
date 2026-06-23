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
		// math.MaxInt64 as a float64 rounds UP to 2^63, so "v > math.MaxInt64"
		// is false for v == 2^63 and int64(v) would then wrap to math.MinInt64
		// (silent corruption). Compare against the exact float64 powers of two
		// that bound the representable int64 range: a valid integer must satisfy
		// -2^63 <= v < 2^63.
		const twoPow63 = 9223372036854775808.0 // 2^63, exactly representable as float64
		if v != math.Trunc(v) || v >= twoPow63 || v < -twoPow63 {
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
