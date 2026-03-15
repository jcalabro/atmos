package lexval

import (
	"fmt"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/lexicon"
	"github.com/rivo/uniseg"
)

func validateString(p *path, f *lexicon.Field, val any, errs *[]*ValidationError) {
	s, ok := val.(string)
	if !ok {
		addErr(errs, p, fmt.Sprintf("expected string, got %T", val))
		return
	}

	if f.Const != nil {
		if cs, ok := f.Const.(string); ok && s != cs {
			addErr(errs, p, fmt.Sprintf("expected const %q", cs))
		}
	}

	if len(f.Enum) > 0 {
		found := false
		for _, e := range f.Enum {
			if s == e {
				found = true
				break
			}
		}
		if !found {
			addErr(errs, p, fmt.Sprintf("value %q not in enum", s))
		}
	}

	if f.MaxLength > 0 && len(s) > f.MaxLength {
		addErr(errs, p, fmt.Sprintf("string length %d exceeds maxLength %d", len(s), f.MaxLength))
	}

	if f.MinLength > 0 && len(s) < f.MinLength {
		addErr(errs, p, fmt.Sprintf("string length %d below minLength %d", len(s), f.MinLength))
	}

	if f.MaxGraphemes > 0 || f.MinGraphemes > 0 {
		gc := uniseg.GraphemeClusterCount(s)
		if f.MaxGraphemes > 0 && gc > f.MaxGraphemes {
			addErr(errs, p, fmt.Sprintf("grapheme count %d exceeds maxGraphemes %d", gc, f.MaxGraphemes))
		}
		if f.MinGraphemes > 0 && gc < f.MinGraphemes {
			addErr(errs, p, fmt.Sprintf("grapheme count %d below minGraphemes %d", gc, f.MinGraphemes))
		}
	}

	if f.Format != "" {
		validateStringFormat(p, f.Format, s, errs)
	}
}

func validateStringFormat(p *path, format, s string, errs *[]*ValidationError) {
	var err error
	switch format {
	case "did":
		_, err = atmos.ParseDID(s)
	case "handle":
		_, err = atmos.ParseHandle(s)
	case "at-uri":
		_, err = atmos.ParseATURI(s)
	case "at-identifier":
		_, err = atmos.ParseATIdentifier(s)
	case "nsid":
		_, err = atmos.ParseNSID(s)
	case "cid":
		_, err = cbor.ParseCIDString(s)
	case "datetime":
		_, err = atmos.ParseDatetime(s)
	case "tid":
		_, err = atmos.ParseTID(s)
	case "record-key":
		_, err = atmos.ParseRecordKey(s)
	case "uri":
		_, err = atmos.ParseURI(s)
	case "language":
		_, err = atmos.ParseLanguage(s)
	default:
		// Unknown format: skip for forward compatibility.
		return
	}
	if err != nil {
		addErr(errs, p, fmt.Sprintf("invalid %s format: %v", format, err))
	}
}
