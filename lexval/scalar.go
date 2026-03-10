package lexval

import (
	"fmt"

	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/lexicon"
)

func validateBoolean(p *path, f *lexicon.Field, val any, errs *[]*ValidationError) {
	b, ok := val.(bool)
	if !ok {
		addErr(errs, p, fmt.Sprintf("expected boolean, got %T", val))
		return
	}
	if f.Const != nil {
		if cb, ok := f.Const.(bool); ok && b != cb {
			addErr(errs, p, fmt.Sprintf("expected const %v", cb))
		}
	}
}

func validateBytes(p *path, f *lexicon.Field, val any, errs *[]*ValidationError) {
	b, ok := val.([]byte)
	if !ok {
		addErr(errs, p, fmt.Sprintf("expected bytes, got %T", val))
		return
	}

	if f.MaxLength > 0 && len(b) > f.MaxLength {
		addErr(errs, p, fmt.Sprintf("bytes length %d exceeds maxLength %d", len(b), f.MaxLength))
	}

	if f.MinLength > 0 && len(b) < f.MinLength {
		addErr(errs, p, fmt.Sprintf("bytes length %d below minLength %d", len(b), f.MinLength))
	}
}

func validateCIDLink(p *path, val any, errs *[]*ValidationError) {
	switch v := val.(type) {
	case cbor.CID:
		if !v.Defined() {
			addErr(errs, p, "cid-link is not defined")
		}
	case *cbor.CID:
		if v == nil || !v.Defined() {
			addErr(errs, p, "cid-link is not defined")
		}
	case map[string]any:
		// JSON representation: {"$link": "bafyrei..."}
		link, ok := v["$link"]
		if !ok {
			addErr(errs, p, "cid-link map missing $link key")
			return
		}
		ls, ok := link.(string)
		if !ok {
			addErr(errs, p, fmt.Sprintf("cid-link $link expected string, got %T", link))
			return
		}
		if _, err := atmos.ParseCID(ls); err != nil {
			addErr(errs, p, fmt.Sprintf("cid-link $link invalid CID: %v", err))
		}
	default:
		addErr(errs, p, fmt.Sprintf("expected cid-link, got %T", val))
	}
}
