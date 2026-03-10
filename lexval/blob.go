package lexval

import (
	"fmt"
	"strings"

	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/lexicon"
)

func validateBlob(p *path, f *lexicon.Field, val any, errs *[]*ValidationError) {
	m, ok := val.(map[string]any)
	if !ok {
		addErr(errs, p, fmt.Sprintf("expected blob object, got %T", val))
		return
	}

	t, _ := m["$type"].(string)
	if t != "blob" {
		addErr(errs, p, "blob missing or wrong $type")
	}

	ref, hasRef := m["ref"]
	if !hasRef {
		addErr(errs, p, "blob missing ref")
	} else {
		switch r := ref.(type) {
		case map[string]any:
			if _, ok := r["$link"].(string); !ok {
				addErr(errs, p, "blob ref missing $link string")
			}
		case cbor.CID:
			if !r.Defined() {
				addErr(errs, p, "blob ref CID is not defined")
			}
		case *cbor.CID:
			if r == nil || !r.Defined() {
				addErr(errs, p, "blob ref CID is not defined")
			}
		default:
			addErr(errs, p, fmt.Sprintf("blob ref expected object or CID, got %T", ref))
		}
	}

	mimeType, hasMime := m["mimeType"]
	if !hasMime {
		addErr(errs, p, "blob missing mimeType")
	} else {
		mt, ok := mimeType.(string)
		if !ok {
			addErr(errs, p, fmt.Sprintf("blob mimeType expected string, got %T", mimeType))
		} else if len(f.Accept) > 0 {
			if !matchMIME(f.Accept, mt) {
				addErr(errs, p, fmt.Sprintf("blob mimeType %q not accepted", mt))
			}
		}
	}

	sizeVal, hasSize := m["size"]
	if !hasSize {
		addErr(errs, p, "blob missing size")
	} else {
		var size int64
		switch sv := sizeVal.(type) {
		case float64:
			size = int64(sv)
		case int64:
			size = sv
		case int:
			size = int64(sv)
		default:
			addErr(errs, p, fmt.Sprintf("blob size expected number, got %T", sizeVal))
			return
		}
		if f.MaxSize > 0 && size > f.MaxSize {
			addErr(errs, p, fmt.Sprintf("blob size %d exceeds maxSize %d", size, f.MaxSize))
		}
	}
}

// matchMIME checks if mimeType matches any of the accept patterns.
// Patterns like "image/*" match any "image/" prefix.
func matchMIME(accept []string, mimeType string) bool {
	for _, pattern := range accept {
		if pattern == "*/*" {
			return true
		}
		if strings.HasSuffix(pattern, "/*") {
			prefix := pattern[:len(pattern)-1] // "image/*" → "image/"
			if strings.HasPrefix(mimeType, prefix) {
				return true
			}
		} else if pattern == mimeType {
			return true
		}
	}
	return false
}
