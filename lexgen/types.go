package lexgen

import (
	"fmt"
	"strings"

	"github.com/jcalabro/atmos/lexicon"
)

// goType returns the Go type string for a lexicon field.
// parentType is the enclosing type name (for generating nested union types).
// fieldName is the field name (for the same purpose).
// required indicates whether the field is required (affects optionality).
// nullable indicates whether the field is nullable.
func (g *fileGen) goType(f *lexicon.Field, parentType, fieldName string, required, nullable bool) (string, error) {
	optional := !required || nullable

	switch f.Type {
	case "string":
		if !optional {
			return "string", nil
		}
		g.addGTImport()
		return "gt.Option[string]", nil

	case "integer":
		if !optional {
			return "int64", nil
		}
		g.addGTImport()
		return "gt.Option[int64]", nil

	case "boolean":
		if !optional {
			return "bool", nil
		}
		g.addGTImport()
		return "gt.Option[bool]", nil

	case "bytes":
		return "[]byte", nil

	case "cid-link":
		name := g.sharedType("LexCIDLink")
		if !optional {
			return name, nil
		}
		g.addGTImport()
		return "gt.Option[" + name + "]", nil

	case "blob":
		name := g.sharedType("LexBlob")
		if !optional {
			return name, nil
		}
		g.addGTImport()
		return "gt.Option[" + name + "]", nil

	case "unknown":
		g.imports["encoding/json"] = true
		return "json.RawMessage", nil

	case "null":
		g.imports["encoding/json"] = true
		return "json.RawMessage", nil

	case "array":
		if f.Items == nil {
			return "[]any", nil
		}
		// Array elements are always required (they exist in the array).
		elemType, err := g.goType(f.Items, parentType, fieldName, true, false)
		if err != nil {
			return "", err
		}
		return "[]" + elemType, nil

	case "object":
		typeName := parentType + "_" + capitalize(fieldName)
		if !optional {
			return typeName, nil
		}
		g.addGTImport()
		return "gt.Option[" + typeName + "]", nil

	case "ref":
		typeName, err := g.resolveRefType(f.Ref)
		if err != nil {
			return "", err
		}
		if !optional {
			return typeName, nil
		}
		g.addGTImport()
		return "gt.Option[" + typeName + "]", nil

	case "union":
		typeName := parentType + "_" + capitalize(fieldName)
		if !optional {
			return typeName, nil
		}
		g.addGTImport()
		return "gt.Option[" + typeName + "]", nil

	default:
		return "", fmt.Errorf("unsupported lexicon type %q", f.Type)
	}
}

func (g *fileGen) addGTImport() {
	g.imports["\"github.com/jcalabro/gt\""] = true
}

// resolveRefType returns the Go type name for a ref string.
func (g *fileGen) resolveRefType(ref string) (string, error) {
	nsid, defName := lexicon.SplitRef(g.schema.ID, ref)

	targetPkg := findPackage(g.cfg, nsid)
	if targetPkg == nil {
		return "", fmt.Errorf("no package configured for ref %q (NSID %q)", ref, nsid)
	}

	typeName := TypeName(nsid, defName)

	// Cross-package reference needs package qualifier.
	if targetPkg.Package != g.pkg.Package {
		alias := targetPkg.Package
		g.imports[alias+" \""+targetPkg.Import+"\""] = true
		return alias + "." + typeName, nil
	}

	return typeName, nil
}

// paramGoType returns the Go type for a query parameter.
// Query params are always passed as function arguments with zero-value
// checks for optional ones, so they use plain types (not Option).
func paramGoType(f *lexicon.Field) string {
	switch f.Type {
	case "string":
		return "string"
	case "integer":
		return "int64"
	case "boolean":
		return "bool"
	case "array":
		if f.Items != nil {
			switch f.Items.Type {
			case "integer":
				return "[]int64"
			case "boolean":
				return "[]bool"
			}
		}
		return "[]string"
	default:
		return "string"
	}
}

// fieldRefShortName returns a short name for a union field from a ref.
// Used as the struct field name in union wrapper types.
func fieldRefShortName(currentNSID, ref string) string {
	nsid, defName := lexicon.SplitRef(currentNSID, ref)
	parts := strings.Split(nsid, ".")
	var base string
	if len(parts) >= 2 {
		base = capitalize(parts[len(parts)-2]) + capitalize(parts[len(parts)-1])
	} else {
		base = capitalize(nsid)
	}
	if defName == "main" {
		return base
	}
	return base + "_" + capitalize(defName)
}
