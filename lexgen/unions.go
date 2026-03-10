package lexgen

import (
	"fmt"
	"strings"

	"github.com/jcalabro/atmos/lexicon"
)

// unionRefInfo holds resolved information about a single union ref.
type unionRefInfo struct {
	fieldName string
	goType    string
	typeID    string
}

// resolveUnionRefs resolves all refs in a union to Go types and type IDs.
func (g *fileGen) resolveUnionRefs(typeName string, refs []string) ([]unionRefInfo, error) {
	resolved := make([]unionRefInfo, 0, len(refs))
	for _, ref := range refs {
		goTyp, err := g.resolveRefType(ref)
		if err != nil {
			return nil, fmt.Errorf("union %s ref %q: %w", typeName, ref, err)
		}
		nsid, defName := lexicon.SplitRef(g.schema.ID, ref)
		typeID := nsid
		if defName != "main" {
			typeID += "#" + defName
		}
		resolved = append(resolved, unionRefInfo{
			fieldName: fieldRefShortName(g.schema.ID, ref),
			goType:    goTyp,
			typeID:    typeID,
		})
	}
	return resolved, nil
}

// genUnionType generates a union wrapper type with marshal/unmarshal methods.
func (g *fileGen) genUnionType(typeName string, refs []string, closed bool) (string, error) {
	g.imports["fmt"] = true
	g.addGTImport()
	g.addCBORImport()

	resolved, err := g.resolveUnionRefs(typeName, refs)
	if err != nil {
		return "", err
	}

	// Open unions need json.RawMessage for unknown variants.
	if !closed {
		g.imports["encoding/json"] = true
	}

	var buf strings.Builder

	// Struct definition.
	fmt.Fprintf(&buf, "// %s is a union type.\n", typeName)
	buf.WriteString("type " + typeName + " struct {\n")
	for _, r := range resolved {
		fmt.Fprintf(&buf, "\t%s gt.Ref[%s]\n", r.fieldName, r.goType)
	}
	if !closed {
		fmt.Fprintf(&buf, "\tUnknown gt.Ref[%s]\n", g.sharedType("UnknownUnionVariant"))
	}
	buf.WriteString("}\n\n")

	// MarshalJSON — thin wrapper.
	fmt.Fprintf(&buf, "func (u %s) MarshalJSON() ([]byte, error) {\n", typeName)
	buf.WriteString("\treturn u.AppendJSON(make([]byte, 0, 256))\n")
	buf.WriteString("}\n\n")

	// AppendJSON
	fmt.Fprintf(&buf, "func (u %s) AppendJSON(buf []byte) ([]byte, error) {\n", typeName)
	for _, r := range resolved {
		fmt.Fprintf(&buf, "\tif u.%s.HasVal() {\n", r.fieldName)
		fmt.Fprintf(&buf, "\t\tv := u.%s.Val()\n", r.fieldName)
		fmt.Fprintf(&buf, "\t\tv.LexiconTypeID = %q\n", r.typeID)
		buf.WriteString("\t\treturn v.AppendJSON(buf)\n")
		buf.WriteString("\t}\n")
	}
	if !closed {
		buf.WriteString("\tif u.Unknown.HasVal() {\n")
		buf.WriteString("\t\treturn append(buf, u.Unknown.Val().Raw...), nil\n")
		buf.WriteString("\t}\n")
	}
	fmt.Fprintf(&buf, "\treturn nil, fmt.Errorf(\"cannot marshal empty union %s\")\n", typeName)
	buf.WriteString("}\n\n")

	// UnmarshalJSON — thin wrapper.
	fmt.Fprintf(&buf, "func (u *%s) UnmarshalJSON(data []byte) error {\n", typeName)
	buf.WriteString("\t_, err := u.UnmarshalJSONAt(data, 0)\n")
	buf.WriteString("\treturn err\n")
	buf.WriteString("}\n\n")

	// UnmarshalJSONAt
	fmt.Fprintf(&buf, "func (u *%s) UnmarshalJSONAt(data []byte, pos int) (int, error) {\n", typeName)
	// Peek $type from the object at pos, then dispatch with UnmarshalJSONAt
	// to avoid parsing the object twice.
	buf.WriteString("\tendPos, err := cbor.SkipJSONValue(data, pos)\n")
	buf.WriteString("\tif err != nil { return 0, err }\n")
	buf.WriteString("\ttyp, err := cbor.PeekJSONType(data[pos:endPos])\n")
	buf.WriteString("\tif err != nil { return 0, err }\n")
	buf.WriteString("\tswitch typ {\n")
	for _, r := range resolved {
		fmt.Fprintf(&buf, "\tcase %q:\n", r.typeID)
		fmt.Fprintf(&buf, "\t\tvar v %s\n", r.goType)
		buf.WriteString("\t\tendPos, err = v.UnmarshalJSONAt(data, pos)\n")
		buf.WriteString("\t\tif err != nil { return 0, err }\n")
		fmt.Fprintf(&buf, "\t\tu.%s = gt.SomeRef(v)\n", r.fieldName)
		buf.WriteString("\t\treturn endPos, nil\n")
	}
	buf.WriteString("\tdefault:\n")
	if closed {
		fmt.Fprintf(&buf, "\t\treturn 0, fmt.Errorf(\"unknown type %%q in union %s\", typ)\n", typeName)
	} else {
		fmt.Fprintf(&buf, "\t\tu.Unknown = gt.SomeRef(%s{Type: typ, Raw: json.RawMessage(data[pos:endPos])})\n", g.sharedType("UnknownUnionVariant"))
		buf.WriteString("\t\treturn endPos, nil\n")
	}
	buf.WriteString("\t}\n")
	buf.WriteString("}\n\n")

	// Generate CBOR marshal/unmarshal for unions.
	cborCode, err := g.genCBORUnion(typeName, resolved, closed)
	if err != nil {
		return "", err
	}
	buf.WriteString(cborCode)

	return buf.String(), nil
}
