package lexgen

import (
	"fmt"
	"sort"
	"strings"

	"github.com/jcalabro/atmos/lexicon"
)

// genJSONMethods generates MarshalJSON/AppendJSON and UnmarshalJSON/UnmarshalJSONAt methods.
func (g *fileGen) genJSONMethods(typeName string, obj *lexicon.Object, isRecord bool) (string, error) {
	g.addCBORImport() // JSON helpers live in the cbor package.

	requiredSet := makeSet(obj.Required)
	nullableSet := makeSet(obj.Nullable)
	fieldNames := sortedKeys(obj.Properties)

	// Build field info list.
	// $type is always optional in JSON (has omitempty tag) even for records.
	// This differs from CBOR where records always emit $type.
	var fields []fieldInfo
	fields = append(fields, fieldInfo{
		jsonName:  "$type",
		goField:   "LexiconTypeID",
		goType:    "string",
		fieldType: "string",
		required:  false,
	})
	for _, fname := range fieldNames {
		f := obj.Properties[fname]
		required := requiredSet[fname]
		nullable := nullableSet[fname]
		goTyp, err := g.goType(f, typeName, fname, required, nullable)
		if err != nil {
			return "", err
		}
		fields = append(fields, fieldInfo{
			jsonName:  fname,
			goField:   exportFieldName(fname),
			goType:    goTyp,
			fieldType: f.Type,
			required:  required,
			nullable:  nullable,
			field:     f,
		})
	}

	// Sort fields alphabetically by JSON key for deterministic output.
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].jsonName < fields[j].jsonName
	})

	var buf strings.Builder

	// Precomputed key tokens: `"keyName":` as byte slices.
	buf.WriteString("// Precomputed JSON key tokens for " + typeName + ".\n")
	buf.WriteString("var (\n")
	for _, f := range fields {
		fmt.Fprintf(&buf, "\tjsonKey_%s_%s = []byte(%q)\n",
			typeName, sanitizeCBORKeyName(f.jsonName), `"`+f.jsonName+`":`)
	}
	buf.WriteString(")\n\n")

	// MarshalJSON + AppendJSON
	g.genMarshalJSON(&buf, typeName, fields)
	buf.WriteString("\n\n")

	// UnmarshalJSON + UnmarshalJSONAt
	g.genUnmarshalJSON(&buf, typeName, fields)

	return buf.String(), nil
}

func (g *fileGen) genMarshalJSON(buf *strings.Builder, typeName string, fields []fieldInfo) {
	// MarshalJSON thin wrapper.
	fmt.Fprintf(buf, "func (s *%s) MarshalJSON() ([]byte, error) {\n", typeName)
	buf.WriteString("\treturn s.AppendJSON(make([]byte, 0, 256))\n")
	buf.WriteString("}\n\n")

	// AppendJSON
	fmt.Fprintf(buf, "func (s *%s) AppendJSON(buf []byte) ([]byte, error) {\n", typeName)
	buf.WriteString("\tbuf = append(buf, '{')\n")
	buf.WriteString("\tfirst := true\n")

	for _, f := range fields {
		keyVar := fmt.Sprintf("jsonKey_%s_%s", typeName, sanitizeCBORKeyName(f.jsonName))
		optional := !f.required || f.nullable

		if optional {
			fmt.Fprintf(buf, "\tif %s {\n", g.cborHasValue(f))
			buf.WriteString("\t\tif !first { buf = append(buf, ',') }\n")
			fmt.Fprintf(buf, "\t\tbuf = append(buf, %s...)\n", keyVar)
			g.genJSONEncodeField(buf, f, "\t\t")
			buf.WriteString("\t\tfirst = false\n")
			buf.WriteString("\t}\n")
		} else {
			buf.WriteString("\tif !first { buf = append(buf, ',') }\n")
			fmt.Fprintf(buf, "\tbuf = append(buf, %s...)\n", keyVar)
			g.genJSONEncodeField(buf, f, "\t")
			buf.WriteString("\tfirst = false\n")
		}
	}

	// Emit unknown fields preserved from a prior JSON unmarshal, maintaining
	// their original order. These are appended after known fields since JSON
	// object key order is not semantically significant.
	buf.WriteString("\tfor _, ef := range s.extra {\n")
	buf.WriteString("\t\tif ef.Encoding != extraEncodingJSON { continue }\n")
	buf.WriteString("\t\tif !first { buf = append(buf, ',') }\n")
	buf.WriteString("\t\tbuf = cbor.AppendJSONString(buf, ef.Key)\n")
	buf.WriteString("\t\tbuf = append(buf, ':')\n")
	buf.WriteString("\t\tbuf = append(buf, ef.Value...)\n")
	buf.WriteString("\t\tfirst = false\n")
	buf.WriteString("\t}\n")
	buf.WriteString("\tbuf = append(buf, '}')\n")
	buf.WriteString("\treturn buf, nil\n")
	buf.WriteString("}")
}

func (g *fileGen) genJSONEncodeField(buf *strings.Builder, f fieldInfo, indent string) {
	accessor := "s." + f.goField

	// Unwrap Option/Ref types.
	isOption := strings.HasPrefix(f.goType, "gt.Option[")
	isRef := strings.HasPrefix(f.goType, "gt.Ref[")
	if isOption || isRef {
		if needsMarshalCBOR(g.resolveFieldType(f)) {
			fmt.Fprintf(buf, "%s{\n", indent)
			fmt.Fprintf(buf, "%s\tv := s.%s.Val()\n", indent, f.goField)
			accessor = "v"
			defer func() { fmt.Fprintf(buf, "%s}\n", indent) }()
			indent += "\t"
		} else {
			accessor = "s." + f.goField + ".Val()"
		}
	}

	// Handle nullable required fields.
	if f.required && f.nullable {
		if isOption || isRef {
			fmt.Fprintf(buf, "%sif !s.%s.HasVal() {\n", indent, f.goField)
			fmt.Fprintf(buf, "%s\tbuf = cbor.AppendJSONNull(buf)\n", indent)
			fmt.Fprintf(buf, "%s} else {\n", indent)
			g.genJSONEncodeValue(buf, f, accessor, indent+"\t")
			fmt.Fprintf(buf, "%s}\n", indent)
			return
		}
	}

	g.genJSONEncodeValue(buf, f, accessor, indent)
}

func (g *fileGen) genJSONEncodeValue(buf *strings.Builder, f fieldInfo, accessor, indent string) {
	ft := g.resolveFieldType(f)
	switch ft {
	case "string":
		fmt.Fprintf(buf, "%sbuf = cbor.AppendJSONString(buf, %s)\n", indent, accessor)
	case "integer":
		fmt.Fprintf(buf, "%sbuf = cbor.AppendJSONInt(buf, %s)\n", indent, accessor)
	case "boolean":
		fmt.Fprintf(buf, "%sbuf = cbor.AppendJSONBool(buf, %s)\n", indent, accessor)
	case "bytes":
		fmt.Fprintf(buf, "%sbuf = cbor.AppendJSONBytes(buf, %s)\n", indent, accessor)
	case "cid-link", "blob", "object", "ref", "union":
		if g.isRefToArrayDef(f) {
			// Ref to an array def — elements have their own AppendJSON methods.
			fmt.Fprintf(buf, "%sbuf = append(buf, '[')\n", indent)
			fmt.Fprintf(buf, "%sfor i := range %s {\n", indent, accessor)
			fmt.Fprintf(buf, "%s\tif i > 0 { buf = append(buf, ',') }\n", indent)
			fmt.Fprintf(buf, "%s\tvar err error\n", indent)
			fmt.Fprintf(buf, "%s\tbuf, err = %s[i].AppendJSON(buf)\n", indent, accessor)
			fmt.Fprintf(buf, "%s\tif err != nil { return nil, err }\n", indent)
			fmt.Fprintf(buf, "%s}\n", indent)
			fmt.Fprintf(buf, "%sbuf = append(buf, ']')\n", indent)
		} else {
			fmt.Fprintf(buf, "%s{\n", indent)
			fmt.Fprintf(buf, "%s\tvar err error\n", indent)
			fmt.Fprintf(buf, "%s\tbuf, err = %s.AppendJSON(buf)\n", indent, accessor)
			fmt.Fprintf(buf, "%s\tif err != nil { return nil, err }\n", indent)
			fmt.Fprintf(buf, "%s}\n", indent)
		}
	case "unknown":
		// json.RawMessage pass-through.
		fmt.Fprintf(buf, "%sbuf = append(buf, %s...)\n", indent, accessor)
	case "array":
		g.genJSONEncodeArray(buf, f, accessor, indent)
	default:
		fmt.Fprintf(buf, "%s// TODO: unsupported JSON encode for type %q\n", indent, f.fieldType)
	}
}

func (g *fileGen) genJSONEncodeArray(buf *strings.Builder, f fieldInfo, accessor, indent string) {
	fmt.Fprintf(buf, "%sbuf = append(buf, '[')\n", indent)
	fmt.Fprintf(buf, "%sfor i, item := range %s {\n", indent, accessor)
	fmt.Fprintf(buf, "%s\tif i > 0 { buf = append(buf, ',') }\n", indent)

	elemType := "unknown"
	if f.field != nil && f.field.Items != nil {
		elemType = f.field.Items.Type
		if elemType == "ref" && f.field.Items.Ref != "" {
			elemType = g.resolvePrimitiveRef(f.field.Items.Ref, elemType)
		}
	}

	switch elemType {
	case "string":
		fmt.Fprintf(buf, "%s\tbuf = cbor.AppendJSONString(buf, item)\n", indent)
	case "integer":
		fmt.Fprintf(buf, "%s\tbuf = cbor.AppendJSONInt(buf, item)\n", indent)
	case "boolean":
		fmt.Fprintf(buf, "%s\tbuf = cbor.AppendJSONBool(buf, item)\n", indent)
	case "bytes":
		fmt.Fprintf(buf, "%s\tbuf = cbor.AppendJSONBytes(buf, item)\n", indent)
	case "ref", "object", "union":
		fmt.Fprintf(buf, "%s\tvar err error\n", indent)
		fmt.Fprintf(buf, "%s\tbuf, err = item.AppendJSON(buf)\n", indent)
		fmt.Fprintf(buf, "%s\tif err != nil { return nil, err }\n", indent)
	default:
		fmt.Fprintf(buf, "%s\t_ = item // TODO: unsupported array element JSON encode\n", indent)
	}

	fmt.Fprintf(buf, "%s}\n", indent)
	fmt.Fprintf(buf, "%sbuf = append(buf, ']')\n", indent)
}

func (g *fileGen) genUnmarshalJSON(buf *strings.Builder, typeName string, fields []fieldInfo) {
	// UnmarshalJSON thin wrapper.
	fmt.Fprintf(buf, "func (s *%s) UnmarshalJSON(data []byte) error {\n", typeName)
	buf.WriteString("\t_, err := s.UnmarshalJSONAt(data, 0)\n")
	buf.WriteString("\treturn err\n")
	buf.WriteString("}\n\n")

	// UnmarshalJSONAt
	fmt.Fprintf(buf, "func (s *%s) UnmarshalJSONAt(data []byte, pos int) (int, error) {\n", typeName)
	buf.WriteString("\ts.extra = clearExtra(s.extra, extraEncodingJSON)\n")
	buf.WriteString("\tvar err error\n")
	buf.WriteString("\tpos, err = cbor.ReadJSONObjectStart(data, pos)\n")
	buf.WriteString("\tif err != nil { return 0, err }\n")
	buf.WriteString("\tfor {\n")
	buf.WriteString("\t\tvar done bool\n")
	buf.WriteString("\t\tpos, done = cbor.ReadJSONObjectEnd(data, pos)\n")
	buf.WriteString("\t\tif done { return pos, nil }\n")
	buf.WriteString("\t\tvar key string\n")
	buf.WriteString("\t\tkey, pos, err = cbor.ReadJSONKey(data, pos)\n")
	buf.WriteString("\t\tif err != nil { return 0, err }\n")
	buf.WriteString("\t\tswitch key {\n")

	for _, f := range fields {
		fmt.Fprintf(buf, "\t\tcase %q:\n", f.jsonName)
		g.genJSONDecodeField(buf, f, "\t\t\t")
	}

	buf.WriteString("\t\tdefault:\n")
	buf.WriteString("\t\t\tvalueStart := pos\n")
	buf.WriteString("\t\t\tpos, err = cbor.SkipJSONValue(data, pos)\n")
	buf.WriteString("\t\t\tif err != nil { return 0, err }\n")
	buf.WriteString("\t\t\ts.extra = append(s.extra, extraField{Key: key, Value: append([]byte(nil), data[valueStart:pos]...), Encoding: extraEncodingJSON})\n")
	buf.WriteString("\t\t}\n")
	buf.WriteString("\t\tpos = cbor.SkipJSONComma(data, pos)\n")
	buf.WriteString("\t}\n")
	buf.WriteString("}")
}

func (g *fileGen) genJSONDecodeField(buf *strings.Builder, f fieldInfo, indent string) {
	isOption := strings.HasPrefix(f.goType, "gt.Option[")
	ft := g.resolveFieldType(f)

	switch ft {
	case "string":
		if isOption {
			fmt.Fprintf(buf, "%sif cbor.IsJSONNull(data, pos) {\n", indent)
			fmt.Fprintf(buf, "%s\tpos, err = cbor.SkipJSONNull(data, pos)\n", indent)
			fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)
			fmt.Fprintf(buf, "%s} else {\n", indent)
			fmt.Fprintf(buf, "%s\tvar v string\n", indent)
			fmt.Fprintf(buf, "%s\tv, pos, err = cbor.ReadJSONString(data, pos)\n", indent)
			fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)
			fmt.Fprintf(buf, "%s\ts.%s = gt.Some(v)\n", indent, f.goField)
			fmt.Fprintf(buf, "%s}\n", indent)
		} else {
			fmt.Fprintf(buf, "%ss.%s, pos, err = cbor.ReadJSONString(data, pos)\n", indent, f.goField)
			fmt.Fprintf(buf, "%sif err != nil { return 0, err }\n", indent)
		}
	case "integer":
		if isOption {
			fmt.Fprintf(buf, "%sif cbor.IsJSONNull(data, pos) {\n", indent)
			fmt.Fprintf(buf, "%s\tpos, err = cbor.SkipJSONNull(data, pos)\n", indent)
			fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)
			fmt.Fprintf(buf, "%s} else {\n", indent)
			fmt.Fprintf(buf, "%s\tvar v int64\n", indent)
			fmt.Fprintf(buf, "%s\tv, pos, err = cbor.ReadJSONInt(data, pos)\n", indent)
			fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)
			fmt.Fprintf(buf, "%s\ts.%s = gt.Some(v)\n", indent, f.goField)
			fmt.Fprintf(buf, "%s}\n", indent)
		} else {
			fmt.Fprintf(buf, "%ss.%s, pos, err = cbor.ReadJSONInt(data, pos)\n", indent, f.goField)
			fmt.Fprintf(buf, "%sif err != nil { return 0, err }\n", indent)
		}
	case "boolean":
		if isOption {
			fmt.Fprintf(buf, "%sif cbor.IsJSONNull(data, pos) {\n", indent)
			fmt.Fprintf(buf, "%s\tpos, err = cbor.SkipJSONNull(data, pos)\n", indent)
			fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)
			fmt.Fprintf(buf, "%s} else {\n", indent)
			fmt.Fprintf(buf, "%s\tvar v bool\n", indent)
			fmt.Fprintf(buf, "%s\tv, pos, err = cbor.ReadJSONBool(data, pos)\n", indent)
			fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)
			fmt.Fprintf(buf, "%s\ts.%s = gt.Some(v)\n", indent, f.goField)
			fmt.Fprintf(buf, "%s}\n", indent)
		} else {
			fmt.Fprintf(buf, "%ss.%s, pos, err = cbor.ReadJSONBool(data, pos)\n", indent, f.goField)
			fmt.Fprintf(buf, "%sif err != nil { return 0, err }\n", indent)
		}
	case "bytes":
		// Read as base64 string, decode.
		fmt.Fprintf(buf, "%s{\n", indent)
		fmt.Fprintf(buf, "%s\tvar raw string\n", indent)
		fmt.Fprintf(buf, "%s\traw, pos, err = cbor.ReadJSONString(data, pos)\n", indent)
		fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)
		fmt.Fprintf(buf, "%s\ts.%s, err = base64.RawStdEncoding.DecodeString(raw)\n", indent, f.goField)
		fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)
		fmt.Fprintf(buf, "%s}\n", indent)
		g.imports["encoding/base64"] = true
	case "cid-link", "blob", "object", "ref", "union":
		if elemType := g.refToArrayElemType(f); elemType != "" {
			fmt.Fprintf(buf, "%s{\n", indent)
			fmt.Fprintf(buf, "%s\tpos, err = cbor.ReadJSONArrayStart(data, pos)\n", indent)
			fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)
			fmt.Fprintf(buf, "%s\ts.%s = nil\n", indent, f.goField)
			fmt.Fprintf(buf, "%s\tfor {\n", indent)
			fmt.Fprintf(buf, "%s\t\tvar done bool\n", indent)
			fmt.Fprintf(buf, "%s\t\tpos, done = cbor.ReadJSONArrayEnd(data, pos)\n", indent)
			fmt.Fprintf(buf, "%s\t\tif done { break }\n", indent)
			fmt.Fprintf(buf, "%s\t\tvar elem %s\n", indent, elemType)
			fmt.Fprintf(buf, "%s\t\tpos, err = elem.UnmarshalJSONAt(data, pos)\n", indent)
			fmt.Fprintf(buf, "%s\t\tif err != nil { return 0, err }\n", indent)
			fmt.Fprintf(buf, "%s\t\ts.%s = append(s.%s, elem)\n", indent, f.goField, f.goField)
			fmt.Fprintf(buf, "%s\t\tpos = cbor.SkipJSONComma(data, pos)\n", indent)
			fmt.Fprintf(buf, "%s\t}\n", indent)
			fmt.Fprintf(buf, "%s}\n", indent)
		} else {
			g.genJSONDecodeNested(buf, f, indent, isOption)
		}
	case "unknown":
		// json.RawMessage: capture raw bytes.
		g.imports["encoding/json"] = true
		fmt.Fprintf(buf, "%s{\n", indent)
		fmt.Fprintf(buf, "%s\tstart := pos\n", indent)
		fmt.Fprintf(buf, "%s\tpos, err = cbor.SkipJSONValue(data, pos)\n", indent)
		fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)
		fmt.Fprintf(buf, "%s\ts.%s = json.RawMessage(data[start:pos])\n", indent, f.goField)
		fmt.Fprintf(buf, "%s}\n", indent)
	case "array":
		g.genJSONDecodeArray(buf, f, indent)
	default:
		fmt.Fprintf(buf, "%spos, err = cbor.SkipJSONValue(data, pos)\n", indent)
		fmt.Fprintf(buf, "%sif err != nil { return 0, err }\n", indent)
	}
}

func (g *fileGen) genJSONDecodeNested(buf *strings.Builder, f fieldInfo, indent string, isOption bool) {
	if isOption {
		innerType := f.goType[len("gt.Option[") : len(f.goType)-1]
		fmt.Fprintf(buf, "%sif cbor.IsJSONNull(data, pos) {\n", indent)
		fmt.Fprintf(buf, "%s\tpos, err = cbor.SkipJSONNull(data, pos)\n", indent)
		fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)
		fmt.Fprintf(buf, "%s} else {\n", indent)
		fmt.Fprintf(buf, "%s\tvar v %s\n", indent, innerType)
		fmt.Fprintf(buf, "%s\tpos, err = v.UnmarshalJSONAt(data, pos)\n", indent)
		fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)
		fmt.Fprintf(buf, "%s\ts.%s = gt.Some(v)\n", indent, f.goField)
		fmt.Fprintf(buf, "%s}\n", indent)
	} else {
		fmt.Fprintf(buf, "%spos, err = s.%s.UnmarshalJSONAt(data, pos)\n", indent, f.goField)
		fmt.Fprintf(buf, "%sif err != nil { return 0, err }\n", indent)
	}
}

func (g *fileGen) genJSONDecodeArray(buf *strings.Builder, f fieldInfo, indent string) {
	// JSON null is valid for array fields (e.g. nil slices encode as null).
	fmt.Fprintf(buf, "%sif !cbor.IsJSONNull(data, pos) {\n", indent)
	fmt.Fprintf(buf, "%s\tpos, err = cbor.ReadJSONArrayStart(data, pos)\n", indent)
	fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)

	elemType := "unknown"
	if f.field != nil && f.field.Items != nil {
		elemType = f.field.Items.Type
		if elemType == "ref" && f.field.Items.Ref != "" {
			elemType = g.resolvePrimitiveRef(f.field.Items.Ref, elemType)
		}
	}

	goElemType := "any"
	if f.field != nil && f.field.Items != nil {
		if strings.HasPrefix(f.goType, "[]") {
			goElemType = f.goType[2:]
		} else if strings.HasPrefix(f.goType, "gt.Option[[]") {
			goElemType = f.goType[len("gt.Option[[]") : len(f.goType)-1]
		}
	}

	fmt.Fprintf(buf, "%s\ts.%s = nil\n", indent, f.goField)
	fmt.Fprintf(buf, "%s\tfor {\n", indent)
	fmt.Fprintf(buf, "%s\t\tvar done bool\n", indent)
	fmt.Fprintf(buf, "%s\t\tpos, done = cbor.ReadJSONArrayEnd(data, pos)\n", indent)
	fmt.Fprintf(buf, "%s\t\tif done { break }\n", indent)

	switch elemType {
	case "string":
		fmt.Fprintf(buf, "%s\t\tvar elem %s\n", indent, goElemType)
		fmt.Fprintf(buf, "%s\t\telem, pos, err = cbor.ReadJSONString(data, pos)\n", indent)
		fmt.Fprintf(buf, "%s\t\tif err != nil { return 0, err }\n", indent)
		fmt.Fprintf(buf, "%s\t\ts.%s = append(s.%s, elem)\n", indent, f.goField, f.goField)
	case "integer":
		fmt.Fprintf(buf, "%s\t\tvar elem %s\n", indent, goElemType)
		fmt.Fprintf(buf, "%s\t\telem, pos, err = cbor.ReadJSONInt(data, pos)\n", indent)
		fmt.Fprintf(buf, "%s\t\tif err != nil { return 0, err }\n", indent)
		fmt.Fprintf(buf, "%s\t\ts.%s = append(s.%s, elem)\n", indent, f.goField, f.goField)
	case "boolean":
		fmt.Fprintf(buf, "%s\t\tvar elem %s\n", indent, goElemType)
		fmt.Fprintf(buf, "%s\t\telem, pos, err = cbor.ReadJSONBool(data, pos)\n", indent)
		fmt.Fprintf(buf, "%s\t\tif err != nil { return 0, err }\n", indent)
		fmt.Fprintf(buf, "%s\t\ts.%s = append(s.%s, elem)\n", indent, f.goField, f.goField)
	case "bytes":
		fmt.Fprintf(buf, "%s\t\t{\n", indent)
		fmt.Fprintf(buf, "%s\t\t\tvar raw string\n", indent)
		fmt.Fprintf(buf, "%s\t\t\traw, pos, err = cbor.ReadJSONString(data, pos)\n", indent)
		fmt.Fprintf(buf, "%s\t\t\tif err != nil { return 0, err }\n", indent)
		fmt.Fprintf(buf, "%s\t\t\tvar elem []byte\n", indent)
		fmt.Fprintf(buf, "%s\t\t\telem, err = base64.RawStdEncoding.DecodeString(raw)\n", indent)
		fmt.Fprintf(buf, "%s\t\t\tif err != nil { return 0, err }\n", indent)
		fmt.Fprintf(buf, "%s\t\t\ts.%s = append(s.%s, elem)\n", indent, f.goField, f.goField)
		fmt.Fprintf(buf, "%s\t\t}\n", indent)
	case "ref", "object", "union":
		fmt.Fprintf(buf, "%s\t\tvar elem %s\n", indent, goElemType)
		fmt.Fprintf(buf, "%s\t\tpos, err = elem.UnmarshalJSONAt(data, pos)\n", indent)
		fmt.Fprintf(buf, "%s\t\tif err != nil { return 0, err }\n", indent)
		fmt.Fprintf(buf, "%s\t\ts.%s = append(s.%s, elem)\n", indent, f.goField, f.goField)
	default:
		fmt.Fprintf(buf, "%s\t\tpos, err = cbor.SkipJSONValue(data, pos)\n", indent)
		fmt.Fprintf(buf, "%s\t\tif err != nil { return 0, err }\n", indent)
	}

	fmt.Fprintf(buf, "%s\t\tpos = cbor.SkipJSONComma(data, pos)\n", indent)
	fmt.Fprintf(buf, "%s\t}\n", indent)
	fmt.Fprintf(buf, "%s} else {\n", indent)
	fmt.Fprintf(buf, "%s\tpos, err = cbor.SkipJSONNull(data, pos)\n", indent)
	fmt.Fprintf(buf, "%s\tif err != nil { return 0, err }\n", indent)
	fmt.Fprintf(buf, "%s}\n", indent)
}
