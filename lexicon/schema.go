// Package lexicon parses ATProto Lexicon JSON schema files.
package lexicon

import "encoding/json"

// EnumValues is a lexicon "enum" constraint. The atproto data model permits
// string enums and integer enums; both are captured here as their string form
// (integers via base-10) so a single representation serves both the string and
// integer validators. Without a tolerant decoder a numeric enum array would
// fail to unmarshal into []string and break parsing of the whole lexicon.
type EnumValues []string

// UnmarshalJSON accepts a JSON array whose elements are strings or numbers.
func (e *EnumValues) UnmarshalJSON(data []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	out := make([]string, 0, len(raw))
	for _, r := range raw {
		var s string
		if err := json.Unmarshal(r, &s); err == nil {
			out = append(out, s)
			continue
		}
		var n json.Number
		if err := json.Unmarshal(r, &n); err == nil {
			out = append(out, n.String())
			continue
		}
		return &json.UnsupportedValueError{Str: string(r)}
	}
	*e = out
	return nil
}

// Schema is a parsed lexicon document.
type Schema struct {
	Lexicon int             `json:"lexicon"` // always 1
	ID      string          `json:"id"`      // NSID, e.g. "app.bsky.feed.post"
	Desc    string          `json:"description,omitempty"`
	Defs    map[string]*Def `json:"defs"`
}

// Def is a single named definition within a schema.
type Def struct {
	Type string `json:"type"` // "record", "query", "procedure", "subscription", "object", "string", "token", etc.
	Desc string `json:"description,omitempty"`

	// Record
	Key    string  `json:"key,omitempty"`    // "tid", "nsid", "any", "literal:self"
	Record *Object `json:"record,omitempty"` // the record's object schema

	// Query / Procedure
	Parameters *Params    `json:"parameters,omitempty"`
	Input      *Body      `json:"input,omitempty"`
	Output     *Body      `json:"output,omitempty"`
	Errors     []ErrorDef `json:"errors,omitempty"`

	// Subscription
	Message *Message `json:"message,omitempty"`

	// Inline object fields (when Type is "object")
	Properties map[string]*Field `json:"properties,omitempty"`
	Required   []string          `json:"required,omitempty"`
	Nullable   []string          `json:"nullable,omitempty"`

	// Inline string fields (when Type is "string")
	Format       string   `json:"format,omitempty"`
	MaxLength    int      `json:"maxLength,omitempty"`
	MinLength    int      `json:"minLength,omitempty"`
	MaxGraphemes int      `json:"maxGraphemes,omitempty"`
	MinGraphemes int      `json:"minGraphemes,omitempty"`
	Enum         EnumValues `json:"enum,omitempty"`
	KnownValues  []string   `json:"knownValues,omitempty"`
	Default      any      `json:"default,omitempty"`
	Const        any      `json:"const,omitempty"`

	// Inline integer fields (when Type is "integer")
	Minimum *int64 `json:"minimum,omitempty"`
	Maximum *int64 `json:"maximum,omitempty"`

	// Inline array fields (when Type is "array")
	Items *Field `json:"items,omitempty"`

	// Inline union fields (when Type is "union")
	Refs   []string `json:"refs,omitempty"`
	Closed bool     `json:"closed,omitempty"`

	// Inline ref field (when Type is "ref")
	Ref string `json:"ref,omitempty"`
}

// Object describes a lexicon object type with named properties.
type Object struct {
	Type       string            `json:"type"` // always "object"
	Desc       string            `json:"description,omitempty"`
	Properties map[string]*Field `json:"properties,omitempty"`
	Required   []string          `json:"required,omitempty"`
	Nullable   []string          `json:"nullable,omitempty"`
}

// Field describes a single field within an object or array items.
// Constraints are shared across types that use them (e.g. maxLength
// applies to strings as byte length and to arrays as element count).
type Field struct {
	Type string `json:"type"` // "string", "integer", "boolean", "bytes", "cid-link", "blob", "array", "object", "ref", "union", "unknown", "null"
	Desc string `json:"description,omitempty"`

	// String / Array / Bytes constraints (maxLength means bytes for string, elements for array)
	Format       string   `json:"format,omitempty"`
	MaxLength    int      `json:"maxLength,omitempty"`
	MinLength    int      `json:"minLength,omitempty"`
	MaxGraphemes int      `json:"maxGraphemes,omitempty"`
	MinGraphemes int      `json:"minGraphemes,omitempty"`
	Enum         EnumValues `json:"enum,omitempty"`
	KnownValues  []string   `json:"knownValues,omitempty"`
	Default      any      `json:"default,omitempty"`
	Const        any      `json:"const,omitempty"`

	// Integer constraints
	Minimum *int64 `json:"minimum,omitempty"`
	Maximum *int64 `json:"maximum,omitempty"`

	// Array items
	Items *Field `json:"items,omitempty"`

	// Ref
	Ref string `json:"ref,omitempty"`

	// Union
	Refs   []string `json:"refs,omitempty"`
	Closed bool     `json:"closed,omitempty"`

	// Blob
	Accept  []string `json:"accept,omitempty"`
	MaxSize int64    `json:"maxSize,omitempty"`

	// Nested object
	Properties map[string]*Field `json:"properties,omitempty"`
	Required   []string          `json:"required,omitempty"`
	Nullable   []string          `json:"nullable,omitempty"`
}

// Params describes query/procedure parameters (restricted to primitives).
type Params struct {
	Type       string            `json:"type"` // always "params"
	Properties map[string]*Field `json:"properties,omitempty"`
	Required   []string          `json:"required,omitempty"`
}

// Body describes a request or response body.
type Body struct {
	Desc     string `json:"description,omitempty"`
	Encoding string `json:"encoding"`
	Schema   *Field `json:"schema,omitempty"`
}

// Message describes a subscription message.
type Message struct {
	Desc   string `json:"description,omitempty"`
	Schema *Field `json:"schema,omitempty"`
}

// ErrorDef describes a named error that an endpoint can return.
type ErrorDef struct {
	Name string `json:"name"`
	Desc string `json:"description,omitempty"`
}
