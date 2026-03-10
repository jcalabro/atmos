package atmos

import "fmt"

// SyntaxError is returned when parsing an invalid identifier.
type SyntaxError struct {
	Type  string
	Value string
	Msg   string
}

func (e *SyntaxError) Error() string {
	if e.Msg != "" {
		return fmt.Sprintf("invalid %s %q: %s", e.Type, e.Value, e.Msg)
	}
	return fmt.Sprintf("invalid %s %q", e.Type, e.Value)
}

func syntaxErr(typ, val, msg string) error {
	return &SyntaxError{Type: typ, Value: val, Msg: msg}
}
