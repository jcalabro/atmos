package lexval

import (
	"strconv"
	"strings"
)

// ValidationError is a single validation failure at a specific field path.
type ValidationError struct {
	Path    string // e.g. "embed.images[0].alt"
	Message string // e.g. "string exceeds maxLength 1000"
}

func (e *ValidationError) Error() string {
	return e.Path + ": " + e.Message
}

// multiError joins multiple validation errors.
type multiError []*ValidationError

func (m multiError) Error() string {
	var b strings.Builder
	for i, e := range m {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(e.Error())
	}
	return b.String()
}

// path tracks the current location in the data tree via a linked list.
// Nodes live on the stack during recursion; String() is only called on error.
type path struct {
	parent *path
	seg    string // ".fieldName" or "[0]"
}

func (p *path) field(name string) *path {
	return &path{parent: p, seg: "." + name}
}

func (p *path) index(i int) *path {
	return &path{parent: p, seg: "[" + strconv.Itoa(i) + "]"}
}

func (p *path) String() string {
	if p == nil {
		return ""
	}
	// Count segments.
	n := 0
	for cur := p; cur != nil; cur = cur.parent {
		n++
	}
	segs := make([]string, n)
	i := n - 1
	for cur := p; cur != nil; cur = cur.parent {
		segs[i] = cur.seg
		i--
	}
	return strings.Join(segs, "")
}

func addErr(errs *[]*ValidationError, p *path, msg string) {
	*errs = append(*errs, &ValidationError{Path: p.String(), Message: msg})
}
