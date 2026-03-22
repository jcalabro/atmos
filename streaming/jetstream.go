package streaming

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/jcalabro/atmos/api/comatproto"
)

// Jetstream event kind constants.
const (
	JetstreamKindCommit   = "commit"
	JetstreamKindAccount  = "account"
	JetstreamKindIdentity = "identity"
)

// Jetstream commit operation constants.
const (
	JetstreamOpCreate = "create"
	JetstreamOpUpdate = "update"
	JetstreamOpDelete = "delete"
)

// JetstreamEvent is the top-level JSON envelope from a Jetstream server.
type JetstreamEvent struct {
	DID      string                                  `json:"did"`
	TimeUS   int64                                   `json:"time_us"`
	Kind     string                                  `json:"kind"`
	Commit   *JetstreamCommit                        `json:"commit,omitempty"`
	Account  *comatproto.SyncSubscribeRepos_Account  `json:"account,omitempty"`
	Identity *comatproto.SyncSubscribeRepos_Identity `json:"identity,omitempty"`
}

// JetstreamCommit describes a single record mutation in a Jetstream event.
type JetstreamCommit struct {
	Rev        string          `json:"rev"`
	Operation  string          `json:"operation"`
	Collection string          `json:"collection"`
	RKey       string          `json:"rkey"`
	Record     json.RawMessage `json:"record,omitempty"`
	CID        string          `json:"cid,omitempty"`
}

// JetstreamError represents an error frame from a Jetstream server.
// Error frames have an "error" key at the top level instead of "kind".
type JetstreamError struct {
	ErrorType string `json:"error"`
	Message   string `json:"message"`
}

func (e *JetstreamError) Error() string {
	return fmt.Sprintf("jetstream: %s: %s", e.ErrorType, e.Message)
}

// jetstreamRawFrame is used for initial JSON unmarshaling to detect whether
// a frame is an event or an error. Both shapes are attempted in one pass.
type jetstreamRawFrame struct {
	JetstreamEvent

	// Error fields (only present on error frames).
	ErrorType string `json:"error"`
	Message   string `json:"message"`
}

// decodeJetstreamFrame decodes a Jetstream JSON text frame into an Event.
// Error frames are returned as *JetstreamError errors.
func decodeJetstreamFrame(data []byte) (Event, error) {
	var raw jetstreamRawFrame
	if err := json.Unmarshal(data, &raw); err != nil {
		return Event{}, fmt.Errorf("decode jetstream frame: %w", err)
	}

	// Error frames have the "error" key set.
	if raw.ErrorType != "" {
		return Event{}, &JetstreamError{
			ErrorType: raw.ErrorType,
			Message:   raw.Message,
		}
	}

	js := &JetstreamEvent{
		DID:      raw.DID,
		TimeUS:   raw.TimeUS,
		Kind:     raw.Kind,
		Commit:   raw.Commit,
		Account:  raw.Account,
		Identity: raw.Identity,
	}

	evt := Event{
		Jetstream: js,
	}

	switch raw.Kind {
	case JetstreamKindCommit:
		// Commit events have no seq at the top level in Jetstream;
		// cursor tracking uses time_us.
	case JetstreamKindAccount:
		evt.Account = raw.Account
		if raw.Account != nil {
			evt.Seq = raw.Account.Seq
		}
	case JetstreamKindIdentity:
		evt.Identity = raw.Identity
		if raw.Identity != nil {
			evt.Seq = raw.Identity.Seq
		}
	default:
		if raw.Kind == "" {
			return Event{}, fmt.Errorf("decode jetstream frame: missing kind field")
		}
		return Event{}, errUnknownType
	}

	return evt, nil
}

// isJetstreamURL returns true if the URL path ends with "/subscribe",
// which is the conventional Jetstream endpoint path.
func isJetstreamURL(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	return strings.HasSuffix(parsed.Path, "/subscribe")
}
