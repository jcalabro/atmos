package space

import (
	"errors"
	"fmt"
)

// VerificationError classifies a failed commit or CAR verification. Err
// remains available through errors.Is/errors.As; Stage is a bounded machine-
// readable component name and must not contain record paths or other sensitive
// high-cardinality values.
type VerificationError struct {
	Stage string
	Err   error
}

func (e *VerificationError) Error() string {
	return fmt.Sprintf("space: %s verification failed: %v", e.Stage, e.Err)
}

// Unwrap returns the underlying parse, cryptographic, limit, cancellation, or
// staging error.
func (e *VerificationError) Unwrap() error { return e.Err }

func wrapVerification(stage string, err error) error {
	if err == nil {
		return nil
	}
	var existing *VerificationError
	if errors.As(err, &existing) {
		return err
	}
	return &VerificationError{Stage: stage, Err: err}
}
