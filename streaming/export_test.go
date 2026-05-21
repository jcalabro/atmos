package streaming

// NewEventWithVerifiedOpsForTest constructs an Event whose Operations()
// will yield ops directly. Test-only.
func NewEventWithVerifiedOpsForTest(ops []Operation) *Event {
	return &Event{verifiedOps: ops, verifierRan: true}
}

// NewStrictEventWithVerifiedOpsForTest constructs an Event whose
// Operations() will yield ops directly, with strict validation
// enabled. Test-only.
func NewStrictEventWithVerifiedOpsForTest(ops []Operation) *Event {
	return &Event{verifiedOps: ops, verifierRan: true, strictValidation: true}
}
