package atmos

// Action is the type of record mutation observed in a firehose
// commit event or yielded by the verifier. Underlying type is string
// so values match the on-the-wire RepoOp.Action lexicon strings
// directly; the named type provides compile-time safety against
// typos at API boundaries (a "creat" literal won't compile against
// a parameter typed Action).
//
// Use the package-level constants — [ActionCreate], [ActionUpdate],
// [ActionDelete], [ActionResync] — rather than string literals.
type Action string

// Action constants. Values match the lexicon's
// com.atproto.sync.subscribeRepos#repoOp.action enum, plus
// [ActionResync] for ops yielded during a getRepo-driven resync.
const (
	ActionCreate Action = "create"
	ActionUpdate Action = "update"
	ActionDelete Action = "delete"
	ActionResync Action = "resync"
)
