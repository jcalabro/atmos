# Spaces Phase 1 fixture generator

`generate.mjs` transcribes the repository crypto and CAR algorithms from
atproto PR #5187 at commit `9d787ebff231ff8f4e01c63717e9f5bcd6e1bc33`.
Its cryptographic dependencies are exact-pinned in `package-lock.json`.

Run `npm ci && npm run generate` deliberately when reviewing a pin update.
Normal builds and tests consume the checked-in JSON and CAR files and require
neither Node nor network access. P-256 and K-256 signatures are deterministic
fixed fixtures; regeneration must reproduce every artifact byte-for-byte.
