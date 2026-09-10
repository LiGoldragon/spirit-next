# spirit agent notes

## Status: deprecated

The living marked Spirit deprecated on 2026-09-10. Psyche is the intended
replacement, re-authoring Spirit semantics from source evidence. Treat this
repository as a legacy semantic donor, not an active development or stack
migration target. Do not add new consumers or resume the unfinished migration
without new explicit direction. The documentation below describes historical
behavior and does not establish active status.

Read this repo's `ARCHITECTURE.md` and `README.md` before editing code.

`spirit` is the production Spirit daemon. It currently builds through the
wired legacy schema/schema-rust toolchain, not the approved Ethos
architecture: the language was renamed Ethos on 2026-07-27, and legacy
schema, schema-language, and schema-rust die under their old names (S1R
entry 7). Spirit's port onto Ethos-based generation is in progress and
blocked at bead `protos-engine-po1.10.11`; do not treat this repo's current
toolchain as the pattern to copy until the port lands. The ordinary public
contract lives in `signal-spirit`; the owner-only meta contract lives in
`meta-signal-spirit`; this repo owns the daemon-local Nexus, SEMA, and daemon
runtime schema surfaces.

Load-bearing rules for this repo:

- Keep the daemon binary on the one binary rkyv configuration argument; DOTOS is
  a CLI/text-edge feature, not daemon startup parsing.
- Do not hand-edit generated runtime modules as the effective fix. Edit
  `schema/*.schema` (the current, not-yet-ported schema/schema-rust toolchain)
  and regenerate with `SPIRIT_UPDATE_SCHEMA_ARTIFACTS=1 cargo build`; commit
  the checked-in generated `src/schema/*.rs` outputs when they change.
- Preserve the no-DOTOS daemon dependency invariant: `dotos-text` gates the text
  surface, and `tests/dependency_surface.rs` guards the binary-only build.
- Process-boundary behavior should be proven through the real daemon/CLI path,
  not only in-memory helpers.

## Protos estate status

Stack: deprecated legacy evidence
Status: deprecated by the living on 2026-09-10
This repository is not a destination for new-stack adoption.
