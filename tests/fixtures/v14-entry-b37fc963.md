# Frozen Spirit v14 payload fixture

`v14-entry-b37fc963.rkyv` was emitted by an isolated temporary Cargo builder
using the published `signal-spirit` revision
`b37fc963292c157452d06e150296c19005dae3f2` and its declared
`signal-domain` revision `801e1c5bcc824c9760e246205826e3c8e962d005`, both
on rkyv `0.8.17` with `little_endian`, `pointer_width_32`, and `unaligned`.

The builder consumes the historical checked-in generated sources only. Its
obsolete frame build surface was deliberately excluded from the fixture build;
the emitted value is the actual old `RecordIdentifier` plus four-field `Entry`
payload: one `Technology.Software.Data.SchemaEvolution` domain, `Constraint`,
the description `fixture emitted by b37fc963`, and `VeryHigh` importance.

The Spirit production-migration test decodes this artifact exclusively through
the local frozen v14 closure, projects it into current named data, and reopens
the resulting v15 store. The temporary builder is not a dependency of Spirit.

The recorded builder source is `v14-entry-b37fc963-builder.rs`. It ran in an
isolated temporary Cargo package. To avoid its unrelated obsolete frame API
from entering the build, that package compiled the historical checked-in
generated `signal.rs` only through the Entry definition; it did not alter the
producer payload declarations or their rkyv derives.

`v14-configuration-b37fc963.rkyv` is emitted by the same isolated builder.
It carries the old daemon socket/meta/trace paths, `Observing` authorization,
and the obsolete historical database path. The migration test proves that the
first four settings become persisted desired Nexus configuration while the
old database path is not projected and the truthful meta marker remains false.
