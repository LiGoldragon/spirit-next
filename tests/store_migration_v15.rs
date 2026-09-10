//! Exact frozen v14 archive cutover witness.
//!
//! The checked-in fixture was emitted by an isolated builder using published
//! signal-spirit b37fc963 and signal-domain 801e1c5 (both rkyv 0.8). It is
//! decoded only through this local four-field closure, including a nested
//! domain taxonomy value; the v15 migration projects it without an old crate
//! or text codec in the Spirit runtime graph.

use std::{fs, os::unix::fs::PermissionsExt};

use sema_engine::{
    Assertion, Engine as SemaDatabase, EngineOpen, SchemaVersion, VersionedStoreName,
    VersioningPolicy,
};
use spirit::{
    Store, StoreMigration, StoreMigrationOutput, StoreMigrationRequest, production_migration::v14,
};
use tempfile::TempDir;

use spirit::production_migration::v14::FrozenContentful as _;

fn entry(description: &str) -> v14::Entry {
    v14::Entry {
        domains: v14::Domains::from(vec![v14::Domain::Technology(v14::Technology::Software(
            v14::Software::Data(v14::DataLeaf::All),
        ))]),
        kind: v14::Kind::Decision,
        description: v14::Description::from(String::from(description)),
        importance: v14::Importance::from(v14::Magnitude::High),
    }
}

fn record(identifier: &str, description: &str) -> v14::StoredRecord {
    v14::StoredRecord {
        record_identifier: v14::RecordIdentifier::from(String::from(identifier)),
        entry: entry(description),
    }
}

fn emitted_by_exact_old_producer() -> v14::StoredRecord {
    let (record_identifier, entry) = rkyv::from_bytes::<
        (v14::RecordIdentifier, v14::Entry),
        rkyv::rancor::Error,
    >(include_bytes!("fixtures/v14-entry-b37fc963.rkyv"))
    .expect("b37fc963 fixture decodes through the frozen v14 closure");
    assert_eq!(record_identifier.content(), "b37-fixture");
    assert_eq!(entry.description.content(), "fixture emitted by b37fc963");
    assert_eq!(entry.kind, v14::Kind::Constraint);
    assert!(matches!(
        entry.domains.content().as_slice(),
        [v14::Domain::Technology(v14::Technology::Software(
            v14::Software::Data(v14::DataLeaf::SchemaEvolution)
        ),)]
    ));
    assert_eq!(*entry.importance.content(), v14::Magnitude::VeryHigh);
    v14::StoredRecord {
        record_identifier,
        entry,
    }
}

#[test]
fn frozen_v14_cutover_projects_historical_wrapped_entry_and_seeds_truthful_lifecycle_row() {
    let sandbox = TempDir::new().expect("isolated state directory");
    let path = sandbox.path().join("spirit.sema");
    let mut database = SemaDatabase::open(
        EngineOpen::new(&path, SchemaVersion::new(14)).with_versioning(VersioningPolicy::new(
            VersionedStoreName::new(v14::STORE_NAME),
        )),
    )
    .expect("open exact frozen v14-layout source");
    let records = database
        .register_table(v14::records_descriptor())
        .expect("v14 records");
    let migrations = database
        .register_table(v14::migrations_descriptor())
        .expect("v14 migrations");
    database
        .assert(Assertion::new(records, emitted_by_exact_old_producer()))
        .expect("seed v14 record");
    database
        .assert(Assertion::new(
            migrations,
            v14::Migration {
                source_schema_version: v14::SourceSchemaVersion::from(13),
                migrated_record_count: v14::MigratedRecordCount::from(1),
            },
        ))
        .expect("seed v14 receipt");
    drop(database);
    let source_bytes = fs::read(&path).expect("source bytes before cutover");

    let result = StoreMigration::new(
        StoreMigrationRequest::new(path.display().to_string())
            .with_legacy_configuration_archive_path(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/fixtures/v14-configuration-b37fc963.rkyv"
            )),
    )
    .run()
    .expect("offline v14 cutover");
    assert!(matches!(result, StoreMigrationOutput::Migrated(_)));
    let store = Store::open(&path).expect("open v15 target");
    assert_eq!(store.store_schema_version(), 15);
    assert_eq!(
        store
            .entry_by_identifier("b37-fixture")
            .expect("lookup")
            .expect("row")
            .description,
        "fixture emitted by b37fc963"
    );
    let state = store
        .nexus_configuration_state()
        .expect("one lifecycle row");
    assert!(
        !state.meta_configure_occurred,
        "a v14 archive cannot imply a meta Configure"
    );
    assert_eq!(
        state.desired_configuration.socket_path,
        "/tmp/old-spirit.sock"
    );
    assert_eq!(
        state
            .desired_configuration
            .optional_meta_socket_path
            .as_deref(),
        Some("/tmp/old-meta.sock")
    );
    assert_eq!(
        state
            .desired_configuration
            .optional_trace_socket_path
            .as_deref(),
        Some("/tmp/old-trace.sock")
    );
    assert_eq!(
        state.desired_configuration.authorization_mode,
        signal_spirit::AuthorizationMode::Observing,
    );
    let rollback = sandbox
        .path()
        .join("spirit.schema-14-rollback/live.v14.sema");
    assert_eq!(
        fs::read(rollback).expect("preserved v14 copy"),
        source_bytes
    );
    assert_eq!(
        fs::metadata(sandbox.path().join("spirit.schema-14-rollback"))
            .expect("rollback metadata")
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
}

#[test]
fn normal_v15_open_refuses_an_unmigrated_v14_source_without_rewriting_it() {
    let sandbox = TempDir::new().expect("isolated state directory");
    let path = sandbox.path().join("spirit.sema");
    let mut database = SemaDatabase::open(
        EngineOpen::new(&path, SchemaVersion::new(14)).with_versioning(VersioningPolicy::new(
            VersionedStoreName::new(v14::STORE_NAME),
        )),
    )
    .expect("open exact frozen v14-layout source");
    let records = database
        .register_table(v14::records_descriptor())
        .expect("v14 records");
    database
        .assert(Assertion::new(
            records,
            record("do-not-touch", "normal startup must not migrate a store"),
        ))
        .expect("seed source");
    drop(database);
    let before = fs::read(&path).expect("source bytes");
    assert!(
        Store::open(&path).is_err(),
        "normal startup requires offline cutover"
    );
    assert_eq!(fs::read(&path).expect("source bytes after refusal"), before);
}
