mod archive;
mod error;
mod family_directory;
#[cfg(feature = "agent-guardian")]
use crate::schema::signal::{RecordSet, Selection};

#[cfg(feature = "agent-guardian")]
mod guardian_bundle;
mod record_identifier;

use std::{
    collections::BTreeSet,
    fmt, fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use nexus::Configurable as _;
#[cfg(feature = "mirror-shipper")]
use sema_engine::PortableCheckpoint;
use sema_engine::{
    Assertion, Checkpoint, CheckpointReceipt, CommitSequence, Engine as SemaDatabase, EngineOpen,
    EngineRecord, EntryDigest, Mutation, QueryPlan, RecordKey, Retraction, SchemaVersion,
    TableReference, VersionedCommitLogEntry, VersionedStoreName, VersioningPolicy,
};

pub(crate) use archive::ArchiveDatabase;
pub use error::StoreError;
pub use family_directory::StoreFamilyDirectory;
#[cfg(feature = "agent-guardian")]
use guardian_bundle::GuardianRecordBundle;
use record_identifier::RecordIdentifierMint;

#[cfg(feature = "agent-guardian")]
use crate::guardian_journal::{GuardianDecision, GuardianJournal, GuardianOperation};
use crate::schema::sema::Contentful as SemaContentful;
use crate::schema::{
    meta_signal::ArchiveDatabaseTarget,
    sema::{
        self as sema_schema, EngineStartFailure as SemaEngineStartFailure,
        EngineStopFailure as SemaEngineStopFailure, Migration, ReadInput as SemaReadInput,
        ReadOutput as SemaReadOutput, RecordFamily, SemaEngine, StoredNexusConfiguration,
        StoredRecord, WriteInput as SemaWriteInput, WriteOutput as SemaWriteOutput,
    },
    signal::{
        ClarificationReceipt, ClarificationRequest, ClarificationResolution,
        ClarificationResolutionReceipt, CountedRecords, DatabaseMarker, Description, DomainMatch,
        Entry, ErrorReport, FoundRecord, GuardianRejection, GuardianRejectionReason, Importance,
        ImportanceBump, ImportanceBumpReceipt, ImportanceSelection, Keyword, KeywordMatch,
        Keywords, Magnitude, ObservedRecord, ObservedRecords, Query, RecordChange,
        RecordChangeReceipt, Retirement, RetirementReceipt, SearchText, SemaReceipt,
        SpiritNexusConfiguration, Supersession, SupersessionReceipt, TextMatch,
    },
};

const TEXT_SEARCH_LIMIT: usize = 25;

use signal_domain::{
    DataLeaf, Domain, DomainScope, DomainScopes, SoftwareDomain, TechnologyDomain,
};

#[cfg(feature = "testing-trace")]
use crate::{ObjectName, TraceEvent, TraceLog, schema::sema::SemaObjectName};

// Version 14 contains only live records and migration receipts. The offline
// migration projects v13 live and lifecycle-archive records into fresh v14
// stores; no prior log or retired-family row is replayed.
pub(super) const SPIRIT_SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(15);

/// The v14 mirror/log generation. It is deliberately distinct from the v13
/// `spirit:sema` root, so no current checkpoint or suffix can attach to the
/// legacy history that contains retired fields.
pub const SPIRIT_STORE_NAME: &str = "spirit:sema:v15";

/// The SEMA durable store: a sema-engine keyed table written to a `*.sema`
/// file.
///
/// SEMA means database work. `Store` maps generated SEMA roots onto
/// sema-engine operations; sema-engine owns the database handle, durable
/// commit sequence, and typed rkyv table access. Spirit owns the
/// production-compatible short/base36 record identifiers because migration must
/// preserve them as stable keys. Query predicate semantics stay here because
/// they are Spirit-specific SEMA behavior, not generic daemon plumbing.
pub struct Store {
    // The engine serializes its own writes; after startup table registration the
    // store no longer needs exclusive ownership of the database handle.
    database: Arc<SemaDatabase>,
    entries: TableReference<StoredRecord>,
    migrations: TableReference<Migration>,
    configurations: TableReference<StoredNexusConfiguration>,
    path: PathBuf,
    archive_target: ArchiveDatabaseTarget,
    #[cfg(feature = "testing-trace")]
    trace_log: TraceLog,
}

#[cfg(feature = "mirror-shipper")]
struct MirrorRestoreImport {
    checkpoint: Checkpoint,
    suffix: Vec<VersionedCommitLogEntry>,
    restored_head: EntryDigest,
}

#[cfg(feature = "mirror-shipper")]
impl MirrorRestoreImport {
    fn from_bundle(bundle: signal_mirror::RestoreBundle) -> Result<Self, StoreError> {
        let checkpoint =
            PortableCheckpoint::from_bytes(bundle.checkpoint.artifact.as_slice().to_vec())
                .decode()?;
        let restored_head = bundle
            .suffix()
            .last()
            .map(|envelope| EntryDigest::new(*envelope.digest.as_bytes()))
            .unwrap_or_else(|| checkpoint.metadata().covered_entry_digest());
        let suffix = bundle
            .into_suffix()
            .into_iter()
            .map(|envelope| {
                rkyv::from_bytes::<VersionedCommitLogEntry, rkyv::rancor::Error>(
                    envelope.payload.as_slice(),
                )
                .map_err(|source| StoreError::ArchiveDecode {
                    message: source.to_string(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            checkpoint,
            suffix,
            restored_head,
        })
    }

    fn into_store(
        self,
        path: impl Into<PathBuf>,
        expected_head: EntryDigest,
    ) -> Result<Store, StoreError> {
        if self.restored_head != expected_head {
            return Err(StoreError::MirrorRestoreHeadMismatch {
                expected: expected_head,
                restored: self.restored_head,
            });
        }
        Store::import(path, self.checkpoint, self.suffix)
    }
}

impl fmt::Debug for Store {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Store")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl SemaEngine for Store {
    fn on_start(&mut self) -> Result<(), SemaEngineStartFailure> {
        #[cfg(feature = "testing-trace")]
        self.trace_sema_activation(SemaObjectName::Started);
        Ok(())
    }

    fn on_stop(&mut self) -> Result<(), SemaEngineStopFailure> {
        #[cfg(feature = "testing-trace")]
        self.trace_sema_activation(SemaObjectName::Stopped);
        Ok(())
    }

    #[cfg(feature = "testing-trace")]
    fn trace_sema_activation(&self, object_name: SemaObjectName) {
        self.trace_log
            .record(TraceEvent::new(ObjectName::Sema(object_name)));
    }

    fn apply_inner(
        &mut self,
        command: sema_schema::sema::Sema<sema_schema::sema::WriteInput>,
    ) -> sema_schema::sema::Sema<sema_schema::sema::WriteOutput> {
        let origin_route = command.origin_route();
        let output = match command.into_root() {
            SemaWriteInput::Record(record) => match self.record(record.content()) {
                Ok(identifier) => SemaWriteOutput::recorded(SemaReceipt {
                    record_identifier: identifier,
                    database_marker: self.database_marker(),
                }),
                Err(error) => SemaWriteOutput::missed(ErrorReport {
                    error_message: error.to_string(),
                }),
            },
            SemaWriteInput::BumpImportance(change) => {
                match self.bump_importance(change.content()) {
                    Ok(Some(receipt)) => SemaWriteOutput::importance_bumped(receipt),
                    Ok(None) => SemaWriteOutput::missed(ErrorReport {
                        error_message: "record not found".into(),
                    }),
                    Err(error) => SemaWriteOutput::missed(ErrorReport {
                        error_message: error.to_string(),
                    }),
                }
            }
            SemaWriteInput::ChangeRecord(change) => match self.change_record(change.content()) {
                Ok(Some(receipt)) => SemaWriteOutput::record_changed(receipt),
                Ok(None) => SemaWriteOutput::missed(ErrorReport {
                    error_message: "record not found".into(),
                }),
                Err(error) => SemaWriteOutput::missed(ErrorReport {
                    error_message: error.to_string(),
                }),
            },
        };
        output.with_origin_route(origin_route)
    }

    fn observe_inner(
        &self,
        query: sema_schema::sema::Sema<sema_schema::sema::ReadInput>,
    ) -> sema_schema::sema::Sema<sema_schema::sema::ReadOutput> {
        let origin_route = query.origin_route();
        let output = match query.into_root() {
            SemaReadInput::Observe(observe) => match self.observe(&observe.content()) {
                Ok(entries) if !entries.is_empty() => SemaReadOutput::observed(ObservedRecords {
                    record_set: entries,
                }),
                Ok(_) => SemaReadOutput::missed(ErrorReport {
                    error_message: "no matching record".into(),
                }),
                Err(error) => SemaReadOutput::missed(ErrorReport {
                    error_message: error.to_string(),
                }),
            },
            SemaReadInput::Intent(intent) => match self.intent(&intent.content()) {
                Ok(entries) if !entries.is_empty() => {
                    SemaReadOutput::intent_results(ObservedRecords {
                        record_set: entries,
                    })
                }
                Ok(_) => SemaReadOutput::missed(ErrorReport {
                    error_message: "no matching record".into(),
                }),
                Err(error) => SemaReadOutput::missed(ErrorReport {
                    error_message: error.to_string(),
                }),
            },
            SemaReadInput::TextSearch(search) => match self.text_search(&search.content()) {
                Ok(entries) if !entries.is_empty() => {
                    SemaReadOutput::text_search_results(ObservedRecords {
                        record_set: entries,
                    })
                }
                Ok(_) => SemaReadOutput::missed(ErrorReport {
                    error_message: "no matching record".into(),
                }),
                Err(error) => SemaReadOutput::missed(ErrorReport {
                    error_message: error.to_string(),
                }),
            },
            SemaReadInput::Lookup(lookup) => {
                let record_identifier = lookup.content();
                match self.entry_by_identifier(&record_identifier) {
                    Ok(Some(entry)) => SemaReadOutput::found(FoundRecord {
                        record_identifier,
                        entry,
                    }),
                    Ok(None) => SemaReadOutput::missed(ErrorReport {
                        error_message: "record not found".into(),
                    }),
                    Err(error) => SemaReadOutput::missed(ErrorReport {
                        error_message: error.to_string(),
                    }),
                }
            }
            SemaReadInput::Count(count) => match self.count(&count.content()) {
                Ok(count) => SemaReadOutput::counted(CountedRecords {
                    record_count: i64::try_from(count).expect("count fits i64"),
                }),
                Err(error) => SemaReadOutput::missed(ErrorReport {
                    error_message: error.to_string(),
                }),
            },
        };
        output.with_origin_route(origin_route)
    }
}

impl Store {
    fn versioning_policy() -> VersioningPolicy {
        VersioningPolicy::new(VersionedStoreName::new(SPIRIT_STORE_NAME))
    }

    /// Open or create the durable SEMA database at `path`.
    ///
    /// A fresh file is created with empty engine counters; an existing
    /// file resumes its persisted commit sequence and record identifier
    /// counter through sema-engine. The store opts into the versioned
    /// commit log with the schema-generated [`RecordFamily`] policy and
    /// registers every schema-declared family through its generated
    /// descriptor, so the log is the authoritative replayable history of
    /// the intent corpus.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StoreError> {
        Self::open_with_configuration(path, crate::Configuration::default_nexus_configuration())
    }

    /// Open a stable Sema and seed its one lifecycle row only when it is new.
    /// Existing state is never replaced by an executable default.
    pub fn open_with_configuration(
        path: impl Into<PathBuf>,
        default_configuration: SpiritNexusConfiguration,
    ) -> Result<Self, StoreError> {
        let path = path.into();
        // sema-engine can update bookkeeping while discovering a schema
        // mismatch. Probe a disposable byte-for-byte copy first so a normal
        // zero-argument startup cannot alter an unmigrated stable Sema.
        if path.exists() {
            let probe =
                path.with_extension(format!("spirit-open-probe-{}.sema", std::process::id()));
            if probe.exists() {
                fs::remove_file(&probe)?;
            }
            fs::copy(&path, &probe)?;
            let probe_result =
                Self::open_with_configuration_unchecked(&probe, default_configuration.clone());
            let _ = fs::remove_file(&probe);
            probe_result?;
        }
        Self::open_with_configuration_unchecked(path, default_configuration)
    }

    fn open_with_configuration_unchecked(
        path: impl Into<PathBuf>,
        default_configuration: SpiritNexusConfiguration,
    ) -> Result<Self, StoreError> {
        let path = path.into();
        let mut database = SemaDatabase::open(
            EngineOpen::new(path.clone(), SPIRIT_SCHEMA_VERSION)
                .with_versioning(Self::versioning_policy()),
        )?;
        let entries = database.register_table(RecordFamily::records_family())?;
        let migrations = database.register_table(RecordFamily::migrations_family())?;
        let configurations = database.register_table(RecordFamily::nexus_configurations_family())?;
        let store = Self {
            database: Arc::new(database),
            entries,
            migrations,
            configurations,
            path,
            archive_target: ArchiveDatabaseTarget::Default,
            #[cfg(feature = "testing-trace")]
            trace_log: TraceLog::default(),
        };
        store.seed_configuration_if_absent(default_configuration)?;
        Ok(store)
    }

    #[cfg(feature = "testing-trace")]
    pub fn open_with_trace(
        path: impl Into<PathBuf>,
        trace_log: TraceLog,
    ) -> Result<Self, StoreError> {
        Self::open(path).map(|store| store.with_trace(trace_log))
    }

    #[cfg(feature = "testing-trace")]
    pub fn with_trace(mut self, trace_log: TraceLog) -> Self {
        self.trace_log = trace_log;
        self
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn seed_configuration_if_absent(
        &self,
        default_configuration: SpiritNexusConfiguration,
    ) -> Result<(), StoreError> {
        if self.configuration_rows()?.is_empty() {
            // Only a completely new Sema may receive executable defaults. An
            // established layout missing this family must go through the
            // offline cutover; startup must not invent metadata over it.
            if !self.records()?.is_empty() || !self.migrations()?.is_empty() {
                return Err(StoreError::ConfigurationInvariant { count: 0 });
            }
            self.database.assert(Assertion::new(
                self.configurations,
                default_configuration.into(),
            ))?;
        }
        Ok(())
    }

    fn configuration_rows(&self) -> Result<Vec<StoredNexusConfiguration>, StoreError> {
        Ok(self
            .database
            .match_records(QueryPlan::all(self.configurations))?
            .records()
            .to_vec())
    }

    /// Read the single persisted desired configuration and truthful meta marker.
    pub fn nexus_configuration_state(
        &self,
    ) -> Result<nexus::ConfigurationState<SpiritNexusConfiguration>, StoreError> {
        let rows = self.configuration_rows()?;
        match rows.as_slice() {
            [row] => Ok(row.state.clone()),
            [] => Err(StoreError::ConfigurationInvariant { count: 0 }),
            rows => Err(StoreError::ConfigurationInvariant { count: rows.len() }),
        }
    }

    /// Persist one standard lifecycle transition atomically in the stable Sema.
    pub fn replace_nexus_configuration(
        &self,
        state: nexus::ConfigurationState<SpiritNexusConfiguration>,
    ) -> Result<(), StoreError> {
        self.database.mutate(Mutation::new(
            self.configurations,
            StoredNexusConfiguration { state },
        ))?;
        Ok(())
    }

    /// Store the owner-configured archive target (the owner-only meta
    /// `Configure` effect).
    ///
    /// The archive is a SEPARATE database from the live intent log. This
    /// method records WHERE that separate archive database lives; it does NOT
    /// open, move, or touch the live database in any way. Every subsequent
    /// Every working read and write keeps landing on the same live `*.sema`
    /// file the store was opened with. Explicit lifecycle operations consume
    /// this target when they capture a record before mutation or retraction.
    ///
    /// This is owner-config storage, not a database operation, so it is
    /// infallible — there is no sema-engine open at configure time.
    pub fn set_archive_target(&mut self, archive_target: ArchiveDatabaseTarget) {
        self.archive_target = archive_target;
    }

    /// The owner-configured archive target: WHERE the separate archive database
    /// lives. Defaults to [`ArchiveDatabaseTarget::Default`] until an owner
    /// `Configure` sets it.
    pub fn archive_target(&self) -> &ArchiveDatabaseTarget {
        &self.archive_target
    }

    /// The store-schema version this build writes: the major store-format
    /// generation, bumped only on a breaking store change. Surfaced on the
    /// version report so a caller can see which store generation a running
    /// daemon serves without opening its `*.sema` file.
    pub fn store_schema_version(&self) -> u32 {
        SPIRIT_SCHEMA_VERSION.value()
    }

    /// The engine's content-addressed store-schema hash: the identity of the
    /// registered family set (family names plus per-family schema hashes).
    /// Rendered as lowercase hex so it travels as version-report text.
    pub fn store_schema_hash(&self) -> String {
        self.database.store_schema_hash().to_string()
    }

    /// A shared handle to the underlying versioned engine, for the mirror
    /// shipper. The returned `Arc` clones the SAME engine instance the store
    /// writes through, so the shipper reads the durable outbox the store's
    /// working writes append to and records the server-confirmed head back
    /// into it. Sharing is safe because every working mutator is `&self`
    /// (the engine holds its own internal write lock).
    pub fn engine_handle(&self) -> Arc<SemaDatabase> {
        Arc::clone(&self.database)
    }

    /// Write a checkpoint of the versioned log: the portable restore
    /// artifact a fresh store imports from.
    pub fn checkpoint(&self) -> Result<CheckpointReceipt, StoreError> {
        Ok(self.database.checkpoint()?)
    }

    /// The latest stored checkpoint, content-verified, when one exists.
    pub fn latest_checkpoint(&self) -> Result<Option<Checkpoint>, StoreError> {
        Ok(self.database.latest_checkpoint()?)
    }

    /// The full versioned commit log: the authoritative replayable history
    /// of every durable write since the store opted into versioning.
    pub fn versioned_log(&self) -> Result<Vec<VersionedCommitLogEntry>, StoreError> {
        Ok(self.database.versioned_commit_log()?)
    }

    /// The current versioned-log head: the `EntryDigest` of the last entry in
    /// the replayable history, or `None` when the store has never committed a
    /// versioned entry. This is the content-addressed identity of the local
    /// head `D` the criome gate authorizes BEFORE fan-out — read straight from
    /// the local log after the working commit, never from `ShipOutcome.head`
    /// (which exists only after a ship has happened).
    ///
    /// Always available, not gated behind the mirror shipper: the head is a
    /// fundamental property of the durable log, read both by the criome gate's
    /// fan-out path AND by the owner-only meta `ObserveHead` query, so an
    /// operator can read the real content head of a seeded record.
    pub fn versioned_log_head(&self) -> Result<Option<EntryDigest>, StoreError> {
        Ok(self
            .versioned_log()?
            .iter()
            .rev()
            .find(|entry| {
                entry
                    .operations()
                    .iter()
                    .any(|operation| operation.table_name() == "records")
            })
            .map(VersionedCommitLogEntry::entry_digest))
    }

    /// The current versioned-log head entry serialized as its wire BODY: the
    /// `rkyv` octets of the head `VersionedCommitLogEntry`, or `None` when the
    /// store has never committed a versioned entry. These are byte-for-byte the
    /// octets the production `mirror::ComponentShipper::envelope_for_entry`
    /// ships for this entry — the same `rkyv::to_bytes::<rancor::Error>` call —
    /// so the value the owner-only meta `ObserveHeadObject` query surfaces is
    /// exactly the body the criome-auth forward carries and the mirror lands.
    /// Re-decoding it (`rkyv::from_bytes::<VersionedCommitLogEntry>`) and
    /// reconstructing through `VersionedCommitLogEntry::new` reproduces the
    /// `versioned_log_head` digest, so the body is genuinely content-addressed,
    /// never an invented format.
    pub fn versioned_log_head_object(&self) -> Result<Option<Vec<u8>>, StoreError> {
        match self.versioned_log()?.iter().rev().find(|entry| {
            entry
                .operations()
                .iter()
                .any(|operation| operation.table_name() == "records")
        }) {
            None => Ok(None),
            Some(entry) => Ok(Some(
                rkyv::to_bytes::<rkyv::rancor::Error>(entry)
                    .map_err(|_| StoreError::ArchiveEncode)?
                    .to_vec(),
            )),
        }
    }

    /// The versioned-log suffix strictly after `sequence`, in commit order —
    /// the entries an importer ingests on top of a checkpoint.
    pub fn versioned_log_from(
        &self,
        sequence: CommitSequence,
    ) -> Result<Vec<VersionedCommitLogEntry>, StoreError> {
        Ok(self.database.versioned_replay_from_sequence(sequence)?)
    }

    /// Restore a fresh store at `path` from a checkpoint plus versioned-log
    /// suffix through the engine-owned import session. The path must hold no
    /// prior engine history; the imported log and counters land verbatim, so
    /// the restored store is indistinguishable from the original at the
    /// imported range. Family registration happens after the import, against
    /// the restored catalog.
    pub fn import(
        path: impl Into<PathBuf>,
        checkpoint: Checkpoint,
        suffix: Vec<VersionedCommitLogEntry>,
    ) -> Result<Self, StoreError> {
        let path = path.into();
        let mut database = SemaDatabase::open(
            EngineOpen::new(path.clone(), SPIRIT_SCHEMA_VERSION)
                .with_versioning(Self::versioning_policy()),
        )?;
        let mut session = database.begin_import()?;
        session.ingest_checkpoint(checkpoint)?;
        session.ingest_suffix(suffix);
        session.commit(&StoreFamilyDirectory::from_generated_families())?;
        let entries = database.register_table(RecordFamily::records_family())?;
        let migrations = database.register_table(RecordFamily::migrations_family())?;
        let configurations = database.register_table(RecordFamily::nexus_configurations_family())?;
        Ok(Self {
            database: Arc::new(database),
            entries,
            migrations,
            configurations,
            path,
            archive_target: ArchiveDatabaseTarget::Default,
            #[cfg(feature = "testing-trace")]
            trace_log: TraceLog::default(),
        })
    }

    /// Restore from a mirror restore bundle only when the bundle's restored
    /// head is the head the authorized reference announced.
    #[cfg(feature = "mirror-shipper")]
    pub fn import_mirror_restore_bundle(
        path: impl Into<PathBuf>,
        bundle: signal_mirror::RestoreBundle,
        expected_head: EntryDigest,
    ) -> Result<Self, StoreError> {
        MirrorRestoreImport::from_bundle(bundle)?.into_store(path, expected_head)
    }

    /// The typed family directory over this store's registered tables, for
    /// fold materialization (rebuild and import).
    pub fn family_directory(&self) -> StoreFamilyDirectory {
        StoreFamilyDirectory {
            entries: self.entries,
            migrations: self.migrations,
            configurations: self.configurations,
        }
    }

    /// Record the typed migration marker as an ordinary logged assert, so
    /// the migration itself is part of the replayable history.
    pub fn record_migration(&self, migration: Migration) -> Result<(), StoreError> {
        self.database
            .assert(Assertion::new(self.migrations, migration))?;
        Ok(())
    }

    /// Every recorded migration marker, oldest source version first.
    pub fn migrations(&self) -> Result<Vec<Migration>, StoreError> {
        let mut migrations = self
            .database
            .match_records(QueryPlan::all(self.migrations))?
            .records()
            .to_vec();
        migrations.sort_by_key(|migration| migration.source_schema_version.clone());
        Ok(migrations)
    }

    /// Resolve the configured archive target to a concrete `*.sema` path for
    /// the SEPARATE archive database.
    ///
    /// `Default` derives a sibling of the live database file
    /// (`<live-stem>.archive.sema`); `Path` uses the owner-supplied path
    /// verbatim. Either way the resolved path is distinct from the live
    /// database path so the archive never collides with the live log.
    fn archive_database_path(&self) -> PathBuf {
        match &self.archive_target {
            ArchiveDatabaseTarget::Default => {
                let stem = self
                    .path
                    .file_stem()
                    .map(|stem| stem.to_string_lossy().into_owned())
                    .unwrap_or_else(|| String::from("spirit"));
                self.path.with_file_name(format!("{stem}.archive.sema"))
            }
            ArchiveDatabaseTarget::Path(archive_path) => {
                PathBuf::from(&archive_path.archive_path_text)
            }
        }
    }

    #[cfg(feature = "agent-guardian")]
    fn guardian_journal_path(&self) -> PathBuf {
        let stem = self
            .path
            .file_stem()
            .map(|stem| stem.to_string_lossy().into_owned())
            .unwrap_or_else(|| String::from("spirit"));
        // Version 7 starts a fresh admission-only journal over v14 entries.
        // Older files stay on disk untouched as rollback material.
        self.path.with_file_name(format!("{stem}.guardian.v7.sema"))
    }

    #[cfg(feature = "agent-guardian")]
    fn open_guardian_journal(&self) -> Result<GuardianJournal, StoreError> {
        GuardianJournal::open(self.guardian_journal_path())
    }

    #[cfg(feature = "agent-guardian")]
    pub(crate) fn record_guardian_decision(
        &self,
        decision: GuardianDecision,
    ) -> Result<(), StoreError> {
        self.open_guardian_journal()?.append(decision)
    }

    #[cfg(feature = "agent-guardian")]
    pub fn guardian_decision_count(&self) -> Result<usize, StoreError> {
        self.open_guardian_journal()?.len()
    }

    /// Open the SEPARATE archive database at the owner-configured target. This
    /// is a distinct `sema-engine` handle over a distinct `*.sema` file; it is
    /// never the live database handle.
    fn open_archive_database(&self) -> Result<ArchiveDatabase, StoreError> {
        ArchiveDatabase::open(self.archive_database_path())
    }

    fn archive_identifier(&self, live_identifier: &str) -> String {
        format!(
            "{live_identifier}-{}",
            self.database_marker().commit_sequence
        )
    }

    pub fn import_record(
        &self,
        record_identifier: String,
        entry: Entry,
    ) -> Result<String, StoreError> {
        let record = StoredRecord::new(record_identifier.clone(), entry);
        // Upsert: overwrite an existing record in place (owner curation /
        // maintenance), insert a new one (restore into an empty store). The
        // SEMA `assert` rejects an existing key, so an existing id needs
        // `mutate` — matching the change-record update path.
        if self
            .entry_by_identifier(record_identifier.as_str())?
            .is_some()
        {
            self.database.mutate(Mutation::new(self.entries, record))?;
        } else {
            self.database.assert(Assertion::new(self.entries, record))?;
        }
        Ok(record_identifier)
    }

    fn record(&self, entry: Entry) -> Result<String, StoreError> {
        let record_identifier = self.next_record_identifier()?;
        self.import_record(record_identifier.clone(), entry)?;
        Ok(record_identifier)
    }

    pub fn propose(&self, entry: Entry) -> Result<SemaReceipt, StoreError> {
        self.record_entry(entry)
    }

    pub fn record_entry(&self, entry: Entry) -> Result<SemaReceipt, StoreError> {
        let record_identifier = self.record(entry)?;
        Ok(SemaReceipt {
            record_identifier,
            database_marker: self.database_marker(),
        })
    }

    pub fn guard_propose(
        &self,
        entry: Entry,
    ) -> Result<Result<SemaReceipt, GuardianRejection>, StoreError> {
        let Some(duplicate) = self.duplicate_record(&entry)? else {
            return Ok(Ok(self.propose(entry)?));
        };
        let record_identifier = duplicate.record_identifier.clone();
        let _importance_receipt = self
            .bump_importance(ImportanceBump {
                record_identifier: record_identifier.clone(),
            })?
            .ok_or_else(|| StoreError::DuplicateRecordVanished(record_identifier.clone()))?;
        let updated_entry = self
            .entry_by_identifier(&record_identifier)?
            .ok_or_else(|| StoreError::DuplicateRecordVanished(record_identifier.clone()))?;
        Ok(Err(GuardianRejection {
            guardian_rejection_reason: GuardianRejectionReason::Duplicate,
            record_set: vec![ObservedRecord {
                record_identifier,
                entry: updated_entry,
            }],
            explanation: "proposal duplicates an existing forward arrow".into(),
        }))
    }

    fn observe(&self, query: &Query) -> Result<Vec<ObservedRecord>, StoreError> {
        let mut records = self
            .records()?
            .into_iter()
            .filter(|record| match query {
                Query::Observe(selection)
                | Query::Count(selection)
                | Query::SubscribeIntent(selection) => {
                    EntryStoreExt::matches(&record.entry, selection)
                }
                _ => false,
            })
            .collect::<Vec<_>>();
        records.sort_by_key(|record| std::cmp::Reverse(record.entry.importance_rank()));
        Ok(records
            .into_iter()
            .map(StoredRecord::into_observed_record)
            .collect())
    }

    fn intent(&self, requested_scopes: &DomainScopes) -> Result<Vec<ObservedRecord>, StoreError> {
        let query = IntentQuery::new(requested_scopes.clone());
        let mut records = Vec::new();
        let mut seen = BTreeSet::new();
        for record in self
            .records()?
            .into_iter()
            .filter(|record| query.matches_entry(&record.entry))
        {
            if seen.insert(record.record_identifier.clone()) {
                records.push(record.into_observed_record());
            }
        }
        records.sort_by_key(|record| std::cmp::Reverse(record.entry.importance_rank()));
        Ok(records)
    }

    fn text_search(&self, search_text: &SearchText) -> Result<Vec<ObservedRecord>, StoreError> {
        let needle = TextSearchNeedle::new(search_text);
        if needle.is_empty() {
            return Ok(Vec::new());
        }

        let mut scored = self
            .records()?
            .into_iter()
            .filter_map(|record| {
                let score = needle.score_entry(&record.entry)?;
                Some((score, record))
            })
            .collect::<Vec<_>>();
        scored.sort_by_key(|(score, record)| {
            std::cmp::Reverse((*score, record.entry.importance_rank()))
        });
        Ok(scored
            .into_iter()
            .take(TEXT_SEARCH_LIMIT)
            .map(|(_score, record)| record.into_observed_record())
            .collect())
    }

    #[cfg(feature = "agent-guardian")]
    pub(crate) fn guardian_records_for_operation(
        &self,
        operation: &GuardianOperation,
    ) -> Result<RecordSet, StoreError> {
        let mut bundle = GuardianRecordBundle::new();
        for candidate in operation.candidate_entries() {
            bundle.extend(self.guardian_records_for_entry(candidate)?);
        }
        match operation {
            GuardianOperation::Clarify(clarification) => {
                if let Some(current) =
                    self.observed_record_by_identifier(&clarification.record_identifier)?
                {
                    bundle.insert(current.clone());
                    let mut candidate = current.entry;
                    candidate.description = clarification.description.clone();
                    bundle.extend(self.guardian_records_for_entry(&candidate)?);
                }
            }
            GuardianOperation::ResolveClarification(resolution) => {
                if let Some(current) =
                    self.observed_record_by_identifier(&resolution.clarification_record_identifier)?
                {
                    bundle.insert(current);
                }
                for target in &resolution.target_clarifications {
                    if let Some(current) =
                        self.observed_record_by_identifier(&target.record_identifier)?
                    {
                        bundle.insert(current.clone());
                        let mut candidate = current.entry;
                        candidate.description = target.description.clone();
                        bundle.extend(self.guardian_records_for_entry(&candidate)?);
                    }
                }
            }
            GuardianOperation::Supersede(supersession) => {
                for retired_identifier in &supersession.retired_identifiers {
                    if let Some(current) = self.observed_record_by_identifier(retired_identifier)? {
                        bundle.insert(current);
                    }
                }
            }
            GuardianOperation::Retire(retirement) => {
                if let Some(current) =
                    self.observed_record_by_identifier(&retirement.record_identifier)?
                {
                    bundle.insert(current);
                }
            }
            GuardianOperation::ChangeRecord(change) => {
                if let Some(current) =
                    self.observed_record_by_identifier(&change.record_identifier)?
                {
                    bundle.insert(current);
                }
            }
            GuardianOperation::Record(_) | GuardianOperation::Propose(_) => {}
        }
        Ok(bundle.into_record_set())
    }

    #[cfg(feature = "agent-guardian")]
    fn guardian_records_for_entry(&self, proposed: &Entry) -> Result<RecordSet, StoreError> {
        let mut bundle = GuardianRecordBundle::new();
        for scope in proposed.guardian_domain_scopes() {
            bundle.extend(self.observe(&Query::Observe(Selection::guardian_domain_scope(scope)))?);
        }
        Ok(bundle.into_record_set())
    }

    #[cfg(feature = "agent-guardian")]
    fn observed_record_by_identifier(
        &self,
        identifier: &str,
    ) -> Result<Option<ObservedRecord>, StoreError> {
        Ok(self
            .database
            .match_records(QueryPlan::key(self.entries, RecordKey::new(identifier)))?
            .records()
            .iter()
            .next()
            .cloned()
            .map(StoredRecord::into_observed_record))
    }

    fn duplicate_record(&self, proposed: &Entry) -> Result<Option<StoredRecord>, StoreError> {
        Ok(self.records()?.into_iter().find(|record| {
            record.entry.kind == proposed.kind
                && record.entry.domains == proposed.domains
                && record
                    .entry
                    .description
                    .trim()
                    .eq_ignore_ascii_case(proposed.description.trim())
        }))
    }

    #[cfg(feature = "agent-guardian")]
    pub(crate) fn apply_duplicate_guardian_rejection(
        &self,
        entry: &Entry,
        fallback: GuardianRejection,
    ) -> Result<GuardianRejection, StoreError> {
        let Some(duplicate) = self.duplicate_record(entry)? else {
            return Ok(fallback);
        };
        let record_identifier = duplicate.record_identifier.clone();
        let _importance_receipt = self
            .bump_importance(ImportanceBump {
                record_identifier: record_identifier.clone(),
            })?
            .ok_or_else(|| StoreError::DuplicateRecordVanished(record_identifier.clone()))?;
        let updated_entry = self
            .entry_by_identifier(&record_identifier)?
            .ok_or_else(|| StoreError::DuplicateRecordVanished(record_identifier.clone()))?;
        Ok(GuardianRejection {
            guardian_rejection_reason: GuardianRejectionReason::Duplicate,
            record_set: vec![ObservedRecord {
                record_identifier,
                entry: updated_entry,
            }],
            explanation: "guardian judged the write as a duplicate".into(),
        })
    }

    pub fn entry_by_identifier(&self, identifier: &str) -> Result<Option<Entry>, StoreError> {
        Ok(self
            .database
            .match_records(QueryPlan::key(self.entries, RecordKey::new(identifier)))?
            .records()
            .iter()
            .next()
            .map(StoredRecord::entry))
    }

    fn count(&self, query: &Query) -> Result<u64, StoreError> {
        Ok(self.observe(query)?.len() as u64)
    }

    fn remove(&self, identifier: &str) -> Result<bool, StoreError> {
        match self
            .database
            .retract(Retraction::new(self.entries, RecordKey::new(identifier)))
        {
            Ok(_receipt) => Ok(true),
            Err(sema_engine::Error::RecordNotFound { .. }) => Ok(false),
            Err(engine_error) => Err(StoreError::Database { engine_error }),
        }
    }

    pub fn retire(&self, retirement: Retirement) -> Result<Option<RetirementReceipt>, StoreError> {
        let record_identifier = retirement.record_identifier;
        let Some(entry) = self.entry_by_identifier(&record_identifier)? else {
            return Ok(None);
        };
        let mut archive = self.open_archive_database()?;
        archive.archive_record(
            StoredRecord::new(record_identifier.clone(), entry),
            self.archive_identifier(&record_identifier),
        )?;
        if !self.remove(&record_identifier)? {
            return Ok(None);
        }
        Ok(Some(RetirementReceipt { record_identifier }))
    }

    fn bump_importance(
        &self,
        change: ImportanceBump,
    ) -> Result<Option<ImportanceBumpReceipt>, StoreError> {
        let record_identifier = change.record_identifier;
        let identifier_text = record_identifier.clone();
        let Some(mut entry) = self.entry_by_identifier(&record_identifier)? else {
            return Ok(None);
        };
        entry.importance = MagnitudeStoreExt::next(&entry.importance);
        let importance = entry.importance.clone();
        self.database.mutate(Mutation::new(
            self.entries,
            StoredRecord::new(identifier_text, entry),
        ))?;
        Ok(Some(ImportanceBumpReceipt {
            record_identifier,
            importance,
        }))
    }

    pub(crate) fn change_record(
        &self,
        change: RecordChange,
    ) -> Result<Option<RecordChangeReceipt>, StoreError> {
        let record_identifier = change.record_identifier;
        let identifier_text = record_identifier.clone();
        if self.entry_by_identifier(&record_identifier)?.is_none() {
            return Ok(None);
        }
        let entry = change.entry;
        self.database.mutate(Mutation::new(
            self.entries,
            StoredRecord::new(identifier_text, entry),
        ))?;
        Ok(Some(RecordChangeReceipt { record_identifier }))
    }

    pub fn clarify(
        &self,
        clarification: ClarificationRequest,
    ) -> Result<Option<ClarificationReceipt>, StoreError> {
        let record_identifier = clarification.record_identifier;
        let identifier_text = record_identifier.clone();
        let Some(mut entry) = self.entry_by_identifier(&record_identifier)? else {
            return Ok(None);
        };
        let mut archive = self.open_archive_database()?;
        archive.archive_record(
            StoredRecord::new(identifier_text.clone(), entry.clone()),
            self.archive_identifier(&record_identifier),
        )?;
        entry.description = clarification.description;
        self.database.mutate(Mutation::new(
            self.entries,
            StoredRecord::new(identifier_text, entry),
        ))?;
        Ok(Some(ClarificationReceipt { record_identifier }))
    }

    pub fn resolve_clarification(
        &self,
        resolution: ClarificationResolution,
    ) -> Result<Option<ClarificationResolutionReceipt>, StoreError> {
        let ClarificationResolution {
            clarification_record_identifier,
            target_clarifications,
            justification: _justification,
        } = resolution;
        let clarification_identifier = clarification_record_identifier.clone();
        if self
            .entry_by_identifier(&clarification_identifier)?
            .is_none()
        {
            return Ok(None);
        }

        let mut snapshots = Vec::new();
        for target in target_clarifications {
            let identifier_text = target.record_identifier.clone();
            let Some(entry) = self.entry_by_identifier(&identifier_text)? else {
                return Ok(None);
            };
            snapshots.push((
                target.record_identifier.clone(),
                identifier_text,
                entry,
                target.description.clone(),
            ));
        }

        let mut archive = self.open_archive_database()?;
        for (_record_identifier, identifier_text, entry, _description) in &snapshots {
            archive.archive_record(
                StoredRecord::new(identifier_text.clone(), entry.clone()),
                self.archive_identifier(identifier_text),
            )?;
        }

        let mut record_identifiers = Vec::new();
        for (record_identifier, identifier_text, mut entry, description) in snapshots {
            entry.description = description;
            self.database.mutate(Mutation::new(
                self.entries,
                StoredRecord::new(identifier_text, entry),
            ))?;
            record_identifiers.push(record_identifier);
        }

        if !self.remove(&clarification_identifier)? {
            return Ok(None);
        }

        Ok(Some(ClarificationResolutionReceipt {
            clarification_record_identifier: clarification_identifier,
            record_identifiers,
        }))
    }

    pub fn supersede(
        &self,
        supersession: Supersession,
    ) -> Result<Option<SupersessionReceipt>, StoreError> {
        let retired_identifiers = supersession.retired_identifiers;
        let replacements = supersession.replacements;
        // Snapshot every retired target up front, BEFORE any mutation, so a
        // missing target is a clean no-op rather than a partial write.
        let mut snapshots = Vec::new();
        for identifier in &retired_identifiers {
            let identifier_text = identifier.clone();
            let Some(entry) = self.entry_by_identifier(&identifier_text)? else {
                return Ok(None);
            };
            snapshots.push((identifier_text, entry));
        }
        // Propose every replacement FIRST. If any propose fails here, the
        // retired records are still intact and the caller can safely retry — this
        // ordering is what prevents a partial supersede from destroying intent
        // (a removed retired record with no replacement). A residual leak of an
        // already-committed replacement on a later propose failure is recoverable
        // (an extra record), never a loss; a single sema-engine WriteTransaction
        // spanning the whole supersede is the eventual end state.
        let mut record_identifiers = Vec::new();
        for replacement in replacements {
            let sema_receipt = self.propose(replacement)?;
            record_identifiers.push(sema_receipt.record_identifier);
        }
        // Only once every replacement is committed do we archive and remove the
        // retired set.
        let mut archive = self.open_archive_database()?;
        for (identifier_text, entry) in &snapshots {
            archive.archive_record(
                StoredRecord::new(identifier_text.clone(), entry.clone()),
                self.archive_identifier(identifier_text),
            )?;
        }
        for (identifier_text, _entry) in &snapshots {
            self.remove(identifier_text)?;
        }
        Ok(Some(SupersessionReceipt {
            retired_identifiers,
            record_identifiers,
        }))
    }

    pub fn len(&self) -> usize {
        self.committed_record_count().unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn committed_record_count(&self) -> Result<usize, StoreError> {
        Ok(self.records()?.len())
    }

    /// The SEMA commit marker: the persisted commit sequence plus a real
    /// content hash of the committed records.
    pub fn database_marker(&self) -> DatabaseMarker {
        DatabaseMarker {
            commit_sequence: i64::try_from(self.commit_sequence().unwrap_or(0))
                .expect("commit sequence fits i64"),
            state_digest: wire_state_digest(self.state_digest().unwrap_or(0)),
        }
    }

    fn commit_sequence(&self) -> Result<u64, StoreError> {
        Ok(self.database.current_commit_sequence()?.value())
    }

    /// A content-addressed digest of committed state: blake3 over each
    /// record's `(identifier, archived bytes)`, folded with the commit
    /// sequence, reduced to the schema's `Integer` digest width. An empty
    /// store (no committed records) digests to zero, so a marker taken
    /// before any write reads `(0, 0)`.
    fn state_digest(&self) -> Result<u64, StoreError> {
        let records = self.records()?;
        if records.is_empty() {
            return Ok(0);
        }
        let commit_sequence = self.commit_sequence()?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(&commit_sequence.to_le_bytes());
        for record in records {
            let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&record)
                .map_err(|_| StoreError::ArchiveEncode)?;
            hasher.update(record.record_identifier.as_bytes());
            hasher.update(&archive);
        }
        let digest = hasher.finalize();
        let mut head = [0_u8; 8];
        head.copy_from_slice(&digest.as_bytes()[..8]);
        Ok(u64::from_le_bytes(head))
    }

    fn records(&self) -> Result<Vec<StoredRecord>, StoreError> {
        Ok(self
            .database
            .match_records(QueryPlan::all(self.entries))?
            .records()
            .to_vec())
    }

    fn next_record_identifier(&self) -> Result<String, StoreError> {
        RecordIdentifierMint::from_records(&self.records()?).next_identifier()
    }
}

impl From<SpiritNexusConfiguration> for StoredNexusConfiguration {
    fn from(configuration: SpiritNexusConfiguration) -> Self {
        Self {
            state: nexus::ConfigurationState::from_default(configuration),
        }
    }
}

impl EngineRecord for StoredNexusConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("nexus-configuration")
    }
}

impl StoredRecord {
    pub(super) fn new(record_identifier: String, entry: Entry) -> Self {
        Self {
            record_identifier,
            entry,
        }
    }

    fn into_observed_record(self) -> ObservedRecord {
        ObservedRecord {
            record_identifier: self.record_identifier,
            entry: self.entry,
        }
    }

    fn entry(&self) -> Entry {
        self.entry.clone()
    }
}

impl EngineRecord for StoredRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.record_identifier.clone())
    }
}

#[cfg(feature = "agent-guardian")]
pub trait GuardianEntryExt {
    fn guardian_domain_scopes(&self) -> DomainScopes;
}

#[cfg(feature = "agent-guardian")]
impl GuardianEntryExt for Entry {
    fn guardian_domain_scopes(&self) -> DomainScopes {
        self.domains
            .iter()
            .cloned()
            .map(|domain| DomainScope { domain })
            .collect()
    }
}

#[cfg(feature = "agent-guardian")]
pub trait GuardianQueryExt {
    fn guardian_domain_scope(scope: DomainScope) -> Self;
    fn guardian_context(domain_match: DomainMatch) -> Self;
}

#[cfg(feature = "agent-guardian")]
impl GuardianQueryExt for crate::schema::signal::Selection {
    fn guardian_domain_scope(scope: DomainScope) -> Self {
        Self::guardian_context(DomainMatch::Full(vec![scope]))
    }

    fn guardian_context(domain_match: DomainMatch) -> Self {
        Self {
            domain_match,
            keyword_match: KeywordMatch::Any,
            text_match: TextMatch::Any,
            selected_kind: None,
            importance_selection: ImportanceSelection::default_observation_importance(),
        }
    }
}

#[derive(Clone, Debug)]
struct IntentQuery {
    requested_scopes: DomainScopes,
}

impl IntentQuery {
    fn new(requested_scopes: DomainScopes) -> Self {
        Self { requested_scopes }
    }

    fn matches_entry(&self, entry: &Entry) -> bool {
        self.requested_scopes
            .iter()
            .any(|requested_scope| self.matches_requested_scope(entry, requested_scope))
    }

    fn matches_requested_scope(&self, entry: &Entry, requested_scope: &DomainScope) -> bool {
        entry
            .domains
            .iter()
            .any(|record_domain| domain_scope_matches(&requested_scope.domain, record_domain))
    }
}

struct TextSearchNeedle {
    words: Vec<String>,
    empty: bool,
}

impl TextSearchNeedle {
    fn new(search_text: &SearchText) -> Self {
        let words = Self::normalized_words(search_text);
        let empty = words.is_empty();
        Self { words, empty }
    }

    fn is_empty(&self) -> bool {
        self.empty
    }

    fn normalized_words(search_text: &str) -> Vec<String> {
        search_text
            .split(|character: char| !character.is_alphanumeric())
            .filter(|word| !word.is_empty())
            .map(str::to_lowercase)
            .collect()
    }

    fn score_entry(&self, entry: &Entry) -> Option<u64> {
        let score = self.score_description(&entry.description);
        (score > 0).then_some(score)
    }

    fn score_description(&self, description: &Description) -> u64 {
        self.score_text(description, 100, 10)
    }

    fn score_text(&self, text: &str, phrase_score: u64, word_score: u64) -> u64 {
        let normalized = text.to_lowercase();
        let normalized_words = Self::normalized_words(text);
        let mut score = 0;
        if !self.words.is_empty() && normalized.contains(&self.words.join(" ")) {
            score += phrase_score;
        }
        for word in &self.words {
            if normalized_words.iter().any(|candidate| candidate == word) {
                score += word_score;
            }
        }
        score
    }
}

pub trait DomainStoreExt {
    fn to_signal_domain(&self) -> Option<signal_domain::Domain>;
}

impl DomainStoreExt for Domain {
    fn to_signal_domain(&self) -> Option<signal_domain::Domain> {
        match self {
            Domain::All => None,
            Domain::Health(payload) => Some(signal_domain::Domain::Health(payload.clone())),
            Domain::Food(payload) => Some(signal_domain::Domain::Food(payload.clone())),
            Domain::Home(payload) => Some(signal_domain::Domain::Home(payload.clone())),
            Domain::Finance(payload) => Some(signal_domain::Domain::Finance(payload.clone())),
            Domain::Work(payload) => Some(signal_domain::Domain::Work(payload.clone())),
            Domain::Craft(payload) => Some(signal_domain::Domain::Craft(payload.clone())),
            Domain::Knowledge(payload) => Some(signal_domain::Domain::Knowledge(payload.clone())),
            Domain::Education(payload) => Some(signal_domain::Domain::Education(payload.clone())),
            Domain::Language(payload) => Some(signal_domain::Domain::Language(payload.clone())),
            Domain::Art(payload) => Some(signal_domain::Domain::Art(payload.clone())),
            Domain::Kinship(payload) => Some(signal_domain::Domain::Kinship(payload.clone())),
            Domain::Selfhood(payload) => Some(signal_domain::Domain::Selfhood(payload.clone())),
            Domain::Spirituality(payload) => {
                Some(signal_domain::Domain::Spirituality(payload.clone()))
            }
            Domain::Governance(payload) => Some(signal_domain::Domain::Governance(payload.clone())),
            Domain::Law(payload) => Some(signal_domain::Domain::Law(payload.clone())),
            Domain::Community(payload) => Some(signal_domain::Domain::Community(payload.clone())),
            Domain::Nature(payload) => Some(signal_domain::Domain::Nature(payload.clone())),
            Domain::Travel(payload) => Some(signal_domain::Domain::Travel(payload.clone())),
            Domain::Commerce(payload) => Some(signal_domain::Domain::Commerce(payload.clone())),
            Domain::Leisure(payload) => Some(signal_domain::Domain::Leisure(payload.clone())),
            Domain::Appearance(payload) => Some(signal_domain::Domain::Appearance(payload.clone())),
            Domain::Safety(payload) => Some(signal_domain::Domain::Safety(payload.clone())),
            Domain::Information(payload) => {
                Some(signal_domain::Domain::Information(payload.clone()))
            }
            Domain::Technology(payload) => Some(signal_domain::Domain::Technology(payload.clone())),
        }
    }
}

// The old `DomainScopeStoreExt` (scope→domain round-trip plus a hand-rolled
// prefix match) is retired: the shared `signal-domain` contract owns the
// scope-matching surface (`DomainScope::matches_domain` / `matches_scope`,
// equivalence expansion through `DomainScope::expand`, and
// `DomainScopes::matches_any_domain`), and spirit consumes it directly.

/// State digests are opaque 64-bit hash identities.  The signal contract's
/// signed integer field carries the same 64 bits; it is never used for
/// arithmetic or ordering.
fn wire_state_digest(digest: u64) -> i64 {
    i64::from_le_bytes(digest.to_le_bytes())
}

/// Domain matching retains the taxonomy-wide `All` semantics from the prior
/// domain projection.  A stored data `All` leaf is the parent scope for every
/// concrete data leaf; exact domains still require equality.
fn domain_scope_matches(requested: &Domain, recorded: &Domain) -> bool {
    requested == recorded
        || matches!(
            recorded,
            Domain::All
                | Domain::Technology(TechnologyDomain::Software(SoftwareDomain::Data(
                    DataLeaf::All
                )))
        )
}

pub trait EntryStoreExt {
    fn matches(&self, query: &crate::schema::signal::Selection) -> bool;
    fn matches_domain_match(&self, domain_match: &DomainMatch) -> bool;
    fn importance_rank(&self) -> u64;
}

impl EntryStoreExt for Entry {
    fn matches(&self, query: &crate::schema::signal::Selection) -> bool {
        self.matches_domain_match(&query.domain_match)
            && query.keyword_match.matches(&self.description)
            && query.text_match.matches(&self.description)
            && query
                .selected_kind
                .as_ref()
                .is_none_or(|kind| &self.kind == kind)
            && query.importance_selection.matches(&self.importance)
    }

    fn matches_domain_match(&self, domain_match: &DomainMatch) -> bool {
        if self.domains.contains(&Domain::All) {
            return match domain_match {
                DomainMatch::Any => true,
                DomainMatch::Partial(scopes) => !scopes.is_empty(),
                DomainMatch::Full(scopes) => !scopes.is_empty(),
            };
        }
        match domain_match {
            DomainMatch::Any => true,
            DomainMatch::Partial(scopes) => scopes.iter().any(|scope| {
                self.domains
                    .iter()
                    .any(|domain| domain_scope_matches(&scope.domain, domain))
            }),
            DomainMatch::Full(scopes) => scopes.iter().all(|scope| {
                self.domains
                    .iter()
                    .any(|domain| domain_scope_matches(&scope.domain, domain))
            }),
        }
    }

    fn importance_rank(&self) -> u64 {
        self.importance.rank()
    }
}

pub trait DescriptionStoreExt {
    fn keywords(&self) -> Keywords;
    fn contains_search_text(&self, search_text: &SearchText) -> bool;
    #[cfg(feature = "agent-guardian")]
    fn contains_description_text(&self, other: &Description) -> bool;
}

impl DescriptionStoreExt for Description {
    fn keywords(&self) -> Keywords {
        let mut keywords = Vec::new();
        let mut seen = BTreeSet::new();
        let mut inside_keyword = false;
        let mut keyword = String::new();
        for character in self.chars() {
            if character == '*' {
                if inside_keyword {
                    let normalized = keyword.trim().to_lowercase();
                    if !normalized.is_empty() && seen.insert(normalized.clone()) {
                        keywords.push(normalized);
                    }
                    keyword.clear();
                    inside_keyword = false;
                } else {
                    keyword.clear();
                    inside_keyword = true;
                }
            } else if inside_keyword {
                keyword.push(character);
            }
        }
        keywords
    }

    fn contains_search_text(&self, search_text: &SearchText) -> bool {
        self.to_lowercase()
            .contains(&search_text.trim().to_lowercase())
    }

    #[cfg(feature = "agent-guardian")]
    fn contains_description_text(&self, other: &Description) -> bool {
        let other = other.trim();
        !other.is_empty() && self.contains_search_text(&other.to_owned())
    }
}

pub trait KeywordStoreExt {
    fn normalized(&self) -> String;
}

impl KeywordStoreExt for Keyword {
    fn normalized(&self) -> String {
        self.trim().to_lowercase()
    }
}

pub trait KeywordsStoreExt {
    fn contains_keyword(&self, expected: &Keyword) -> bool;
    fn contains_any(&self, expected: &Keywords) -> bool;
    fn contains_all(&self, expected: &Keywords) -> bool;
}

impl KeywordsStoreExt for Keywords {
    fn contains_keyword(&self, expected: &Keyword) -> bool {
        let expected = expected.normalized();
        self.iter().any(|keyword| keyword.normalized() == expected)
    }

    fn contains_any(&self, expected: &Keywords) -> bool {
        expected
            .iter()
            .any(|keyword| self.contains_keyword(keyword))
    }

    fn contains_all(&self, expected: &Keywords) -> bool {
        expected
            .iter()
            .all(|keyword| self.contains_keyword(keyword))
    }
}

pub trait QueryStoreExt {
    fn matches(&self, entry: &Entry) -> bool;
}

impl QueryStoreExt for crate::schema::signal::Selection {
    fn matches(&self, entry: &Entry) -> bool {
        EntryStoreExt::matches(entry, self)
    }
}

pub trait KeywordMatchStoreExt {
    fn matches(&self, description: &Description) -> bool;
}

impl KeywordMatchStoreExt for KeywordMatch {
    fn matches(&self, description: &Description) -> bool {
        match self {
            Self::Any => true,
            Self::AnyKeyword(expected) => description.keywords().contains_any(expected),
            Self::AllKeywords(expected) => description.keywords().contains_all(expected),
        }
    }
}

pub trait TextMatchStoreExt {
    fn matches(&self, description: &Description) -> bool;
}

impl TextMatchStoreExt for TextMatch {
    fn matches(&self, description: &Description) -> bool {
        match self {
            Self::Any => true,
            Self::ContainsText(search_text) => description.contains_search_text(search_text),
        }
    }
}

pub trait ImportanceSelectionStoreExt {
    fn default_observation_importance() -> Self;
    fn matches(&self, importance: &Importance) -> bool;
}

impl ImportanceSelectionStoreExt for ImportanceSelection {
    fn default_observation_importance() -> Self {
        Self::Any
    }

    fn matches(&self, importance: &Importance) -> bool {
        match self {
            Self::Any => true,
            Self::ExactImportance(expected) => importance == expected,
            Self::AtMostImportance(maximum) => importance.rank() <= maximum.rank(),
            Self::AtLeastImportance(minimum) => importance.rank() >= minimum.rank(),
        }
    }
}

pub trait ImportanceStoreExt {
    fn next(&self) -> Self;
}

impl ImportanceStoreExt for Importance {
    fn next(&self) -> Self {
        MagnitudeStoreExt::next(self)
    }
}

pub trait MagnitudeStoreExt {
    fn rank(&self) -> u64;
    fn next(&self) -> Self;
}

impl MagnitudeStoreExt for Magnitude {
    fn rank(&self) -> u64 {
        match self {
            Self::Zero => 0,
            Self::Minimum => 1,
            Self::VeryLow => 2,
            Self::Low => 3,
            Self::Medium => 4,
            Self::High => 5,
            Self::VeryHigh => 6,
            Self::Maximum => 7,
        }
    }

    fn next(&self) -> Self {
        match self {
            Self::Zero => Self::Minimum,
            Self::Minimum => Self::VeryLow,
            Self::VeryLow => Self::Low,
            Self::Low => Self::Medium,
            Self::Medium => Self::High,
            Self::High => Self::VeryHigh,
            Self::VeryHigh | Self::Maximum => Self::Maximum,
        }
    }
}

#[cfg(test)]
mod compatibility_tests {
    use super::{TextSearchNeedle, domain_scope_matches, wire_state_digest};
    use signal_domain::{DataLeaf, Domain, SoftwareDomain, TechnologyDomain};

    #[test]
    fn state_digest_preserves_high_bit_hash_identity() {
        let digest = u64::MAX - 17;
        assert_eq!(
            wire_state_digest(digest).to_le_bytes(),
            digest.to_le_bytes()
        );
    }

    #[test]
    fn data_all_is_a_parent_scope_for_concrete_data() {
        let all = Domain::Technology(TechnologyDomain::Software(SoftwareDomain::Data(
            DataLeaf::All,
        )));
        let concrete = Domain::Technology(TechnologyDomain::Software(SoftwareDomain::Data(
            DataLeaf::Persistence,
        )));
        assert!(domain_scope_matches(&concrete, &all));
        assert!(!domain_scope_matches(&all, &concrete));
    }

    #[test]
    fn text_search_normalizes_unicode_case_and_punctuation() {
        let needle = TextSearchNeedle::new(&"CAFÉ—Routíng".into());
        assert_eq!(needle.words, vec!["café", "routíng"]);
        assert_eq!(needle.score_text("The café routíng guide", 100, 10), 120);
    }

    #[test]
    fn text_search_rewards_phrase_then_each_matching_word() {
        let needle = TextSearchNeedle::new(&"routing protocol".into());
        assert_eq!(
            needle.score_text("routing protocol architecture", 100, 10),
            120
        );
        assert_eq!(needle.score_text("protocol before routing", 100, 10), 20);
    }

    #[test]
    fn text_search_rejects_empty_or_punctuation_only_needles() {
        assert!(TextSearchNeedle::new(&"   ".into()).is_empty());
        assert!(TextSearchNeedle::new(&"---".into()).is_empty());
    }
}
