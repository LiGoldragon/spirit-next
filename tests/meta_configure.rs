//! Owner-only Configure behavior through the final typed meta Signal boundary.
mod support;

#[cfg(not(feature = "agent-guardian"))]
use spirit::schema::signal::{DomainMatch, ImportanceSelection, Selection};
use spirit::schema::{
    meta_signal::{
        ArchiveDatabaseTarget, ArchivePath, ConfigureRequest, Query as MetaQuery,
        Response as MetaResponse,
    },
    signal::{
        Entry, Justification, Kind, Magnitude, Query, RecordRequest, Response, VerbatimQuote,
    },
};
use spirit::{
    Configuration, Daemon, DaemonError, MetaSignalTransport, SignalTransport, SpiritDaemon,
};
use std::{
    os::unix::fs::PermissionsExt,
    path::Path,
    thread,
    time::{Duration, Instant},
};
use support::domain_fixtures;
use tempfile::TempDir;

struct DaemonThread {
    handle: Option<thread::JoinHandle<()>>,
}
impl DaemonThread {
    fn spawn(configuration: Configuration) -> Self {
        Self {
            handle: Some(thread::spawn(move || {
                Daemon::new(configuration).run().expect("daemon run")
            })),
        }
    }
}
impl Drop for DaemonThread {
    fn drop(&mut self) {
        drop(self.handle.take());
    }
}
fn wait_for_socket(path: &Path) {
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(5) {
        if path.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }
    panic!("socket did not appear at {}", path.display());
}
fn configure_request(target: ArchiveDatabaseTarget) -> ConfigureRequest {
    ConfigureRequest {
        spirit_nexus_configuration: Configuration::default_nexus_configuration(),
        archive_database_target: target,
        selected_mirror_target: None,
        selected_criome_gate_target: None,
        selected_guardian_prompt_target: None,
    }
}
fn record_query(description: &str) -> Query {
    Query::Record(RecordRequest {
        entry: Entry {
            domains: domain_fixtures::domains(&["meta-configure"]),
            kind: Kind::Decision,
            description: description.into(),
            importance: Magnitude::Minimum,
        },
        justification: Justification {
            testimony: vec![VerbatimQuote {
                quote_text: description.into(),
                optional_antecedent: None,
            }],
            reasoning: description.into(),
        },
    })
}
#[cfg(not(feature = "agent-guardian"))]
fn observe_query() -> Query {
    Query::Observe(Selection {
        domain_match: DomainMatch::Full(domain_fixtures::scopes(&["meta-configure"])),
        keyword_match: spirit::schema::signal::KeywordMatch::Any,
        text_match: spirit::schema::signal::TextMatch::Any,
        selected_kind: Some(Kind::Decision),
        importance_selection: ImportanceSelection::Any,
    })
}

#[test]
fn configure_sets_archive_target_and_leaves_live_database_unchanged() {
    let temp = TempDir::new().expect("tempdir");
    let working = temp.path().join("spirit.sock");
    let meta = temp.path().join("spirit-meta.sock");
    let live = temp.path().join("live.sema");
    let archive = temp.path().join("archive.sema");
    let _daemon =
        DaemonThread::spawn(Configuration::new(&working, &live).with_meta_socket_path(&meta));
    wait_for_socket(&working);
    wait_for_socket(&meta);
    let target = ArchiveDatabaseTarget::Path(ArchivePath {
        archive_path_text: archive.to_string_lossy().into_owned(),
    });
    let reply = MetaSignalTransport::connect(&meta)
        .expect("connect meta")
        .configure(configure_request(target.clone()))
        .expect("configure");
    match reply {
        MetaResponse::Configured(receipt) => assert_eq!(receipt.archive_database_target, target),
        other => panic!("configure failed: {other:?}"),
    }
    let record = SignalTransport::connect(&working)
        .expect("connect working")
        .exchange(&record_query("intent after configure"))
        .expect("record");
    #[cfg(not(feature = "agent-guardian"))]
    {
        assert!(matches!(record, Response::RecordAccepted(_)));
        let observed = SignalTransport::connect(&working)
            .expect("reconnect working")
            .exchange(&observe_query())
            .expect("observe");
        let Response::RecordsStashed(stashed) = observed else {
            panic!("expected live observation: {observed:?}")
        };
        assert_eq!(stashed.record_count, 1);
    }
    #[cfg(feature = "agent-guardian")]
    assert!(matches!(record, Response::GuardianRejected(_)));
    assert!(live.exists());
    assert!(!archive.exists());
}
#[test]
fn meta_socket_carries_owner_only_mode() {
    let temp = TempDir::new().expect("tempdir");
    let working = temp.path().join("spirit.sock");
    let meta = temp.path().join("spirit-meta.sock");
    let db = temp.path().join("intent.sema");
    let _daemon =
        DaemonThread::spawn(Configuration::new(&working, &db).with_meta_socket_path(&meta));
    wait_for_socket(&meta);
    assert_eq!(
        std::fs::metadata(&meta)
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}
#[test]
fn working_socket_rejects_meta_configure_bytes() {
    let temp = TempDir::new().expect("tempdir");
    let working = temp.path().join("spirit.sock");
    let meta = temp.path().join("spirit-meta.sock");
    let db = temp.path().join("intent.sema");
    let _daemon =
        DaemonThread::spawn(Configuration::new(&working, &db).with_meta_socket_path(&meta));
    wait_for_socket(&working);
    let target = ArchiveDatabaseTarget::Path(ArchivePath {
        archive_path_text: db.to_string_lossy().into_owned(),
    });
    assert!(
        MetaSignalTransport::connect(&working)
            .expect("connect")
            .exchange(&MetaQuery::Configure(configure_request(target)))
            .is_err()
    );
}
#[test]
fn daemon_rejects_missing_meta_socket_before_serving() {
    let temp = TempDir::new().expect("tempdir");
    let configuration = Configuration::new(
        temp.path().join("spirit.sock"),
        temp.path().join("intent.sema"),
    );
    assert!(matches!(
        Daemon::new(configuration).run(),
        Err(DaemonError::<SpiritDaemon>::MissingMetaSocket)
    ));
}
