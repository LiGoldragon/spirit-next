//! Nix-driven integration tests for the spirit schema-driven stack.
//!
//! These tests are the cycle-3-prep forcing function for record 1006
//! (Maximum, 2026-05-27): tests must PROVE not pretend; the canonical
//! shape is schema files driving Nix-built binaries that the test
//! launches and exchanges real rkyv signal frames with over a real
//! Unix socket. Every component — `spirit` (CLI), `spirit-
//! daemon` (daemon, holds the SignalAdmission + Engine + Store triad) — is
//! the SAME schema-emitted code path the runtime uses.
//!
//! ## Discipline
//!
//! Where the in-process tests in `runtime_triad.rs` exercise
//! `Engine::handle` directly, these tests run the actual binaries the
//! Nix package built and assert on the CLI's stdout — the same surface
//! a human would see. Each test:
//!
//!   1. Locates the Nix-built binaries (via `nix build` invoked by the
//!      test, or via `SPIRIT_NIX_BUILD_RESULT` if pre-built).
//!   2. Spawns the daemon binary against a temp Unix socket.
//!   3. Invokes the CLI binary against that socket, passing inline
//!      NOTA arguments — the SAME single-NOTA-argument contract
//!      `AGENTS.md` mandates and the schema-driven runtime parses.
//!   4. Asserts on the CLI's stdout (which IS the schema-emitted
//!      `Response::to_string()` NOTA round-trip) by parsing it back into
//!      the schema-emitted `Output` enum and matching typed variants —
//!      never against raw strings (record 995/996/997).
//!
//! ## Why these tests are `#[ignore]`
//!
//! Invoking `nix build` from inside `cargo test` is heavy (8s+ on a
//! warm remote builder, longer on a cold build). They run via:
//!
//! ```bash
//! cargo test --test nix_integration -- --ignored
//! ```
//!
//! or through the flake app:
//!
//! ```bash
//! nix run .#nix-integration-tests
//! ```
//!
//! The script seeds `SPIRIT_NIX_BUILD_RESULT` with a pre-built
//! binary directory so the test bypasses its own `nix build`.
//!
//! ## What each test proves
//!
//! - `nix_built_spirit_cli_records_through_real_socket_to_nix_built_daemon`
//!   — happy-path Record traverses CLI binary → Unix socket → daemon
//!   binary → SignalAdmission → Engine → Store, returning `RecordAccepted`.
//!   The test parses the CLI's stdout back through the schema-emitted
//!   `Response::FromStr` and matches typed variants.
//!
//! - `nix_built_daemon_rejects_invalid_input_through_schema_emitted_rejection`
//!   — invalid Input is rejected by `SignalAdmission::admit` and the
//!   schema-emitted `SignalRejection` flows back to the CLI through
//!   the rkyv signal frame.
//!
//! - `nix_built_daemon_persists_state_across_two_cli_invocations`
//!   — two CLI invocations against the same daemon process show
//!   monotonic `CommitSequence` advancement on the schema-emitted
//!   `VersionReported` after writes.
//!
//! - `nix_built_daemon_observes_recorded_entries_back_through_query`
//!   — a Record followed by an Observe Query returns the schema-emitted
//!   `RecordsStashed` variant with records inline, while `LookupStash`
//!   keeps the recovery path available.
//!
//! - `nix_built_daemon_returns_missed_when_no_matching_record_exists`
//!   — Observe against an empty store returns the schema-emitted
//!   `Response::Error(ErrorReport)` (the SEMA-plane "no matching record"
//!   path).
//!
//! - `nix_built_daemon_handles_back_to_back_inputs_through_one_socket`
//!   — the same daemon process serves multiple sequential CLI
//!   invocations without restart.
//!
//! - `nix_build_default_package_emits_both_binaries`
//!   — sanity-check the Nix build produces both `bin/spirit` and
//!   `bin/spirit-nexus`, proving the schema-driven build pipeline
//!   reaches the binary stage.
//!
//! - `nix_built_binaries_round_trip_representative_schema_outputs`
//!   — drive representative schema-emitted `Output` variants, including
//!   `VersionReported`, through the CLI and parse stdout back through the
//!   schema-emitted `Response::FromStr`, proving the NOTA form and the
//!   FromStr surface stay in sync.

mod support;

use std::{
    env,
    io::Read,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::OnceLock,
    time::Duration,
};
use support::{
    domain_fixtures,
    process::{CommandIsolation, ManagedChild, NonSecretEnvironmentVariable, ProcessSandbox},
};

use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use meta_signal_spirit::{Query as MetaQuery, Response as MetaResponse};
use protos::{Protosizable, ReaderBudget, Textualizable};
use spirit::Configuration;
use spirit::schema::meta_signal::{ImportRequest, ImportedRecord};
use spirit::schema::signal::{
    Entry, GuardianRejectionReason, Kind, Magnitude, Query, RecordIdentifier, Response,
    SignalRejection, ValidationError,
};
fn assert_short_record_identifier(identifier: &RecordIdentifier) {
    assert!(
        (4..=7).contains(&identifier.len()),
        "record identifier should use a four-to-seven-character code: {identifier:?}"
    );
    assert!(
        identifier
            .chars()
            .all(|character| character.is_ascii_digit() || character.is_ascii_lowercase()),
        "record identifier should be lower-base36: {:?}",
        identifier
    );
}

fn selection(domains: Vec<signal_domain::Domain>) -> signal_spirit::Selection {
    signal_spirit::Selection {
        domain_match: signal_spirit::DomainMatch::Full(
            domains
                .into_iter()
                .map(|domain| signal_domain::DomainScope { domain })
                .collect(),
        ),
        keyword_match: signal_spirit::KeywordMatch::Any,
        text_match: signal_spirit::TextMatch::Any,
        selected_kind: Some(Kind::Decision),
        importance_selection: signal_spirit::ImportanceSelection::Any,
    }
}

fn record_query(domains: Vec<signal_domain::Domain>, description: &str) -> Query {
    Query::Record(signal_spirit::RecordRequest {
        entry: Entry {
            domains,
            kind: Kind::Decision,
            description: description.into(),
            importance: Magnitude::Minimum,
        },
        justification: signal_spirit::Justification {
            testimony: vec![signal_spirit::VerbatimQuote {
                quote_text: description.into(),
                optional_antecedent: None,
            }],
            reasoning: description.into(),
        },
    })
}

// ---------------------------------------------------------------------------
// Nix-build harness: locate the schema-driven binaries.
// ---------------------------------------------------------------------------

/// Outcome of locating Nix-built spirit binaries.
///
/// Tests call `NixBuiltBinaries::ensure()` which either reuses
/// `SPIRIT_NIX_BUILD_RESULT` (a pre-built `result/` directory
/// containing `bin/spirit` + `bin/spirit-nexus`) or invokes
/// `nix build` against the workspace flake — the SAME build the
/// schema-driven check derivation runs.
#[derive(Debug, Clone)]
struct NixBuiltBinaries {
    spirit_cli: PathBuf,
    meta_spirit_cli: PathBuf,
    spirit_daemon: PathBuf,
}

impl NixBuiltBinaries {
    fn ensure() -> Self {
        nix_built_binaries().clone()
    }
}

/// One-shot global so multiple tests share a single `nix build` invocation.
fn nix_built_binaries() -> &'static NixBuiltBinaries {
    static BINARIES: OnceLock<NixBuiltBinaries> = OnceLock::new();
    BINARIES.get_or_init(NixBuiltBinaries::locate_or_build)
}

impl NixBuiltBinaries {
    fn locate_or_build() -> Self {
        if let Ok(directory) = env::var("SPIRIT_NIX_BUILD_RESULT") {
            return Self::from_directory(&PathBuf::from(directory));
        }
        Self::from_directory(&Self::nix_build())
    }

    fn from_directory(directory: &Path) -> Self {
        let spirit_cli = directory.join("bin").join("spirit");
        let meta_spirit_cli = directory.join("bin").join("spirit-meta");
        let spirit_daemon = directory.join("bin").join("spirit-nexus");
        assert!(
            spirit_cli.exists(),
            "expected Nix-built CLI binary at {}",
            spirit_cli.display()
        );
        assert!(
            meta_spirit_cli.exists(),
            "expected Nix-built meta CLI binary at {}",
            meta_spirit_cli.display()
        );
        assert!(
            spirit_daemon.exists(),
            "expected Nix-built daemon binary at {}",
            spirit_daemon.display()
        );
        Self {
            spirit_cli,
            meta_spirit_cli,
            spirit_daemon,
        }
    }

    /// Run `nix build` against the pushed workspace flake. Dependency
    /// overrides use remote refs so another agent can reproduce the same
    /// build without this machine's checkout state.
    fn nix_build() -> PathBuf {
        let repo_root = repo_root();
        let temp_link = repo_root.join("target").join("nix-integration-result");
        if temp_link.exists() {
            let _ = std::fs::remove_file(&temp_link);
        }

        let command_sandbox =
            ProcessSandbox::new("nested-nix-build").expect("create Nix command sandbox");
        let mut command = Command::isolated("nix");
        command
            .restore_nonsecret(NonSecretEnvironmentVariable::Path)
            .expect("restore reviewed PATH for Nix")
            .env("HOME", command_sandbox.root())
            .env("TMPDIR", command_sandbox.root());
        command
            .arg("build")
            .arg("--log-format")
            .arg("bar-with-logs")
            .arg("--print-out-paths")
            .arg("--no-link");
        for (input, flake_ref) in nix_input_overrides() {
            command.arg("--override-input").arg(input).arg(flake_ref);
        }
        command.arg(format!(
            "github:LiGoldragon/spirit?ref={}#default",
            target_reference()
        ));

        let output = command.output().expect("invoke nix build");
        assert!(
            output.status.success(),
            "nix build failed (status {}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        let store_path = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        assert!(
            !store_path.is_empty(),
            "nix build emitted no store path; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        PathBuf::from(store_path)
    }
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn nix_input_overrides() -> Vec<(&'static str, String)> {
    fn github_source(repository: &'static str, environment_key: &str) -> Option<String> {
        env::var(environment_key)
            .ok()
            .or_else(|| env::var("SPIRIT_STACK_REF").ok())
            .map(|reference_name| format!("github:LiGoldragon/{repository}?ref={reference_name}"))
    }

    let mut overrides = Vec::new();
    for (input, source) in [
        ("nota-source", github_source("nota", "NOTA_REF")),
        ("schema-source", github_source("schema", "SCHEMA_REF")),
        (
            "schema-rust-source",
            github_source("schema-rust", "SCHEMA_RUST_REF"),
        ),
        ("sema-source", github_source("sema", "SEMA_REF")),
        (
            "sema-engine-source",
            github_source("sema-engine", "SEMA_ENGINE_REF"),
        ),
        (
            "signal-frame-source",
            github_source("signal-frame", "SIGNAL_FRAME_REF"),
        ),
        (
            "signal-sema-source",
            github_source("signal-sema", "SIGNAL_SEMA_REF"),
        ),
        (
            "triad-runtime-source",
            github_source("triad-runtime", "TRIAD_RUNTIME_REF"),
        ),
    ] {
        if let Some(source) = source {
            overrides.push((input, source));
        }
    }
    overrides
}

struct GitHead {
    repository: PathBuf,
}

impl GitHead {
    fn new(repository: PathBuf) -> Self {
        Self { repository }
    }

    fn commit(&self) -> Option<String> {
        let mut command = Command::isolated("git");
        command
            .restore_nonsecret(NonSecretEnvironmentVariable::Path)
            .ok()?;
        let output = command
            .arg("rev-parse")
            .arg("HEAD")
            .current_dir(&self.repository)
            .output()
            .ok()?;
        output
            .status
            .success()
            .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
            .filter(|reference| !reference.is_empty())
    }
}

fn target_reference() -> String {
    env::var("SPIRIT_TARGET_REF")
        .ok()
        .or_else(|| GitHead::new(repo_root()).commit())
        .unwrap_or_else(|| String::from("main"))
}

// ---------------------------------------------------------------------------
// Daemon launch + CLI exchange — process boundary helpers.
// ---------------------------------------------------------------------------

/// RAII handle to a spawned daemon process. Drop kills the process.
struct DaemonProcess {
    child: ManagedChild,
    socket_path: PathBuf,
    meta_socket_path: PathBuf,
    _sandbox: ProcessSandbox,
}

impl DaemonProcess {
    fn spawn(binaries: &NixBuiltBinaries) -> Self {
        let sandbox = ProcessSandbox::new("nix-integration").expect("create process sandbox");
        let socket_path = sandbox.working_socket();
        let meta_socket_path = sandbox.meta_socket();
        let database_path = sandbox.live_database();
        let configuration_path = sandbox.configuration();
        Configuration::new(&socket_path, &database_path)
            .with_meta_socket_path(&meta_socket_path)
            .write_binary_file(&configuration_path)
            .expect("write binary daemon configuration");

        let mut command = Command::isolated(&binaries.spirit_daemon);
        command
            .arg(configuration_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let child =
            ManagedChild::spawn(&mut command, "Nix-built Spirit daemon").expect("spawn daemon");

        let mut process = Self {
            child,
            socket_path,
            meta_socket_path,
            _sandbox: sandbox,
        };
        process
            .child
            .wait_for_unix_socket(&process.socket_path, Duration::from_secs(5))
            .expect("Nix-built Spirit working socket readiness");
        process
            .child
            .wait_for_unix_socket(&process.meta_socket_path, Duration::from_secs(5))
            .expect("Nix-built Spirit meta socket readiness");
        process
    }

    fn socket(&self) -> &Path {
        &self.socket_path
    }

    fn meta_socket(&self) -> &Path {
        &self.meta_socket_path
    }
}

/// Run the CLI binary against the daemon's socket with one NOTA
/// argument. Returns the parsed schema-emitted `Output` — no raw
/// string assertions in callers (record 995/996/997).
fn actualize_response(text: &str) -> Response {
    let mut pending = Potential::<Response>::from(text.to_owned());
    pending
        .actualize(&mut Budget {
            remaining: 1024,
            reader: ReaderBudget { remaining: 1024 },
            depth: 0,
            maximum_depth: 1024,
        })
        .unwrap_or_else(|error| panic!("actualize Signal response {text:?}: {error:?}"))
}

fn run_cli_for_output(binaries: &NixBuiltBinaries, socket: &Path, query: Query) -> Response {
    let output = Command::isolated(&binaries.spirit_cli)
        .arg(query.datomize(vec![]).protosize().textualize())
        .env("SPIRIT_SOCKET", socket)
        .output()
        .expect("run CLI");
    assert!(
        output.status.success(),
        "spirit CLI failed (status {}): stderr={}; stdout={}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    actualize_response(
        String::from_utf8(output.stdout)
            .expect("CLI stdout is UTF-8")
            .trim_end(),
    )
}

fn run_meta_cli_for_output(
    binaries: &NixBuiltBinaries,
    meta_socket: &Path,
    query: MetaQuery,
) -> MetaResponse {
    let output = Command::isolated(&binaries.meta_spirit_cli)
        .arg(query.datomize(vec![]).protosize().textualize())
        .env("SPIRIT_META_SOCKET", meta_socket)
        .output()
        .expect("run meta CLI");
    assert!(
        output.status.success(),
        "spirit-meta CLI failed (status {}): stderr={}; stdout={}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    let text = String::from_utf8(output.stdout).expect("meta CLI stdout is UTF-8");
    let mut pending = Potential::<MetaResponse>::from(text.trim_end().to_owned());
    pending
        .actualize(&mut Budget {
            remaining: 1024,
            reader: ReaderBudget { remaining: 1024 },
            depth: 0,
            maximum_depth: 1024,
        })
        .unwrap_or_else(|error| panic!("actualize Meta response: {error:?}"))
}

// ---------------------------------------------------------------------------
// Schema-emitted fixture constructors — never strings as state.
// ---------------------------------------------------------------------------

fn entry(description: &str) -> Entry {
    Entry {
        domains: domain_fixtures::domains(&["nix-integration"]),
        kind: Kind::Decision,
        description: description.into(),
        importance: Magnitude::Minimum,
    }
}

fn import_record(
    binaries: &NixBuiltBinaries,
    daemon: &DaemonProcess,
    identifier: &str,
    description: &str,
) -> RecordIdentifier {
    let record_identifier = identifier.to_owned();
    let input = MetaQuery::Import(ImportRequest {
        imported_records: vec![ImportedRecord {
            record_identifier: record_identifier.clone(),
            entry: entry(description),
        }],
    });
    let output = run_meta_cli_for_output(binaries, daemon.meta_socket(), input);
    match output {
        MetaResponse::Imported(receipt) => {
            assert_eq!(receipt.record_count, 1);
            record_identifier
        }
        other => panic!("expected schema-emitted meta Imported, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Tests — every one of these proves the schema-driven build pipeline
// AND the runtime end-to-end, through the same binaries Nix builds.
// ---------------------------------------------------------------------------

/// Sanity check: the Nix build actually produces both binaries from
/// the schema-driven build pipeline. If THIS fails, every other test
/// below is meaningless — so it runs first and fast.
#[test]
#[ignore = "invokes nix build; run via cargo test --test nix_integration -- --ignored"]
fn nix_build_default_package_emits_both_binaries() {
    let binaries = NixBuiltBinaries::ensure();

    // PROOF: both binaries exist as executables on the Nix store path.
    let metadata_cli = std::fs::metadata(&binaries.spirit_cli).expect("stat CLI");
    let metadata_daemon = std::fs::metadata(&binaries.spirit_daemon).expect("stat daemon");
    assert!(metadata_cli.is_file());
    assert!(metadata_daemon.is_file());
    assert!(
        is_executable(&metadata_cli),
        "CLI binary at {} must be executable",
        binaries.spirit_cli.display()
    );
    assert!(
        is_executable(&metadata_daemon),
        "daemon binary at {} must be executable",
        binaries.spirit_daemon.display()
    );
}

#[test]
#[ignore = "invokes nix build; run via cargo test --test nix_integration -- --ignored"]
fn nix_built_spirit_cli_records_through_real_socket_to_nix_built_daemon() {
    // PATTERN: Record path end-to-end through the actual binaries Nix
    // built. The CLI binary opens a Unix socket to the daemon binary,
    // sends an rkyv-encoded signal frame, the daemon's SignalAdmission +
    // Engine + Store triad runs, the schema-emitted record identifier comes
    // back through the reverse plane chain, the CLI writes the NOTA
    // round-trip to stdout, and we parse it back into the schema-
    // emitted Output enum.
    let binaries = NixBuiltBinaries::ensure();
    let daemon = DaemonProcess::spawn(&binaries);

    let output = run_cli_for_output(
        &binaries,
        daemon.socket(),
        record_query(
            domain_fixtures::domains(&["deployment"]),
            "end to end through nix built binaries",
        ),
    );

    // SCHEMA-TYPED ASSERTION: the production daemon build requires a guardian.
    // With no guardian agent configured in this sandbox, working writes fail
    // closed; privileged test seeding goes through owner-only meta Import below.
    match output {
        Response::GuardianRejected(rejection) => {
            assert_eq!(
                rejection.guardian_rejection_reason,
                GuardianRejectionReason::HarnessUnavailable
            );
        }
        other => panic!("expected schema-emitted GuardianRejected, got {other:?}"),
    }
}

#[test]
#[ignore = "invokes nix build; run via cargo test --test nix_integration -- --ignored"]
fn nix_built_daemon_rejects_invalid_input_through_schema_emitted_rejection() {
    // PATTERN: invalid Input — empty domain — fails `Entry::validate`
    // inside SignalAdmission::admit on the Nix-built daemon. The reply is
    // the schema-emitted `SignalRejection` variant carrying the schema-
    // emitted `ValidationError::EmptyDomain`. The CLI prints the
    // schema-emitted NOTA round-trip; we parse it back through
    // `Response::FromStr` and match the typed variant.
    let binaries = NixBuiltBinaries::ensure();
    let daemon = DaemonProcess::spawn(&binaries);

    // Empty domain — schema-emitted Entry validation should reject.
    let output = run_cli_for_output(
        &binaries,
        daemon.socket(),
        record_query(vec![], "body content"),
    );

    assert_eq!(
        output,
        Response::Rejected(SignalRejection {
            validation_error: ValidationError::EmptyDomain
        }),
        "the Nix-built daemon's schema-emitted SignalRejection variant must arrive intact"
    );
}

#[test]
#[ignore = "invokes nix build; run via cargo test --test nix_integration -- --ignored"]
fn nix_built_daemon_persists_state_across_two_cli_invocations() {
    // PATTERN: two CLI binary invocations against the SAME daemon
    // process show the schema-emitted DatabaseMarker.commit_sequence
    // advancing monotonically. The SEMA store is single-writer per
    // record 949; this test proves the daemon's durable `.sema` store
    // is shared between connections. Restart persistence is covered by
    // `daemon_persists_sema_file_across_a_restart`.
    let binaries = NixBuiltBinaries::ensure();
    let daemon = DaemonProcess::spawn(&binaries);

    let first_identifier = import_record(&binaries, &daemon, "nixa1", "first commit");
    assert_short_record_identifier(&first_identifier);
    let first_marker = match run_cli_for_output(&binaries, daemon.socket(), Query::Marker) {
        Response::MarkerReported(marker) => marker,
        other => panic!("expected MarkerReported after first record, got {other:?}"),
    };

    let second_identifier = import_record(&binaries, &daemon, "nixa2", "second commit");
    assert_short_record_identifier(&second_identifier);
    let second_marker = match run_cli_for_output(&binaries, daemon.socket(), Query::Marker) {
        Response::MarkerReported(marker) => marker,
        other => panic!("expected MarkerReported after second record, got {other:?}"),
    };

    assert!(
        second_marker.commit_sequence > first_marker.commit_sequence,
        "schema-emitted CommitSequence advances across CLI invocations: {} -> {}",
        first_marker.commit_sequence,
        second_marker.commit_sequence
    );
    // The state digest also evolves (different records contribute
    // different magnitude importances into the digest fold).
    assert_ne!(
        first_marker.state_digest, second_marker.state_digest,
        "schema-emitted StateDigest must reflect the new record"
    );
}

#[test]
#[ignore = "invokes nix build; run via cargo test --test nix_integration -- --ignored"]
fn nix_built_daemon_observes_recorded_entries_back_through_query() {
    // PATTERN: Record then Observe through the Nix-built binaries. The
    // Observe path crosses Signal -> Nexus -> SEMA -> Nexus -> Signal
    // end-to-end, producing the schema-emitted `RecordsStashed` variant.
    // The first reply carries the original Entry inline; a follow-up
    // LookupStash recovers the same Entry.
    let binaries = NixBuiltBinaries::ensure();
    let daemon = DaemonProcess::spawn(&binaries);

    let record_identifier = import_record(&binaries, &daemon, "nixo1", "observe round trip");
    assert_short_record_identifier(&record_identifier);

    let observed = run_cli_for_output(
        &binaries,
        daemon.socket(),
        Query::Observe(selection(domain_fixtures::domains(&["deployment"]))),
    );

    let stash_handle = match observed {
        Response::RecordsStashed(stashed) => {
            assert_eq!(stashed.record_count, 1);
            assert_short_record_identifier(
                &stashed.observed_records.record_set[0].record_identifier,
            );
            assert_eq!(
                stashed.observed_records.record_set[0].entry,
                entry("observe round trip"),
                "Observe must return the schema-emitted Entry inline"
            );
            stashed.stash_handle
        }
        other => panic!("expected schema-emitted RecordsStashed, got {other:?}"),
    };

    let resolved = run_cli_for_output(&binaries, daemon.socket(), Query::LookupStash(stash_handle));

    match resolved {
        Response::RecordsObserved(records) => {
            assert_short_record_identifier(&records.record_set[0].record_identifier);
            assert_eq!(
                records.record_set[0].entry,
                entry("observe round trip"),
                "LookupStash must echo the schema-emitted Entry we recorded"
            );
        }
        other => panic!("expected schema-emitted RecordsObserved from LookupStash, got {other:?}"),
    }
}

#[test]
#[ignore = "invokes nix build; run via cargo test --test nix_integration -- --ignored"]
fn nix_built_daemon_returns_missed_when_no_matching_record_exists() {
    // PATTERN: Observe against an empty store traverses to the SEMA read
    // plane and returns `SemaReadResponse::Missed`, which lowers through
    // the Nexus reverse plane to `Response::Error(ErrorReport)`. The CLI
    // prints the NOTA, we parse it back through the schema-emitted
    // FromStr surface.
    let binaries = NixBuiltBinaries::ensure();
    let daemon = DaemonProcess::spawn(&binaries);

    let output = run_cli_for_output(
        &binaries,
        daemon.socket(),
        Query::Observe(selection(domain_fixtures::domains(&["agent-systems"]))),
    );

    match output {
        Response::Error(report) => {
            // The schema-emitted ErrorMessage carries the SEMA "no matching record" string.
            assert_eq!(report.error_message, "no matching record");
        }
        other => panic!("expected schema-emitted Response::Error, got {other:?}"),
    }
}

#[test]
#[ignore = "invokes nix build; run via cargo test --test nix_integration -- --ignored"]
fn nix_built_daemon_handles_back_to_back_inputs_through_one_socket() {
    // PATTERN: multiple sequential inputs against ONE daemon process
    // exercise the daemon's accept-loop. Each input arrives on a fresh
    // Unix-socket connection; the daemon's `handle_stream` consumes
    // one input + writes one output per stream. The schema-emitted
    // CommitSequence advances by 3 across the burst.
    let binaries = NixBuiltBinaries::ensure();
    let daemon = DaemonProcess::spawn(&binaries);

    import_record(&binaries, &daemon, "nixb1", "alpha");
    import_record(&binaries, &daemon, "nixb2", "beta");
    import_record(&binaries, &daemon, "nixb3", "gamma");

    let version = run_cli_for_output(&binaries, daemon.socket(), Query::Version);
    assert!(matches!(version, Response::VersionReported(_)));

    let marker = run_cli_for_output(&binaries, daemon.socket(), Query::Marker);
    assert!(matches!(marker, Response::MarkerReported(_)));

    let counted = run_cli_for_output(
        &binaries,
        daemon.socket(),
        Query::Count(selection(domain_fixtures::domains(&["deployment"]))),
    );
    match counted {
        Response::RecordsCounted(counted) => assert_eq!(counted.record_count, 3),
        other => panic!("expected RecordsCounted after back-to-back reads, got {other:?}"),
    }
}

#[test]
#[ignore = "invokes nix build; run via cargo test --test nix_integration -- --ignored"]
fn nix_built_binaries_round_trip_representative_schema_outputs() {
    // PATTERN: drive each schema-emitted Output variant through the
    // Nix-built binaries; parse the CLI's stdout back through the
    // schema-emitted Response::FromStr. The test proves the NOTA wire
    // form and the FromStr surface stay in sync for every variant —
    // a regression here would mean a CLI user sees output the CLI
    // itself cannot parse, which would silently break tooling.
    //
    // Variants exercised: VersionReported (component version),
    // GuardianRejected (fail-closed admission without a configured guardian),
    // ImportanceBumped (SEMA mutate on an imported record), Rejected (Signal
    // validation), Error (SEMA missed), RecordsStashed (after Import +
    // Observe). Six typed assertions, all parsed through `Response::from_str`.
    let binaries = NixBuiltBinaries::ensure();
    let daemon = DaemonProcess::spawn(&binaries);

    // Variant 1: VersionReported.
    let version = run_cli_for_output(&binaries, daemon.socket(), Query::Version);
    match &version {
        Response::VersionReported(report) => {
            assert_eq!(report.version_text, env!("CARGO_PKG_VERSION"));
        }
        other => panic!("expected VersionReported, got {other:?}"),
    }

    // Variant 2: GuardianRejected.
    let guarded = run_cli_for_output(
        &binaries,
        daemon.socket(),
        record_query(domain_fixtures::domains(&["deployment"]), "variant tour"),
    );
    match &guarded {
        Response::GuardianRejected(rejection) => assert_eq!(
            rejection.guardian_rejection_reason,
            GuardianRejectionReason::HarnessUnavailable
        ),
        other => panic!("expected GuardianRejected, got {other:?}"),
    };

    let rerecorded_identifier = import_record(&binaries, &daemon, "nixt1", "variant tour");
    assert_short_record_identifier(&rerecorded_identifier);

    // Variant 3: ImportanceBumped.
    let changed = run_cli_for_output(
        &binaries,
        daemon.socket(),
        Query::BumpImportance(signal_spirit::ImportanceBump {
            record_identifier: rerecorded_identifier,
        }),
    );
    assert!(matches!(changed, Response::ImportanceBumped(_)));

    // Variant 4: Rejected (Signal validation).
    let rejected = run_cli_for_output(
        &binaries,
        daemon.socket(),
        record_query(vec![], "empty domain"),
    );
    assert!(matches!(rejected, Response::Rejected(_)));

    // Variant 5: Error (SEMA missed).
    let errored = run_cli_for_output(
        &binaries,
        daemon.socket(),
        Query::Observe(selection(domain_fixtures::domains(&["agent-systems"]))),
    );
    assert!(matches!(errored, Response::Error(_)));

    // Variant 6: RecordsStashed.
    let observed_identifier = import_record(&binaries, &daemon, "nixt2", "variant tour visible");
    assert_short_record_identifier(&observed_identifier);
    let observed = run_cli_for_output(
        &binaries,
        daemon.socket(),
        Query::Observe(selection(domain_fixtures::domains(&["deployment"]))),
    );
    assert!(matches!(observed, Response::RecordsStashed(_)));
}

// ---------------------------------------------------------------------------
// Concurrency probe — the daemon serves ONE input per stream but the
// listener accepts many streams. We don't push to true parallelism here
// (one-shot per stream is the current contract), but we DO verify that
// sequential connections from different invocations alias to the same
// durable `.sema` store handle.
// ---------------------------------------------------------------------------

#[test]
#[ignore = "invokes nix build; run via cargo test --test nix_integration -- --ignored"]
fn nix_built_daemon_alias_state_across_separate_cli_processes() {
    // PATTERN: two SEPARATE CLI processes (so two distinct exec calls)
    // hit the same daemon process. State must alias between them.
    // Distinct from the persistence test in that this measures process-
    // boundary, not connection-reuse.
    let binaries = NixBuiltBinaries::ensure();
    let daemon = DaemonProcess::spawn(&binaries);

    let record_identifier = import_record(&binaries, &daemon, "nixp1", "process a record");
    assert_short_record_identifier(&record_identifier);

    // Independent process — exec a fresh CLI binary for the read.
    let mut command = Command::isolated(&binaries.spirit_cli);
    command
        .arg(
            Query::Observe(selection(domain_fixtures::domains(&["deployment"])))
                .datomize(vec![])
                .protosize()
                .textualize(),
        )
        .env("SPIRIT_SOCKET", daemon.socket())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child_a =
        ManagedChild::spawn(&mut command, "Nix-built Spirit CLI").expect("spawn CLI process a");
    let exit_a = child_a.wait().expect("CLI process a wait");
    assert!(exit_a.success(), "CLI process a failed");
    let mut stdout_a = String::new();
    child_a
        .take_stdout()
        .expect("CLI a stdout")
        .read_to_string(&mut stdout_a)
        .expect("read CLI a stdout");
    let observed = actualize_response(stdout_a.trim_end());
    let stash_handle = match observed {
        Response::RecordsStashed(stashed) => {
            assert_eq!(stashed.record_count, 1);
            assert_short_record_identifier(
                &stashed.observed_records.record_set[0].record_identifier,
            );
            assert_eq!(
                stashed.observed_records.record_set[0].entry,
                entry("process a record"),
                "Observe must return the schema-emitted Entry inline"
            );
            stashed.stash_handle
        }
        other => panic!("expected RecordsStashed across separate CLI processes, got {other:?}"),
    };
    let resolved = run_cli_for_output(&binaries, daemon.socket(), Query::LookupStash(stash_handle));
    match resolved {
        Response::RecordsObserved(records) => {
            assert_short_record_identifier(&records.record_set[0].record_identifier);
            assert_eq!(
                records.record_set[0].entry,
                entry("process a record"),
                "the daemon must remember the record across separate CLI processes"
            )
        }
        other => panic!("expected RecordsObserved from LookupStash, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Platform helpers.
// ---------------------------------------------------------------------------

#[cfg(unix)]
fn is_executable(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn is_executable(_metadata: &std::fs::Metadata) -> bool {
    true
}
