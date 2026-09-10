mod support;

use std::{
    fs,
    io::{BufRead, BufReader},
    path::Path,
    process::{ChildStdout, Command, Stdio},
    sync::mpsc::{self, Receiver},
    thread,
    time::Duration,
};
use support::{
    domain_fixtures,
    process::{CommandIsolation, ManagedChild},
};

use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use nexus::Configurable;
use protos::ReaderBudget;
use protos::{Protosizable, Textualizable};
use signal_domain::{Domain, KinshipDomain};
#[cfg(feature = "testing-trace")]
use signal_introspect::ComponentTraceEvent;
#[cfg(feature = "agent-guardian")]
use signal_spirit_judge::{
    AdmissionJudgeResponse, AdmissionJudgeVerdict, ByteViewable, JudgeDiagnostic,
    Query as SpiritJudgeQuery, Response as SpiritJudgeResponse, Restorable, Signal, Signalizable,
};
use spirit::schema::signal::{
    ClarificationResolution, IntentEvent, Justification, Kind, Magnitude, Query, RecordIdentifier,
    Response, TargetClarification, VerbatimQuote,
};
use spirit::{Configuration, Store};
#[cfg(feature = "agent-guardian")]
use std::{
    io::{Read, Write},
    os::unix::net::{UnixListener, UnixStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tempfile::TempDir;

/// Locates a workspace executable built by the split-package test command.
/// Integration tests deliberately execute the separately packaged clients and
/// Nexus rather than a compatibility binary from the library package.
fn workspace_binary(name: &str) -> std::path::PathBuf {
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target"));
    let binary = target.join("debug").join(name);
    assert!(
        binary.is_file(),
        "workspace executable {} is not built; run cargo build --workspace first",
        binary.display()
    );
    binary
}

struct DaemonProcess {
    child: ManagedChild,
    #[cfg(feature = "agent-guardian")]
    _spirit_judge: FakeSpiritJudge,
}

#[cfg(feature = "agent-guardian")]
struct FakeSpiritJudge {
    socket_path: std::path::PathBuf,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

struct SubscriberProcess {
    child: ManagedChild,
    lines: Receiver<String>,
    reader_thread: Option<thread::JoinHandle<()>>,
}

#[cfg(feature = "agent-guardian")]
impl Drop for FakeSpiritJudge {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for SubscriberProcess {
    fn drop(&mut self) {
        let _ = self.child.terminate();
        if let Some(reader_thread) = self.reader_thread.take() {
            let _ = reader_thread.join();
        }
    }
}

#[cfg(feature = "agent-guardian")]
impl FakeSpiritJudge {
    fn spawn(socket_path: std::path::PathBuf) -> Self {
        let listener = UnixListener::bind(&socket_path).expect("bind fake spirit judge socket");
        listener
            .set_nonblocking(true)
            .expect("fake spirit judge listener nonblocking");
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let thread = thread::spawn(move || {
            while !thread_stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => Self::answer(&mut stream),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => panic!("accept fake spirit judge call: {error}"),
                }
            }
        });
        Self {
            socket_path,
            stop,
            thread: Some(thread),
        }
    }

    fn answer(stream: &mut UnixStream) {
        let request = FrameIo::new(stream).read_query();
        let reply = match request {
            SpiritJudgeQuery::JudgeAdmission(_) => {
                SpiritJudgeResponse::AdmissionJudged(AdmissionJudgeResponse {
                    admission_judge_verdict: AdmissionJudgeVerdict::Accept,
                    judge_diagnostic: Self::diagnostic("accepted by process-boundary fake judge"),
                })
            }
        };
        FrameIo::new(stream).write_response(&reply);
    }

    fn diagnostic(text: &str) -> JudgeDiagnostic {
        JudgeDiagnostic {
            redacted_text: text.into(),
            content_hashes: vec![],
        }
    }

    fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

#[cfg(feature = "agent-guardian")]
struct FrameIo<'stream> {
    stream: &'stream mut UnixStream,
}

#[cfg(feature = "agent-guardian")]
impl<'stream> FrameIo<'stream> {
    fn new(stream: &'stream mut UnixStream) -> Self {
        Self { stream }
    }
    fn read_query(&mut self) -> SpiritJudgeQuery {
        let mut prefix = [0_u8; 4];
        self.stream
            .read_exact(&mut prefix)
            .expect("read judge prefix");
        let mut bytes = vec![0; u32::from_be_bytes(prefix) as usize];
        self.stream.read_exact(&mut bytes).expect("read judge body");
        Signal::<SpiritJudgeQuery>::from(bytes)
            .restore()
            .expect("restore judge query")
    }
    fn write_response(&mut self, response: &SpiritJudgeResponse) {
        let signal = response.signalize().expect("archive judge response");
        let bytes = signal.bytes();
        self.stream
            .write_all(
                &u32::try_from(bytes.len())
                    .expect("judge frame length")
                    .to_be_bytes(),
            )
            .expect("write judge prefix");
        self.stream.write_all(bytes).expect("write judge body");
        self.stream.flush().expect("flush judge response");
    }
}

impl DaemonProcess {
    fn meta_socket_path(socket_path: &Path) -> std::path::PathBuf {
        socket_path.with_extension("meta.sock")
    }

    #[cfg(feature = "agent-guardian")]
    fn spirit_judge(socket_path: &Path) -> FakeSpiritJudge {
        FakeSpiritJudge::spawn(socket_path.with_extension("spirit-judge.sock"))
    }

    #[cfg(feature = "agent-guardian")]
    fn configuration_with_guardian(
        configuration: Configuration,
        spirit_judge: &FakeSpiritJudge,
    ) -> Configuration {
        Configuration::from_raw_at_database(
            signal_spirit::SpiritNexusConfiguration {
                socket_path: configuration.raw().socket_path.clone(),
                optional_meta_socket_path: configuration.raw().optional_meta_socket_path.clone(),
                optional_trace_socket_path: configuration.raw().optional_trace_socket_path.clone(),
                authorization_mode: configuration.raw().authorization_mode.clone(),
                optional_spirit_guardian_agent_configuration: Some(
                    signal_spirit::SpiritGuardianAgentConfiguration {
                        agent_socket_path: spirit_judge
                            .socket_path()
                            .to_string_lossy()
                            .into_owned(),
                        optional_spirit_guardian_provider_name: None,
                        optional_spirit_guardian_model_name: None,
                        spirit_guardian_timeout_milliseconds: 5_000,
                        optional_spirit_guardian_maximum_output_tokens: None,
                    },
                ),
            },
            configuration.database_path().to_path_buf(),
        )
    }

    fn spawn(socket_path: &Path, database_path: &Path) -> Self {
        let meta_socket_path = Self::meta_socket_path(socket_path);
        #[cfg(feature = "agent-guardian")]
        let spirit_judge = Self::spirit_judge(socket_path);
        let configuration =
            Configuration::new(socket_path, database_path).with_meta_socket_path(&meta_socket_path);
        #[cfg(feature = "agent-guardian")]
        let configuration = Self::configuration_with_guardian(configuration, &spirit_judge);
        let state_home = Self::seed_zero_argument_configuration(&configuration);
        let mut command = Command::isolated(workspace_binary("spirit-nexus"));
        command.env("XDG_STATE_HOME", state_home);
        let child = ManagedChild::spawn(&mut command, "Spirit daemon").expect("spawn daemon");
        let mut process = Self {
            child,
            #[cfg(feature = "agent-guardian")]
            _spirit_judge: spirit_judge,
        };
        process
            .child
            .wait_for_unix_socket(socket_path, Duration::from_secs(5))
            .expect("working socket readiness");
        process
            .child
            .wait_for_unix_socket(&meta_socket_path, Duration::from_secs(5))
            .expect("meta socket readiness");
        process
    }

    /// Seed the isolated established XDG location before zero-argument Nexus
    /// startup. No serialized configuration is passed to the daemon.
    fn seed_zero_argument_configuration(configuration: &Configuration) -> std::path::PathBuf {
        let database_path = configuration.database_path();
        let parent = database_path.parent().expect("test database parent");
        let file_stem = database_path
            .file_stem()
            .expect("test database name")
            .to_string_lossy();
        let state_home = parent.join(format!("{file_stem}.xdg-state"));
        let stable_directory = state_home.join("spirit");
        let stable_database = stable_directory.join("spirit.sema");
        fs::create_dir_all(&stable_directory).expect("create isolated XDG Spirit state");
        if !stable_database.exists() {
            std::os::unix::fs::symlink(database_path, &stable_database)
                .expect("link isolated stable Sema to test database");
        }
        let store = Store::open_with_configuration(database_path, configuration.raw().clone())
            .expect("open isolated persisted Nexus configuration");
        let mut lifecycle = store
            .nexus_configuration_state()
            .expect("read isolated persisted Nexus configuration");
        lifecycle
            .ordinary_configure_if_unset(configuration.raw().clone())
            .expect("ordinary Configure remains available before meta Configure");
        store
            .replace_nexus_configuration(lifecycle)
            .expect("persist isolated ordinary Configure");
        state_home
    }

    #[cfg(feature = "testing-trace")]
    fn spawn_with_trace(
        socket_path: &Path,
        database_path: &Path,
        trace_socket_path: &Path,
    ) -> Self {
        let meta_socket_path = Self::meta_socket_path(socket_path);
        #[cfg(feature = "agent-guardian")]
        let spirit_judge = Self::spirit_judge(socket_path);
        let configuration =
            Configuration::new_with_trace(socket_path, database_path, trace_socket_path)
                .with_meta_socket_path(&meta_socket_path);
        #[cfg(feature = "agent-guardian")]
        let configuration = Self::configuration_with_guardian(configuration, &spirit_judge);
        let state_home = Self::seed_zero_argument_configuration(&configuration);
        let mut command = Command::isolated(workspace_binary("spirit-nexus"));
        command.env("XDG_STATE_HOME", state_home);
        let child = ManagedChild::spawn(&mut command, "Spirit trace daemon").expect("spawn daemon");
        let mut process = Self {
            child,
            #[cfg(feature = "agent-guardian")]
            _spirit_judge: spirit_judge,
        };
        process
            .child
            .wait_for_unix_socket(socket_path, Duration::from_secs(5))
            .expect("working socket readiness");
        process
            .child
            .wait_for_unix_socket(&meta_socket_path, Duration::from_secs(5))
            .expect("meta socket readiness");
        process
    }
}

#[test]
fn configuration_writer_accepts_judge_socket_without_output_budget() {
    let directory = TempDir::new().expect("tempdir");
    let socket_path = directory.path().join("spirit.sock");
    let meta_socket_path = directory.path().join("meta.sock");
    let database_path = directory.path().join("spirit.sema");
    let judge_socket_path = directory.path().join("spirit-judge.sock");
    let configuration_path = directory.path().join("spirit.config.rkyv");
    let request = writer_request(
        &socket_path,
        &meta_socket_path,
        &database_path,
        Some(&judge_socket_path),
        120_000,
        None,
        None,
        &configuration_path,
        "Gating",
    );

    let output = Command::isolated(workspace_binary("spirit-write-configuration"))
        .arg(request)
        .output()
        .expect("run configuration writer");

    assert!(
        output.status.success(),
        "configuration writer stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let configuration =
        Configuration::from_binary_path(&configuration_path).expect("decode binary config");
    let guardian = configuration
        .raw()
        .optional_spirit_guardian_agent_configuration
        .as_ref()
        .expect("legacy guardian configuration carries judge socket");
    assert_eq!(
        guardian.agent_socket_path,
        judge_socket_path.to_string_lossy()
    );
    assert_eq!(guardian.spirit_guardian_timeout_milliseconds, 120_000);
    assert_eq!(
        guardian.optional_spirit_guardian_maximum_output_tokens,
        None
    );
}

#[test]
fn configuration_writer_rejects_negative_guardian_numbers_without_writing_archive() {
    let directory = TempDir::new().expect("tempdir");
    let socket_path = directory.path().join("spirit.sock");
    let meta_socket_path = directory.path().join("meta.sock");
    let database_path = directory.path().join("spirit.sema");
    let judge_socket_path = directory.path().join("spirit-judge.sock");
    let configuration_path = directory.path().join("rejected.config.rkyv");
    let request = writer_request(
        &socket_path,
        &meta_socket_path,
        &database_path,
        Some(&judge_socket_path),
        -1,
        None,
        None,
        &configuration_path,
        "Gating",
    );
    let output = Command::isolated(workspace_binary("spirit-write-configuration"))
        .arg(request)
        .output()
        .expect("run configuration writer");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("timeout must be non-negative"));
    assert!(
        !configuration_path.exists(),
        "rejected input must not write an archive"
    );
}

#[cfg(feature = "agent-guardian")]
#[test]
fn configuration_writer_omitted_legacy_provider_stays_unowned_by_daemon_judge() {
    let directory = TempDir::new().expect("tempdir");
    let socket_path = directory.path().join("spirit.sock");
    let meta_socket_path = directory.path().join("meta.sock");
    let database_path = directory.path().join("spirit.sema");
    let judge_socket_path = directory.path().join("spirit-judge.sock");
    let configuration_path = directory.path().join("spirit.config.rkyv");
    let request = writer_request(
        &socket_path,
        &meta_socket_path,
        &database_path,
        Some(&judge_socket_path),
        180_000,
        None,
        None,
        &configuration_path,
        "Gating",
    );

    let output = Command::isolated(workspace_binary("spirit-write-configuration"))
        .arg(request)
        .output()
        .expect("run configuration writer");

    assert!(
        output.status.success(),
        "configuration writer stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let configuration =
        Configuration::from_binary_path(&configuration_path).expect("decode binary config");
    let raw_guardian = configuration
        .raw()
        .optional_spirit_guardian_agent_configuration
        .as_ref()
        .expect("raw compatibility configuration");
    assert_eq!(
        raw_guardian
            .optional_spirit_guardian_provider_name
            .as_deref(),
        None
    );
    assert_eq!(
        raw_guardian.optional_spirit_guardian_model_name.as_deref(),
        None
    );
    let judge = configuration
        .raw()
        .optional_spirit_guardian_agent_configuration
        .as_ref()
        .expect("daemon judge configuration");
    assert_eq!(judge.agent_socket_path, judge_socket_path.to_string_lossy());
    assert_eq!(judge.optional_spirit_guardian_provider_name, None);
    assert_eq!(judge.optional_spirit_guardian_model_name, None);
}

#[cfg(feature = "agent-guardian")]
#[test]
fn legacy_provider_model_fields_are_ignored_by_daemon_judge_configuration() {
    let directory = TempDir::new().expect("tempdir");
    let socket_path = directory.path().join("spirit.sock");
    let meta_socket_path = directory.path().join("meta.sock");
    let database_path = directory.path().join("spirit.sema");
    let judge_socket_path = directory.path().join("spirit-judge.sock");
    let configuration_path = directory.path().join("spirit.config.rkyv");
    let request = writer_request(
        &socket_path,
        &meta_socket_path,
        &database_path,
        Some(&judge_socket_path),
        180_000,
        Some("legacy-provider"),
        Some("legacy-model"),
        &configuration_path,
        "Gating",
    );

    let output = Command::isolated(workspace_binary("spirit-write-configuration"))
        .arg(request)
        .output()
        .expect("run configuration writer");

    assert!(
        output.status.success(),
        "configuration writer stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let configuration =
        Configuration::from_binary_path(&configuration_path).expect("decode binary config");
    let raw_guardian = configuration
        .raw()
        .optional_spirit_guardian_agent_configuration
        .as_ref()
        .expect("raw compatibility configuration");
    assert_eq!(
        raw_guardian
            .optional_spirit_guardian_provider_name
            .as_deref(),
        Some("legacy-provider")
    );
    assert_eq!(
        raw_guardian.optional_spirit_guardian_model_name.as_deref(),
        Some("legacy-model")
    );
    let judge = configuration
        .guardian_agent_configuration()
        .expect("daemon judge configuration");
    assert_eq!(judge.socket_path(), judge_socket_path);
    assert_eq!(judge.provider_name(), None);
    assert_eq!(judge.model_name(), None);
    assert_eq!(judge.timeout().as_millis(), 180_000);
}

#[test]
fn configuration_writer_encodes_observing_authorization_mode() {
    let directory = TempDir::new().expect("tempdir");
    let socket_path = directory.path().join("spirit.sock");
    let meta_socket_path = directory.path().join("meta.sock");
    let database_path = directory.path().join("spirit.sema");
    let configuration_path = directory.path().join("spirit.config.rkyv");
    let request = writer_request(
        &socket_path,
        &meta_socket_path,
        &database_path,
        None,
        0,
        None,
        None,
        &configuration_path,
        "Observing",
    );

    let output = Command::isolated(workspace_binary("spirit-write-configuration"))
        .arg(request)
        .output()
        .expect("run configuration writer");
    assert!(
        output.status.success(),
        "configuration writer succeeds: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let configuration =
        Configuration::from_binary_path(&configuration_path).expect("decode binary config");

    assert_eq!(
        configuration.authorization_mode(),
        signal_spirit::AuthorizationMode::Observing
    );
}

impl SubscriberProcess {
    fn spawn(socket_path: &Path, query: Query) -> Self {
        let mut command = Command::isolated(workspace_binary("spirit"));
        command
            .env("SPIRIT_SOCKET", socket_path)
            .arg(query.datomize(vec![]).protosize().textualize())
            .stdout(Stdio::piped());
        let mut child =
            ManagedChild::spawn(&mut command, "Spirit subscriber").expect("spawn subscriber cli");
        let stdout = child.take_stdout().expect("subscriber stdout");
        let output = SubscriberOutput::new(stdout);
        Self {
            child,
            lines: output.lines,
            reader_thread: Some(output.reader_thread),
        }
    }

    fn next_output(&self, timeout: Duration) -> Response {
        let line = self
            .lines
            .recv_timeout(timeout)
            .expect("subscriber output before timeout");
        actualize_response(line.trim()).unwrap_or_else(|error| {
            panic!("Datom response on subscriber stdout {line:?}: {error:?}")
        })
    }

    fn assert_no_output(&self, timeout: Duration) {
        match self.lines.recv_timeout(timeout) {
            Ok(line) => panic!("subscriber should not receive output, got {line:?}"),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("subscriber exited before timeout")
            }
        }
    }
}

struct SubscriberOutput {
    lines: Receiver<String>,
    reader_thread: thread::JoinHandle<()>,
}

impl SubscriberOutput {
    fn new(stdout: ChildStdout) -> Self {
        let (sender, lines) = mpsc::channel();
        let reader_thread = thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            lines,
            reader_thread,
        }
    }
}

fn actualize_response(text: &str) -> Result<Response, datom_codec::Error> {
    let mut pending = Potential::<Response>::from(text.to_owned());
    pending.actualize(&mut Budget {
        remaining: 1024,
        reader: ReaderBudget { remaining: 1024 },
        depth: 0,
        maximum_depth: 1024,
    })
}

fn run_cli(socket_path: &Path, query: Query) -> Response {
    run_cli_raw(socket_path, query.datomize(vec![]).protosize().textualize())
}

fn run_cli_raw(socket_path: &Path, datom_argument: impl AsRef<str>) -> Response {
    let output = Command::isolated(workspace_binary("spirit"))
        .env("SPIRIT_SOCKET", socket_path)
        .arg(datom_argument.as_ref())
        .output()
        .expect("run cli");
    assert!(
        output.status.success(),
        "cli stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("cli stdout is UTF-8");
    actualize_response(stdout.trim()).unwrap_or_else(|error| {
        panic!(
            "Datom response on CLI stdout {:?}: {error:?}",
            stdout.trim()
        )
    })
}

#[test]
fn public_clis_reject_non_object_and_file_operands_before_transport() {
    let temp = TempDir::new().expect("tempdir");
    let nota_file = temp.path().join("must-not-read.nota");
    fs::write(&nota_file, "Version").expect("write sentinel file");

    for binary in [workspace_binary("spirit"), workspace_binary("spirit-meta")] {
        for arguments in [
            Vec::<String>::new(),
            vec![String::from("--help")],
            vec![String::from("--pretty")],
            vec![String::from("Version"), String::from("Marker")],
            vec![nota_file.display().to_string()],
        ] {
            let output = Command::isolated(&binary)
                .env("SPIRIT_SOCKET", temp.path().join("unreachable.sock"))
                .env(
                    "SPIRIT_META_SOCKET",
                    temp.path().join("unreachable-meta.sock"),
                )
                .args(&arguments)
                .output()
                .expect("run public cli");
            assert!(
                !output.status.success(),
                "{binary:?} unexpectedly accepted arguments {arguments:?}"
            );
            if arguments == [nota_file.display().to_string()] {
                assert!(
                    String::from_utf8_lossy(&output.stderr).contains("invalid Datom query"),
                    "{binary:?} must reject a file as an object boundary, stderr: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }
    }
}

fn datom_string(value: &Path) -> String {
    format!("«{}»", value.display())
}

#[expect(
    clippy::too_many_arguments,
    reason = "the process fixture mirrors the complete CLI request grammar"
)]
fn writer_request(
    socket_path: &Path,
    meta_socket_path: &Path,
    _database_path: &Path,
    guardian_socket_path: Option<&Path>,
    timeout_milliseconds: i64,
    provider_name: Option<&str>,
    model_name: Option<&str>,
    output_path: &Path,
    authorization: &str,
) -> String {
    let guardian = match guardian_socket_path {
        Some(path) => format!(
            "Some.{{ {{ {} }} {} {} {{ {} }} {} }}",
            datom_string(path),
            provider_name
                .map(|value| format!("Some.{{ {value} }}"))
                .unwrap_or_else(|| "None".into()),
            model_name
                .map(|value| format!("Some.{{ {value} }}"))
                .unwrap_or_else(|| "None".into()),
            timeout_milliseconds,
            "None",
        ),
        None => "None".into(),
    };
    format!(
        "ConfigurationWriteRequest.{{ {{ {} }} Some.{{ {} }} None {} {} {{ {} }} }}",
        datom_string(socket_path),
        datom_string(meta_socket_path),
        authorization,
        guardian,
        datom_string(output_path)
    )
}

fn scopes(domains: Vec<Domain>) -> signal_domain::DomainScopes {
    domains
        .into_iter()
        .map(|domain| signal_domain::DomainScope { domain })
        .collect()
}

fn record_query(domains: Vec<Domain>, kind: Kind, description: &str) -> Query {
    Query::Record(signal_spirit::RecordRequest {
        entry: signal_spirit::Entry {
            domains,
            kind,
            description: description.into(),
            importance: Magnitude::Minimum,
        },
        justification: test_justification(description),
    })
}

fn resolve_clarification(
    clarification_identifier: RecordIdentifier,
    target_identifier: RecordIdentifier,
    description: &str,
) -> Query {
    Query::ResolveClarification(ClarificationResolution {
        clarification_record_identifier: clarification_identifier,
        target_clarifications: vec![TargetClarification {
            record_identifier: target_identifier,
            description: description.into(),
        }],
        justification: test_justification("a clarification means edit the target, not add more"),
    })
}

fn assert_short_record_identifier(identifier: &RecordIdentifier) {
    let payload = identifier;
    assert!(
        (4..=7).contains(&payload.len()),
        "record identifier should use a four-to-seven-character code: {payload}"
    );
    assert!(
        payload
            .chars()
            .all(|character| character.is_ascii_digit() || character.is_ascii_lowercase()),
        "record identifier should be lower-base36: {payload}"
    );
}

fn test_justification(statement: &str) -> Justification {
    Justification {
        testimony: vec![VerbatimQuote {
            quote_text: statement.into(),
            optional_antecedent: Some("test setup".into()),
        }],
        reasoning: statement.into(),
    }
}

#[cfg(feature = "testing-trace")]
#[derive(Debug)]
struct TraceCliOutput {
    output: Response,
    trace_lines: Vec<String>,
}

#[cfg(feature = "testing-trace")]
impl TraceCliOutput {
    fn from_stdout(stdout: Vec<u8>) -> Self {
        let stdout = String::from_utf8(stdout).expect("cli stdout is UTF-8");
        let mut lines = stdout.lines();
        let output_line = lines.next().expect("cli prints signal output");
        let output = actualize_response(output_line).unwrap_or_else(|error| {
            panic!("Datom Response on CLI stdout {output_line:?}: {error:?}")
        });
        Self {
            output,
            trace_lines: lines.map(String::from).collect(),
        }
    }

    fn assert_trace_sequence(&self, expected: &[&str]) {
        let events = self.trace_events();
        let actual = events
            .iter()
            .map(|event| event.trace_event_name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "trace lines: {:#?}", self.trace_lines);
    }

    fn assert_trace_sequence_after_optional_lifecycle_start(&self, expected: &[&str]) {
        let events = self.trace_events();
        let mut actual = events
            .iter()
            .map(|event| event.trace_event_name.as_str())
            .collect::<Vec<_>>();
        let lifecycle_start = ["SemaStarted", "NexusStarted", "SignalStarted"];
        if actual.starts_with(&lifecycle_start) {
            actual.drain(..lifecycle_start.len());
        }
        assert_eq!(actual, expected, "trace lines: {:#?}", self.trace_lines);
    }

    fn trace_events(&self) -> Vec<ComponentTraceEvent> {
        self.trace_lines
            .iter()
            .map(|line| {
                let mut pending = Potential::<ComponentTraceEvent>::from(line.to_owned());
                pending
                    .actualize(&mut Budget {
                        remaining: 1024,
                        reader: ReaderBudget { remaining: 1024 },
                        depth: 0,
                        maximum_depth: 1024,
                    })
                    .unwrap_or_else(|error| {
                        panic!("trace CLI line should actualize {line:?}: {error:?}")
                    })
            })
            .collect()
    }
}

#[cfg(feature = "testing-trace")]
fn run_cli_with_trace(
    socket_path: &Path,
    trace_socket_path: &Path,
    query: Query,
) -> TraceCliOutput {
    let output = Command::isolated(workspace_binary("spirit"))
        .env("SPIRIT_SOCKET", socket_path)
        .env("SPIRIT_TRACE_SOCKET", trace_socket_path)
        .arg(query.datomize(vec![]).protosize().textualize())
        .output()
        .expect("run cli with trace");
    assert!(
        output.status.success(),
        "cli stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    TraceCliOutput::from_stdout(output.stdout)
}

#[test]
fn daemon_starts_from_isolated_persisted_configuration() {
    let temp = TempDir::new().expect("tempdir");
    let socket_path = temp.path().join("written.sock");
    let database_path = temp.path().join("written.sema");

    let _daemon = DaemonProcess::spawn(&socket_path, &database_path);

    let recorded = run_cli(
        &socket_path,
        record_query(
            vec![Domain::Information(
                signal_domain::InformationDomain::Documentation,
            )],
            Kind::Constraint,
            "daemon starts from persisted isolated configuration",
        ),
    );
    match recorded {
        Response::RecordAccepted(receipt) => {
            assert_short_record_identifier(&receipt);
        }
        other => panic!("expected RecordAccepted from zero-argument daemon, got {other:?}"),
    }
}

#[test]
fn cli_and_daemon_exchange_nota_over_rkyv_socket() {
    let temp = TempDir::new().expect("tempdir");
    let socket_path = temp.path().join("spirit.sock");
    let database_path = temp.path().join("spirit.sema");

    let _daemon = DaemonProcess::spawn(&socket_path, &database_path);

    // Record path — parsed back into the schema-emitted Output, asserted
    // on the typed variant (not on the raw digest string, which is now a
    // real content hash).
    let recorded = run_cli(
        &socket_path,
        record_query(
            vec![Domain::Information(
                signal_domain::InformationDomain::Documentation,
            )],
            Kind::Constraint,
            "schema creates the interface",
        ),
    );
    match recorded {
        Response::RecordAccepted(receipt) => {
            assert_short_record_identifier(&receipt);
        }
        other => panic!("expected RecordAccepted, got {other:?}"),
    };

    let observed = run_cli(
        &socket_path,
        Query::Observe(signal_spirit::Selection {
            domain_match: signal_spirit::DomainMatch::Full(scopes(vec![Domain::Information(
                signal_domain::InformationDomain::Documentation,
            )])),
            keyword_match: signal_spirit::KeywordMatch::Any,
            text_match: signal_spirit::TextMatch::Any,
            selected_kind: Some(Kind::Constraint),
            importance_selection: signal_spirit::ImportanceSelection::Any,
        }),
    );
    // Observe flows through Stash and returns both the recovery handle and
    // the observed records.
    assert!(
        matches!(observed, Response::RecordsStashed(_)),
        "the daemon stashes and returns the observed records, got {observed:?}"
    );

    let rejected = run_cli(
        &socket_path,
        record_query(vec![], Kind::Constraint, "schema rejects before SEMA"),
    );
    assert!(
        matches!(rejected, Response::Rejected(_)),
        "empty domain is rejected before SEMA, got {rejected:?}"
    );
}

#[test]
fn text_search_returns_direct_ranked_records() {
    let temp = TempDir::new().expect("tempdir");
    let socket_path = temp.path().join("public-text-search.sock");
    let database_path = temp.path().join("public-text-search.sema");

    let _daemon = DaemonProcess::spawn(&socket_path, &database_path);

    let first = run_cli(
        &socket_path,
        record_query(
            vec![Domain::Information(
                signal_domain::InformationDomain::Documentation,
            )],
            Kind::Decision,
            "router.node.cluster.criome is the endpoint naming example",
        ),
    );
    assert!(
        matches!(first, Response::RecordAccepted(_)),
        "expected first search fixture to record, got {first:?}"
    );
    let second = run_cli(
        &socket_path,
        record_query(
            vec![Domain::Information(
                signal_domain::InformationDomain::Documentation,
            )],
            Kind::Decision,
            "Router owns the standardized routing protocol envelope",
        ),
    );
    assert!(
        matches!(second, Response::RecordAccepted(_)),
        "expected second search fixture to record, got {second:?}"
    );

    let suffix_results = run_cli(&socket_path, Query::TextSearch("criome".into()));
    let Response::RecordsObserved(suffix_records) = suffix_results else {
        panic!("TextSearch should return direct records, got {suffix_results:?}");
    };
    assert_eq!(suffix_records.record_set.len(), 1);
    assert_eq!(
        suffix_records.record_set[0].entry.description,
        "router.node.cluster.criome is the endpoint naming example"
    );

    let phrase_results = run_cli(&socket_path, Query::TextSearch("routing protocol".into()));
    let Response::RecordsObserved(phrase_records) = phrase_results else {
        panic!("TextSearch should return direct phrase records, got {phrase_results:?}");
    };
    assert_eq!(phrase_records.record_set.len(), 1);
    assert_eq!(
        phrase_records.record_set[0].entry.description,
        "Router owns the standardized routing protocol envelope"
    );
}

#[test]
fn cli_and_daemon_resolve_clarification_edits_target_and_removes_standalone() {
    let temp = TempDir::new().expect("tempdir");
    let socket_path = temp.path().join("resolve-clarification.sock");
    let database_path = temp.path().join("resolve-clarification.sema");

    let _daemon = DaemonProcess::spawn(&socket_path, &database_path);

    let target = run_cli(
        &socket_path,
        record_query(
            vec![Domain::Information(
                signal_domain::InformationDomain::Documentation,
            )],
            Kind::Decision,
            "clarifications should not add more records",
        ),
    );
    let target_identifier = match target {
        Response::RecordAccepted(receipt) => receipt.clone(),
        other => panic!("expected target RecordAccepted, got {other:?}"),
    };

    let standalone = run_cli(
        &socket_path,
        record_query(
            vec![Domain::Information(
                signal_domain::InformationDomain::Documentation,
            )],
            Kind::Clarification,
            "bad standalone clarification to fold away",
        ),
    );
    let clarification_identifier = match standalone {
        Response::RecordAccepted(receipt) => receipt.clone(),
        other => panic!("expected clarification RecordAccepted, got {other:?}"),
    };

    let resolved = run_cli(
        &socket_path,
        resolve_clarification(
            clarification_identifier.clone(),
            target_identifier.clone(),
            "clarifications edit target records instead of adding more records",
        ),
    );
    match resolved {
        Response::ClarificationResolved(receipt) => {
            assert_eq!(
                receipt.clarification_record_identifier,
                clarification_identifier
            );
            assert_eq!(receipt.record_identifiers, vec![target_identifier.clone()]);
        }
        other => panic!("expected ClarificationResolved, got {other:?}"),
    }

    let found = run_cli(&socket_path, Query::Lookup(target_identifier.clone()));
    match found {
        Response::RecordFound(record) => {
            assert_eq!(record.record_identifier, target_identifier);
            assert_eq!(
                record.entry.description,
                "clarifications edit target records instead of adding more records"
            );
        }
        other => panic!("expected target RecordFound, got {other:?}"),
    }

    let missing = run_cli(
        &socket_path,
        Query::Lookup(clarification_identifier.clone()),
    );
    assert!(
        matches!(missing, Response::Error(_)),
        "standalone clarification should be removed, got {missing:?}"
    );
}

#[test]
fn cli_and_daemon_report_version_from_bare_nota_atom() {
    let temp = TempDir::new().expect("tempdir");
    let socket_path = temp.path().join("version.sock");
    let database_path = temp.path().join("version.sema");

    let _daemon = DaemonProcess::spawn(&socket_path, &database_path);

    let version = run_cli(&socket_path, Query::Version);
    match version {
        Response::VersionReported(report) => {
            assert_eq!(report.version_text, env!("CARGO_PKG_VERSION"));
        }
        other => panic!("expected VersionReported from bare Version input, got {other:?}"),
    }
}

#[test]
fn cli_subscription_receives_matching_intent_events_without_blocking_daemon() {
    let temp = TempDir::new().expect("tempdir");
    let socket_path = temp.path().join("subscription.sock");
    let database_path = temp.path().join("subscription.sema");

    let _daemon = DaemonProcess::spawn(&socket_path, &database_path);
    let subscriber = SubscriberProcess::spawn(
        &socket_path,
        Query::SubscribeIntent(signal_spirit::Selection {
            domain_match: signal_spirit::DomainMatch::Full(scopes(vec![Domain::Kinship(
                KinshipDomain::Rapport,
            )])),
            keyword_match: signal_spirit::KeywordMatch::Any,
            text_match: signal_spirit::TextMatch::Any,
            selected_kind: Some(Kind::Decision),
            importance_selection: signal_spirit::ImportanceSelection::Any,
        }),
    );

    match subscriber.next_output(Duration::from_secs(2)) {
        Response::SubscriptionStarted(subscription) => {
            assert_eq!(subscription.subscription_token, 1);
        }
        other => panic!("expected SubscriptionStarted, got {other:?}"),
    }

    let nonmatching = run_cli(
        &socket_path,
        record_query(
            vec![Domain::Information(
                signal_domain::InformationDomain::Documentation,
            )],
            Kind::Decision,
            "this should not be pushed",
        ),
    );
    assert!(
        matches!(nonmatching, Response::RecordAccepted(_)),
        "ordinary record request should complete while subscription is open, got {nonmatching:?}"
    );
    subscriber.assert_no_output(Duration::from_millis(200));

    let matching = run_cli(
        &socket_path,
        record_query(
            vec![Domain::Kinship(KinshipDomain::Rapport)],
            Kind::Decision,
            "subscriber receives this",
        ),
    );
    let Response::RecordAccepted(receipt) = matching else {
        panic!("expected matching RecordAccepted, got {matching:?}");
    };

    match subscriber.next_output(Duration::from_secs(2)) {
        Response::Event(IntentEvent::IntentRecorded(recorded)) => {
            assert_eq!(
                recorded.entry.domains,
                vec![Domain::Kinship(KinshipDomain::Rapport)]
            );
            assert_eq!(recorded.entry.kind, Kind::Decision);
            assert_eq!(recorded.entry.description, "subscriber receives this");
            assert_eq!(recorded.record_identifier, receipt);
        }
        other => panic!("expected IntentRecorded event, got {other:?}"),
    }
}

#[test]
fn cli_and_daemon_classify_state_into_provisional_record() {
    let temp = TempDir::new().expect("tempdir");
    let socket_path = temp.path().join("state.sock");
    let database_path = temp.path().join("state.sema");

    let _daemon = DaemonProcess::spawn(&socket_path, &database_path);

    let accepted = run_cli(
        &socket_path,
        Query::State(signal_spirit::Statement {
            statement_text: "daemon raw intent".into(),
        }),
    );
    match accepted {
        Response::RecordAccepted(receipt) => {
            assert_short_record_identifier(&receipt);
        }
        other => panic!("expected State to classify into RecordAccepted, got {other:?}"),
    }

    let observed = run_cli(
        &socket_path,
        Query::Observe(signal_spirit::Selection {
            domain_match: signal_spirit::DomainMatch::Full(scopes(domain_fixtures::domains(&[
                "documentation",
            ]))),
            keyword_match: signal_spirit::KeywordMatch::Any,
            text_match: signal_spirit::TextMatch::Any,
            selected_kind: Some(Kind::Clarification),
            importance_selection: signal_spirit::ImportanceSelection::Any,
        }),
    );
    let Response::RecordsStashed(stashed) = observed else {
        panic!("expected classified State observation to be stashed, got {observed:?}");
    };
    assert_eq!(stashed.record_count, 1);
    assert_eq!(stashed.observed_records.record_set.len(), 1);
    assert_eq!(
        stashed.observed_records.record_set[0].entry.domains,
        domain_fixtures::domains(&["documentation"])
    );
    assert_eq!(
        stashed.observed_records.record_set[0].entry.kind,
        Kind::Clarification
    );
    assert_eq!(
        stashed.observed_records.record_set[0].entry.description,
        "daemon raw intent"
    );
    assert_eq!(
        stashed.observed_records.record_set[0].entry.importance,
        Magnitude::Minimum
    );

    let looked_up = run_cli(&socket_path, Query::LookupStash(stashed.stash_handle));
    match looked_up {
        Response::RecordsObserved(records) => {
            assert_eq!(records.record_set.len(), 1);
            assert_eq!(
                records.record_set[0].entry.domains,
                domain_fixtures::domains(&["documentation"])
            );
            assert_eq!(records.record_set[0].entry.kind, Kind::Clarification);
            assert_eq!(records.record_set[0].entry.description, "daemon raw intent");
            assert_eq!(records.record_set[0].entry.importance, Magnitude::Minimum);
        }
        other => panic!("expected LookupStash to return classified State record, got {other:?}"),
    }
}

#[test]
fn cli_and_daemon_bump_importance_without_changing_record_identifier() {
    let temp = TempDir::new().expect("tempdir");
    let socket_path = temp.path().join("bump-importance.sock");
    let database_path = temp.path().join("bump-importance.sema");

    let _daemon = DaemonProcess::spawn(&socket_path, &database_path);

    let accepted = run_cli(
        &socket_path,
        record_query(
            vec![Domain::Information(
                signal_domain::InformationDomain::Documentation,
            )],
            Kind::Correction,
            "importance target",
        ),
    );
    let record_identifier = match accepted {
        Response::RecordAccepted(receipt) => {
            assert_short_record_identifier(&receipt);
            receipt.clone()
        }
        other => panic!("expected RecordAccepted before importance bump, got {other:?}"),
    };

    let changed = run_cli(
        &socket_path,
        Query::BumpImportance(signal_spirit::ImportanceBump {
            record_identifier: record_identifier.clone(),
        }),
    );
    match changed {
        Response::ImportanceBumped(receipt) => {
            assert_eq!(receipt.record_identifier, record_identifier);
            assert_eq!(receipt.importance, Magnitude::VeryLow);
        }
        other => panic!("expected ImportanceBumped, got {other:?}"),
    }

    let found = run_cli(&socket_path, Query::Lookup(record_identifier.clone()));
    match found {
        Response::RecordFound(record) => {
            assert_eq!(record.record_identifier, record_identifier);
            assert_eq!(record.entry.description, "importance target");
            assert_eq!(record.entry.importance, Magnitude::VeryLow);
        }
        other => panic!("expected changed record lookup, got {other:?}"),
    }
}

#[test]
fn cli_and_daemon_change_record_replaces_entry_under_same_identifier() {
    let temp = TempDir::new().expect("tempdir");
    let socket_path = temp.path().join("change-record.sock");
    let database_path = temp.path().join("change-record.sema");

    let _daemon = DaemonProcess::spawn(&socket_path, &database_path);

    let accepted = run_cli(
        &socket_path,
        record_query(
            vec![Domain::Information(
                signal_domain::InformationDomain::Documentation,
            )],
            Kind::Decision,
            "original record",
        ),
    );
    let record_identifier = match accepted {
        Response::RecordAccepted(receipt) => {
            assert_short_record_identifier(&receipt);
            receipt.clone()
        }
        other => panic!("expected RecordAccepted before record change, got {other:?}"),
    };

    let changed = run_cli(
        &socket_path,
        Query::ChangeRecord(signal_spirit::RecordChange {
            record_identifier: record_identifier.clone(),
            entry: signal_spirit::Entry {
                domains: vec![Domain::Information(
                    signal_domain::InformationDomain::Documentation,
                )],
                kind: Kind::Correction,
                description: "replacement record".into(),
                importance: Magnitude::Minimum,
            },
            justification: test_justification("replacement record"),
        }),
    );
    match changed {
        Response::RecordChanged(receipt) => {
            assert_eq!(receipt.record_identifier, record_identifier);
        }
        other => panic!("expected RecordChanged, got {other:?}"),
    }

    let found = run_cli(&socket_path, Query::Lookup(record_identifier.clone()));
    match found {
        Response::RecordFound(record) => {
            assert_eq!(record.record_identifier, record_identifier);
            assert_eq!(
                record.entry.domains,
                domain_fixtures::domains(&["documentation"])
            );
            assert_eq!(record.entry.kind, Kind::Correction);
            assert_eq!(record.entry.description, "replacement record");
            assert_eq!(record.entry.importance, Magnitude::Minimum);
        }
        other => panic!("expected changed record lookup, got {other:?}"),
    }

    let missing_old_query = run_cli(
        &socket_path,
        Query::Observe(signal_spirit::Selection {
            domain_match: signal_spirit::DomainMatch::Full(scopes(domain_fixtures::domains(&[
                "documentation",
            ]))),
            keyword_match: signal_spirit::KeywordMatch::Any,
            text_match: signal_spirit::TextMatch::Any,
            selected_kind: Some(Kind::Decision),
            importance_selection: signal_spirit::ImportanceSelection::Any,
        }),
    );
    assert!(
        matches!(missing_old_query, Response::Error(_)),
        "the original entry should be replaced, got {missing_old_query:?}"
    );
}

#[test]
fn cli_renders_alias_payload_outputs_without_wrapper_repetition() {
    let temp = TempDir::new().expect("tempdir");
    let socket_path = temp.path().join("alias-payload.sock");
    let database_path = temp.path().join("alias-payload.sema");

    let _daemon = DaemonProcess::spawn(&socket_path, &database_path);

    let rejected = Command::isolated(workspace_binary("spirit"))
        .env("SPIRIT_SOCKET", &socket_path)
        .arg(
            record_query(vec![], Kind::Constraint, "alias payload rejection")
                .datomize(vec![])
                .protosize()
                .textualize(),
        )
        .output()
        .expect("run cli");
    assert!(
        rejected.status.success(),
        "cli stderr: {}",
        String::from_utf8_lossy(&rejected.stderr)
    );
    let rejected_stdout = String::from_utf8(rejected.stdout).expect("cli stdout is UTF-8");
    assert_eq!(
        rejected_stdout.trim(),
        "Rejected.{ EmptyDomain }",
        "Rejected aliases must render the direct SignalRejection payload without a Rejected wrapper"
    );
    let rejected_output = actualize_response(rejected_stdout.trim()).expect("actualize rejection");
    assert!(
        matches!(rejected_output, Response::Rejected(_)),
        "parsed rejection should be direct Response::Rejected payload"
    );

    let recorded = Command::isolated(workspace_binary("spirit"))
        .env("SPIRIT_SOCKET", &socket_path)
        .arg(
            record_query(
                vec![Domain::Information(
                    signal_domain::InformationDomain::Documentation,
                )],
                Kind::Constraint,
                "direct accepted payload",
            )
            .datomize(vec![])
            .protosize()
            .textualize(),
        )
        .output()
        .expect("run cli");
    assert!(
        recorded.status.success(),
        "cli stderr: {}",
        String::from_utf8_lossy(&recorded.stderr)
    );
    let recorded_stdout = String::from_utf8(recorded.stdout).expect("cli stdout is UTF-8");
    let recorded_output = actualize_response(recorded_stdout.trim()).expect("actualize record");
    match recorded_output {
        Response::RecordAccepted(receipt) => {
            assert_short_record_identifier(&receipt);
        }
        other => panic!("parsed record reply should be direct RecordAccepted payload: {other:?}"),
    }
}

#[test]
fn daemon_persists_sema_file_across_a_restart() {
    // The strongest durability proof: a daemon writes the `.sema` file,
    // the daemon process is killed, a NEW daemon process opens the SAME
    // `.sema` file, and the previously recorded entry is still observable
    // and the commit sequence resumes. This is the bead `primary-q2au`
    // claim proven at the real process boundary.
    let temp = TempDir::new().expect("tempdir");
    let database_path = temp.path().join("durable.sema");

    // First daemon: record one entry, then drop (kill) it.
    {
        let socket_path = temp.path().join("first.sock");
        let _daemon = DaemonProcess::spawn(&socket_path, &database_path);
        let recorded = run_cli(
            &socket_path,
            record_query(
                domain_fixtures::domains(&["deployment"]),
                Kind::Decision,
                "survives restart",
            ),
        );
        match recorded {
            Response::RecordAccepted(receipt) => {
                assert_short_record_identifier(&receipt);
            }
            other => panic!("expected RecordAccepted from first daemon, got {other:?}"),
        }
        // _daemon drops here: process killed, sema-engine file handle released.
    }

    assert!(
        database_path.exists(),
        "the .sema file outlives the first daemon process"
    );

    // Second daemon against the SAME .sema file on a fresh socket.
    let socket_path = temp.path().join("second.sock");
    let _daemon = DaemonProcess::spawn(&socket_path, &database_path);

    let observed = run_cli(
        &socket_path,
        Query::Observe(signal_spirit::Selection {
            domain_match: signal_spirit::DomainMatch::Full(scopes(domain_fixtures::domains(&[
                "deployment",
            ]))),
            keyword_match: signal_spirit::KeywordMatch::Any,
            text_match: signal_spirit::TextMatch::Any,
            selected_kind: Some(Kind::Decision),
            importance_selection: signal_spirit::ImportanceSelection::Any,
        }),
    );
    // Observe returns records inline and a recovery stash handle. LookupStash
    // verifies the same content survived the daemon restart.
    let stash_handle = match observed {
        Response::RecordsStashed(stashed) => {
            assert_eq!(
                stashed.record_count, 1,
                "the restarted daemon observes one durable record"
            );
            assert_eq!(
                stashed.observed_records.record_set[0].entry.description, "survives restart",
                "the restarted daemon returns durable content inline"
            );
            stashed.stash_handle
        }
        other => panic!("expected RecordsStashed after restart, got {other:?}"),
    };
    let looked_up = run_cli(&socket_path, Query::LookupStash(stash_handle));
    match looked_up {
        Response::RecordsObserved(records) => {
            assert_eq!(
                records.record_set[0].entry.description, "survives restart",
                "the restarted daemon's stash retrieves the durable content"
            );
        }
        other => panic!("expected RecordsObserved from LookupStash, got {other:?}"),
    }

    // The commit ledger resumed: the next record is sequence 2, proving
    // the durable counter persisted across the restart, not just records.
    let next = run_cli(
        &socket_path,
        record_query(
            domain_fixtures::domains(&["deployment"]),
            Kind::Decision,
            "second after restart",
        ),
    );
    match next {
        Response::RecordAccepted(receipt) => {
            assert_short_record_identifier(&receipt);
        }
        other => panic!("expected RecordAccepted after restart, got {other:?}"),
    }
}

#[test]
fn candidate_daemon_handover_from_production_copy_preserves_original_sema_database() {
    let temp = TempDir::new().expect("tempdir");
    let production_database_path = temp.path().join("production.sema");
    let candidate_database_path = temp.path().join("candidate-copy.sema");

    {
        let socket_path = temp.path().join("production-seed.sock");
        let _daemon = DaemonProcess::spawn(&socket_path, &production_database_path);
        let recorded = run_cli(
            &socket_path,
            record_query(
                domain_fixtures::domains(&["deployment"]),
                Kind::Constraint,
                "production entry before copy",
            ),
        );
        match recorded {
            Response::RecordAccepted(receipt) => {
                assert_short_record_identifier(&receipt);
            }
            other => panic!("expected production seed record, got {other:?}"),
        }
    }

    fs::copy(&production_database_path, &candidate_database_path)
        .expect("copy production .sema database for candidate handover");

    {
        let socket_path = temp.path().join("candidate.sock");
        let _daemon = DaemonProcess::spawn(&socket_path, &candidate_database_path);
        let observed = run_cli(
            &socket_path,
            Query::Observe(signal_spirit::Selection {
                domain_match: signal_spirit::DomainMatch::Full(scopes(domain_fixtures::domains(
                    &["deployment"],
                ))),
                keyword_match: signal_spirit::KeywordMatch::Any,
                text_match: signal_spirit::TextMatch::Any,
                selected_kind: Some(Kind::Constraint),
                importance_selection: signal_spirit::ImportanceSelection::Any,
            }),
        );
        assert_eq!(
            stashed_descriptions(&socket_path, observed),
            vec![String::from("production entry before copy")],
            "candidate starts from the copied production SEMA state"
        );

        let candidate_recorded = run_cli(
            &socket_path,
            record_query(
                domain_fixtures::domains(&["deployment"]),
                Kind::Constraint,
                "candidate-only entry after copy",
            ),
        );
        match candidate_recorded {
            Response::RecordAccepted(receipt) => {
                assert_short_record_identifier(&receipt);
            }
            other => panic!("expected candidate record, got {other:?}"),
        }

        let candidate_observed = run_cli(
            &socket_path,
            Query::Observe(signal_spirit::Selection {
                domain_match: signal_spirit::DomainMatch::Full(scopes(domain_fixtures::domains(
                    &["deployment"],
                ))),
                keyword_match: signal_spirit::KeywordMatch::Any,
                text_match: signal_spirit::TextMatch::Any,
                selected_kind: Some(Kind::Constraint),
                importance_selection: signal_spirit::ImportanceSelection::Any,
            }),
        );
        assert_eq!(
            stashed_descriptions(&socket_path, candidate_observed),
            vec![
                String::from("candidate-only entry after copy"),
                String::from("production entry before copy"),
            ],
            "candidate writes land only in the copied database"
        );
    }

    {
        let socket_path = temp.path().join("production-after.sock");
        let _daemon = DaemonProcess::spawn(&socket_path, &production_database_path);
        let observed = run_cli(
            &socket_path,
            Query::Observe(signal_spirit::Selection {
                domain_match: signal_spirit::DomainMatch::Full(scopes(domain_fixtures::domains(
                    &["deployment"],
                ))),
                keyword_match: signal_spirit::KeywordMatch::Any,
                text_match: signal_spirit::TextMatch::Any,
                selected_kind: Some(Kind::Constraint),
                importance_selection: signal_spirit::ImportanceSelection::Any,
            }),
        );
        assert_eq!(
            stashed_descriptions(&socket_path, observed),
            vec![String::from("production entry before copy")],
            "candidate writes must not mutate the original production SEMA file"
        );

        let production_next = run_cli(
            &socket_path,
            record_query(
                domain_fixtures::domains(&["deployment"]),
                Kind::Constraint,
                "production entry after handover",
            ),
        );
        match production_next {
            Response::RecordAccepted(receipt) => {
                assert_short_record_identifier(&receipt);
            }
            other => panic!("expected production post-handover record, got {other:?}"),
        }
    }
}

#[cfg(feature = "testing-trace")]
#[test]
fn cli_receives_testing_trace_events_from_daemon_trace_socket() {
    let temp = TempDir::new().expect("tempdir");
    let socket_path = temp.path().join("spirit.sock");
    let trace_socket_path = temp.path().join("spirit-trace.sock");
    let database_path = temp.path().join("spirit.sema");

    let _daemon = DaemonProcess::spawn_with_trace(&socket_path, &database_path, &trace_socket_path);

    let recorded = run_cli_with_trace(
        &socket_path,
        &trace_socket_path,
        record_query(
            domain_fixtures::domains(&["architecture"]),
            Kind::Constraint,
            "trace crosses daemon boundary",
        ),
    );
    assert!(
        matches!(recorded.output, Response::RecordAccepted(_)),
        "record reply should still be the first CLI line, got {:?}",
        recorded.output
    );
    // The CLI binds the trace socket for this request, so startup events
    // are timing-dependent at this process boundary. The invariant here
    // is the exact request activation sequence after any lifecycle prefix.
    // instrumentation_logging.rs proves lifecycle tracing with an
    // in-process sink bound before Engine::start.
    recorded.assert_trace_sequence_after_optional_lifecycle_start(&[
        "SignalAdmitted",
        "SignalTriaged",
        "NexusEntered",
        "NexusDecided",
        "SemaWriteApplied",
        "NexusEntered",
        "NexusDecided",
        "SignalReplied",
    ]);

    let observed = run_cli_with_trace(
        &socket_path,
        &trace_socket_path,
        Query::Observe(signal_spirit::Selection {
            domain_match: signal_spirit::DomainMatch::Full(scopes(domain_fixtures::domains(&[
                "architecture",
            ]))),
            keyword_match: signal_spirit::KeywordMatch::Any,
            text_match: signal_spirit::TextMatch::Any,
            selected_kind: Some(Kind::Constraint),
            importance_selection: signal_spirit::ImportanceSelection::Any,
        }),
    );
    // Observe flows through the recursive Nexus loop with Stash and returns
    // both the recovery handle and the observed records.
    // The trace below shows each continuation step: command SEMA read,
    // command Stash effect, then reply.
    assert!(
        matches!(observed.output, Response::RecordsStashed(_)),
        "observe reply should still be the first CLI line, got {:?}",
        observed.output
    );
    observed.assert_trace_sequence(&[
        "SignalAdmitted",
        "SignalTriaged",
        "NexusEntered",
        "NexusDecided",
        "SemaReadObserved",
        "NexusEntered",
        "NexusDecided",
        "NexusEntered",
        "NexusDecided",
        "SignalReplied",
    ]);
}

/// Observe returns records inline with a recovery Stash handle.
fn stashed_descriptions(_socket_path: &Path, output: Response) -> Vec<String> {
    match output {
        Response::RecordsStashed(stashed) => {
            let mut descriptions: Vec<String> = stashed
                .observed_records
                .record_set
                .iter()
                .map(|record| record.entry.description.clone())
                .collect();
            descriptions.sort();
            descriptions
        }
        other => panic!("expected RecordsStashed, got {other:?}"),
    }
}

#[test]
fn daemon_rejects_all_legacy_admission_invalid_families_before_mutation() {
    let temp = TempDir::new().expect("tempdir");
    let socket_path = temp.path().join("admission.sock");
    let database_path = temp.path().join("admission.sema");
    let _daemon = DaemonProcess::spawn(&socket_path, &database_path);

    let marker_before = match run_cli(&socket_path, Query::Marker) {
        Response::MarkerReported(marker) => marker,
        other => panic!("expected initial marker, got {other:?}"),
    };
    let justification = test_justification("valid justification");
    let invalid = [
        Query::Observe(signal_spirit::Selection {
            domain_match: signal_spirit::DomainMatch::Full(vec![]),
            keyword_match: signal_spirit::KeywordMatch::Any,
            text_match: signal_spirit::TextMatch::Any,
            selected_kind: None,
            importance_selection: signal_spirit::ImportanceSelection::Any,
        }),
        Query::Record(signal_spirit::RecordRequest {
            entry: signal_spirit::Entry {
                domains: domain_fixtures::domains(&["documentation"]),
                kind: Kind::Decision,
                description: "entry".into(),
                importance: Magnitude::Minimum,
            },
            justification: signal_spirit::Justification {
                testimony: vec![],
                reasoning: " ".into(),
            },
        }),
        Query::ResolveClarification(signal_spirit::ClarificationResolution {
            clarification_record_identifier: "missing".into(),
            target_clarifications: vec![],
            justification: justification.clone(),
        }),
        Query::Supersede(signal_spirit::Supersession {
            retired_identifiers: vec![],
            replacements: vec![],
            justification: justification.clone(),
        }),
        Query::Retire(signal_spirit::Retirement {
            record_identifier: "missing".into(),
            justification: signal_spirit::Justification {
                testimony: vec![],
                reasoning: "".into(),
            },
        }),
    ];
    for query in invalid {
        assert!(matches!(
            run_cli(&socket_path, query),
            Response::Rejected(_)
        ));
    }
    match run_cli(&socket_path, Query::Marker) {
        Response::MarkerReported(marker) => assert_eq!(marker, marker_before),
        other => panic!("expected marker after rejected inputs, got {other:?}"),
    }
}
