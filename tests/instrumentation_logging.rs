mod support;

use spirit::{
    AuthorizationObjectName, Engine, ObjectName, SignalObjectName, Store, TraceEvent, TraceLog,
    schema::{
        nexus::NexusObjectName,
        sema::SemaObjectName,
        signal::{
            DomainMatch, Entry, ImportanceSelection, Justification, Kind, Magnitude, Query,
            RecordRequest, Response, SignalRejection, ValidationError, VerbatimQuote,
        },
    },
};
use support::domain_fixtures;
use tempfile::TempDir;

struct SemaFile {
    #[allow(dead_code)]
    directory: TempDir,
    path: std::path::PathBuf,
}

impl SemaFile {
    fn new() -> Self {
        let directory = TempDir::new().expect("tempdir");
        let path = directory.path().join("instrumentation.sema");
        Self { directory, path }
    }

    fn engine_with_trace(&self, trace_log: TraceLog) -> Engine {
        Engine::new_with_trace(Store::open(&self.path).expect("open sema store"), trace_log)
    }
}

fn entry(description: &str) -> Entry {
    Entry {
        domains: domain_fixtures::domains(&["trace"]),
        kind: Kind::Decision,
        description: description.to_owned(),
        importance: Magnitude::Minimum,
    }
}

fn record_request(entry: Entry) -> RecordRequest {
    let statement = entry.description.clone();
    RecordRequest {
        entry,
        justification: Justification {
            testimony: vec![VerbatimQuote {
                quote_text: statement.clone(),
                optional_antecedent: None,
            }],
            reasoning: statement,
        },
    }
}

#[test]
fn testing_trace_records_real_signal_nexus_and_sema_activations() {
    let sema = SemaFile::new();
    let trace_log = TraceLog::recording();
    let mut engine = sema.engine_with_trace(trace_log.clone());

    let recorded = engine.handle(Query::Record(record_request(entry("trace witness"))));
    match recorded.root() {
        Response::RecordAccepted(receipt) => assert!(!receipt.is_empty()),
        other => panic!("expected RecordAccepted, got {other:?}"),
    };

    let observed = engine.handle(Query::Observe(spirit::schema::signal::Selection {
        domain_match: DomainMatch::Full(domain_fixtures::scopes(&["trace"])),
        keyword_match: spirit::schema::signal::KeywordMatch::Any,
        text_match: spirit::schema::signal::TextMatch::Any,
        selected_kind: Some(Kind::Decision),
        importance_selection: ImportanceSelection::Any,
    }));
    // Observe flows through Stash and returns both records and a recovery
    // handle. The trace below witnesses the recursive Nexus loop:
    // each NexusEntered/NexusDecided pair is one decision step. Observe needs
    // three steps: command SEMA read, command stash effect, reply to Signal.
    let record_marker = match observed.root() {
        Response::RecordsStashed(_) => engine.database_marker(),
        other => panic!("expected RecordsStashed, got {other:?}"),
    };

    assert_activation_names(
        &trace_log.events(),
        &[
            "SignalAdmitted",
            "SignalTriaged",
            "NexusEntered",
            "NexusDecided",
            "SemaWriteApplied",
            "NexusEntered",
            "NexusDecided",
            "SignalReplied",
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
        ],
    );

    let events = trace_log.events();
    assert_activation_objects(
        &events,
        &[
            ObjectName::Signal(SignalObjectName::Admitted),
            ObjectName::Signal(SignalObjectName::Triaged),
            ObjectName::Nexus(NexusObjectName::Entered),
            ObjectName::Nexus(NexusObjectName::Decided),
            ObjectName::Sema(SemaObjectName::WriteApplied),
            ObjectName::Nexus(NexusObjectName::Entered),
            ObjectName::Nexus(NexusObjectName::Decided),
            ObjectName::Signal(SignalObjectName::Replied),
            ObjectName::Signal(SignalObjectName::Admitted),
            ObjectName::Signal(SignalObjectName::Triaged),
            ObjectName::Nexus(NexusObjectName::Entered),
            ObjectName::Nexus(NexusObjectName::Decided),
            ObjectName::Sema(SemaObjectName::ReadObserved),
            ObjectName::Nexus(NexusObjectName::Entered),
            ObjectName::Nexus(NexusObjectName::Decided),
            ObjectName::Nexus(NexusObjectName::Entered),
            ObjectName::Nexus(NexusObjectName::Decided),
            ObjectName::Signal(SignalObjectName::Replied),
        ],
    );
    let archive =
        rkyv::to_bytes::<rkyv::rancor::Error>(&events[4]).expect("trace event archives as rkyv");
    let decoded = rkyv::from_bytes::<TraceEvent, rkyv::rancor::Error>(&archive)
        .expect("trace event decodes from rkyv");
    assert_eq!(decoded, events[4]);
    #[cfg(feature = "datom-cli")]
    {
        let rendered = events[4].to_string();
        assert_eq!(rendered, "{ Sema.WriteApplied }");
        let parsed = rendered
            .parse::<TraceEvent>()
            .expect("trace event parses from Datom");
        assert_eq!(parsed, events[4]);
    }
    assert_ne!(
        record_marker.state_digest, 0,
        "trace is attached to a real SEMA write, not a string-presence check"
    );
}

#[test]
fn authorization_trace_projects_to_spirit_authorization_event() {
    let event = TraceEvent::new(ObjectName::Authorization(AuthorizationObjectName::Observed));
    let projected = signal_introspect::ComponentTraceEvent::from(event);

    assert_eq!(
        projected.introspection_target,
        signal_introspect::IntrospectionTarget::Spirit
    );
    assert_eq!(
        projected.trace_layer,
        signal_introspect::TraceLayer::Authorization
    );
    assert_eq!(projected.trace_event_name, "AuthorizationObserved");
}

#[test]
fn testing_trace_records_lifecycle_hooks_from_generated_engine_traits() {
    let sema = SemaFile::new();
    let trace_log = TraceLog::recording();
    let mut engine = sema.engine_with_trace(trace_log.clone());

    engine.start().expect("start lifecycle hooks run");
    engine.stop().expect("stop lifecycle hooks run");

    assert_activation_names(
        &trace_log.events(),
        &[
            "SemaStarted",
            "NexusStarted",
            "SignalStarted",
            "SignalStopped",
            "NexusStopped",
            "SemaStopped",
        ],
    );
    assert_activation_objects(
        &trace_log.events(),
        &[
            ObjectName::Sema(SemaObjectName::Started),
            ObjectName::Nexus(NexusObjectName::Started),
            ObjectName::Signal(SignalObjectName::Started),
            ObjectName::Signal(SignalObjectName::Stopped),
            ObjectName::Nexus(NexusObjectName::Stopped),
            ObjectName::Sema(SemaObjectName::Stopped),
        ],
    );
}

#[test]
fn testing_trace_builds_record_activations_by_default() {
    let sema = SemaFile::new();
    let mut engine = Engine::new(Store::open(&sema.path).expect("open sema store"));

    let output = engine.handle(Query::Record(record_request(entry(
        "default trace witness",
    ))));
    assert!(matches!(output.root(), Response::RecordAccepted(_)));

    assert_activation_names(
        &engine.trace_events(),
        &[
            "SignalAdmitted",
            "SignalTriaged",
            "NexusEntered",
            "NexusDecided",
            "SemaWriteApplied",
            "NexusEntered",
            "NexusDecided",
            "SignalReplied",
        ],
    );
}

#[test]
fn testing_trace_records_signal_rejection_without_nexus_or_sema_activations() {
    let sema = SemaFile::new();
    let trace_log = TraceLog::recording();
    let mut engine = sema.engine_with_trace(trace_log.clone());

    let mut invalid_entry = entry("invalid trace witness");
    invalid_entry.domains = vec![];

    let output = engine.handle(Query::Record(record_request(invalid_entry)));

    assert_eq!(engine.record_count(), 0);
    assert_eq!(
        output.root(),
        &Response::Rejected(SignalRejection {
            validation_error: ValidationError::EmptyDomain
        })
    );
    assert_activation_names(&trace_log.events(), &["SignalRejected", "SignalReplied"]);
}

fn assert_activation_names(events: &[TraceEvent], expected: &[&str]) {
    let actual = events.iter().map(TraceEvent::name).collect::<Vec<_>>();
    assert_eq!(actual, expected, "trace events: {events:#?}");
}

fn assert_activation_objects(events: &[TraceEvent], expected: &[ObjectName]) {
    let actual = events
        .iter()
        .map(TraceEvent::object_name)
        .collect::<Vec<ObjectName>>();
    let expected = expected.to_vec();
    assert_eq!(actual, expected, "trace events: {events:#?}");
}
