#![cfg(feature = "agent-guardian")]

mod support;

use std::{
    io::{Read, Write},
    os::unix::net::{UnixListener, UnixStream},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use signal_spirit_judge::{
    AdmissionJudgeOperation, AdmissionJudgeResponse, AdmissionJudgeVerdict, ByteViewable,
    JudgeDiagnostic, Query as SpiritJudgeQuery, Response as SpiritJudgeResponse, Restorable,
    Signal, Signalizable,
};
use spirit::{
    AgentGuardian, AgentGuardianConfiguration, Engine, Store,
    schema::signal::{
        ClarificationRequest, ClarificationResolution, Entry, GuardianRejectionReason,
        Justification, Kind, Magnitude, Proposal, Query, RecordChange, RecordIdentifier,
        RecordRequest, Response, Retirement, Supersession, TargetClarification, VerbatimQuote,
    },
};
use tempfile::TempDir;

use support::domain_fixtures;

struct FakeSpiritJudge {
    directory: TempDir,
    captured_requests: Arc<Mutex<Vec<SpiritJudgeQuery>>>,
    thread: thread::JoinHandle<()>,
}

impl FakeSpiritJudge {
    fn accept_once() -> Self {
        Self::accepting(1)
    }

    fn accepting(request_count: usize) -> Self {
        let directory = TempDir::new().expect("tempdir");
        let socket_path = directory.path().join("spirit-judge.sock");
        let listener = UnixListener::bind(&socket_path).expect("bind fake spirit judge");
        let captured_requests = Arc::new(Mutex::new(Vec::new()));
        let thread_requests = Arc::clone(&captured_requests);
        let thread = thread::spawn(move || {
            for _ in 0..request_count {
                let (mut stream, _) = listener.accept().expect("accept judge request");
                let request_payload = SpiritJudgeSignalFrame::new(&mut stream).read_query();
                thread_requests
                    .lock()
                    .expect("capture request")
                    .push(request_payload.clone());
                let reply = match request_payload {
                    SpiritJudgeQuery::JudgeAdmission(_) => {
                        SpiritJudgeResponse::AdmissionJudged(AdmissionJudgeResponse {
                            admission_judge_verdict: AdmissionJudgeVerdict::Accept,
                            judge_diagnostic: JudgeDiagnostic {
                                redacted_text: "accepted".into(),
                                content_hashes: vec![],
                            },
                        })
                    }
                };
                SpiritJudgeSignalFrame::new(&mut stream).write_response(&reply);
            }
        });
        Self {
            directory,
            captured_requests,
            thread,
        }
    }

    fn guardian(&self) -> AgentGuardian {
        AgentGuardian::new(AgentGuardianConfiguration::new(
            self.directory.path().join("spirit-judge.sock"),
            None,
            None,
            Duration::from_secs(5),
            None,
        ))
    }

    fn join(self) -> Vec<SpiritJudgeQuery> {
        self.thread.join().expect("fake judge joins");
        self.captured_requests
            .lock()
            .expect("read requests")
            .clone()
    }
}

struct SpiritJudgeSignalFrame<'stream> {
    stream: &'stream mut UnixStream,
}

impl<'stream> SpiritJudgeSignalFrame<'stream> {
    fn new(stream: &'stream mut UnixStream) -> Self {
        Self { stream }
    }
    fn read_query(&mut self) -> SpiritJudgeQuery {
        let mut prefix = [0_u8; 4];
        self.stream.read_exact(&mut prefix).expect("read prefix");
        let mut bytes = vec![0; u32::from_be_bytes(prefix) as usize];
        self.stream.read_exact(&mut bytes).expect("read body");
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
                    .expect("u32 frame length")
                    .to_be_bytes(),
            )
            .expect("write prefix");
        self.stream.write_all(bytes).expect("write body");
        self.stream.flush().expect("flush response");
    }
}

#[test]
fn daemon_admission_sends_one_typed_spirit_judge_request() {
    let judge = FakeSpiritJudge::accept_once();
    let database = TempDir::new().expect("database tempdir");
    let mut engine = Engine::new(Store::open(database.path().join("intent.sema")).expect("store"));
    engine.set_guardian(judge.guardian());

    let output = engine.handle(Query::Record(record_request(entry(
        "typed judge request crosses the daemon boundary",
    ))));

    assert!(
        matches!(output.root(), Response::RecordAccepted(_)),
        "expected record accepted, got {:?}",
        output.root()
    );
    let requests = judge.join();
    let Some(SpiritJudgeQuery::JudgeAdmission(packet)) = requests.first() else {
        panic!("expected one admission judge request: {requests:?}");
    };
    assert!(matches!(
        packet.admission_judge_operation,
        signal_spirit_judge::AdmissionJudgeOperation::Record(_)
    ));
    assert!(packet.record_set.is_empty());
}

#[test]
fn required_guardian_rejects_proposal_when_judge_is_unconfigured() {
    let database = TempDir::new().expect("database tempdir");
    let mut engine = Engine::new(Store::open(database.path().join("intent.sema")).expect("store"));
    let setup_identifier = accept_record(&mut engine, entry("existing setup record"));
    engine.require_guardian();

    let output = engine.handle(input_propose(entry(
        "unguarded proposal should fail closed",
    )));

    match output.root() {
        Response::GuardianRejected(rejection) => {
            assert_eq!(
                rejection.guardian_rejection_reason,
                GuardianRejectionReason::HarnessUnavailable
            );
            assert_eq!(
                rejection.explanation,
                "guardian is required but no guardian agent is configured"
            );
        }
        other => panic!("expected GuardianRejected for missing proposal judge, got {other:?}"),
    }
    assert_eq!(engine.record_count(), 1);
    assert!(!setup_identifier.is_empty());
    assert_eq!(engine.guardian_decision_count(), 1);
}

#[test]
fn daemon_admission_includes_existing_record_context_for_lifecycle_operations() {
    let judge = FakeSpiritJudge::accepting(5);
    let database = TempDir::new().expect("database tempdir");
    let mut engine = Engine::new(Store::open(database.path().join("intent.sema")).expect("store"));

    let clarify_identifier = accept_record(&mut engine, entry("clarify context"));
    let change_identifier = accept_record(&mut engine, entry("change context"));
    let supersede_identifier = accept_record(&mut engine, entry("supersede context"));
    let retire_identifier = accept_record(&mut engine, entry("retire context"));
    let resolution_identifier = accept_record(&mut engine, entry("resolution context"));
    let resolution_target_identifier = accept_record(&mut engine, entry("resolution target"));
    engine.set_guardian(judge.guardian());

    let clarified = engine.handle(input_clarify(
        clarify_identifier,
        "redacted clarify replacement",
    ));
    assert!(matches!(clarified.root(), Response::Clarified(_)));
    let changed = engine.handle(input_change_record(
        change_identifier,
        entry("change replacement"),
    ));
    assert!(matches!(changed.root(), Response::RecordChanged(_)));
    let superseded = engine.handle(input_supersede(
        supersede_identifier,
        entry("supersede replacement"),
    ));
    assert!(matches!(superseded.root(), Response::Superseded(_)));
    let retired = engine.handle(input_retire(retire_identifier));
    assert!(matches!(retired.root(), Response::Retired(_)));
    let resolved = engine.handle(input_resolve_clarification(
        resolution_identifier,
        resolution_target_identifier,
        "redacted resolution replacement",
    ));
    assert!(matches!(
        resolved.root(),
        Response::ClarificationResolved(_)
    ));

    let requests = judge.join();
    assert_eq!(
        requests.len(),
        5,
        "expected one admission request per operation"
    );
    assert!(matches!(
        admission_operation(&requests[0]),
        AdmissionJudgeOperation::Clarify(_)
    ));
    assert!(matches!(
        admission_operation(&requests[1]),
        AdmissionJudgeOperation::ChangeRecord(_)
    ));
    assert!(matches!(
        admission_operation(&requests[2]),
        AdmissionJudgeOperation::Supersede(_)
    ));
    assert!(matches!(
        admission_operation(&requests[3]),
        AdmissionJudgeOperation::Retire(_)
    ));
    assert!(matches!(
        admission_operation(&requests[4]),
        AdmissionJudgeOperation::ResolveClarification(_)
    ));
}

fn admission_operation(request: &SpiritJudgeQuery) -> &AdmissionJudgeOperation {
    let SpiritJudgeQuery::JudgeAdmission(packet) = request;
    &packet.admission_judge_operation
}

fn accept_record(engine: &mut Engine, entry: Entry) -> RecordIdentifier {
    match engine.handle(input_record(entry)).into_root() {
        Response::RecordAccepted(identifier) => identifier,
        other => panic!("expected setup record accepted, got {other:?}"),
    }
}

fn entry(description: &str) -> Entry {
    Entry {
        domains: domain_fixtures::domains(&["judge-socket"]),
        kind: Kind::Decision,
        description: description.into(),
        importance: Magnitude::Minimum,
    }
}

fn input_record(entry: Entry) -> Query {
    Query::Record(record_request(entry))
}

fn input_propose(entry: Entry) -> Query {
    Query::Propose(Proposal {
        entry,
        justification: justification("proposed forward arrow"),
    })
}

fn input_clarify(record_identifier: RecordIdentifier, description: &str) -> Query {
    Query::Clarify(ClarificationRequest {
        record_identifier,
        description: description.into(),
        justification: justification(description),
    })
}

fn input_change_record(record_identifier: RecordIdentifier, entry: Entry) -> Query {
    Query::ChangeRecord(RecordChange {
        record_identifier,
        entry,
        justification: justification("change record"),
    })
}

fn input_supersede(record_identifier: RecordIdentifier, replacement: Entry) -> Query {
    Query::Supersede(Supersession {
        retired_identifiers: vec![record_identifier],
        replacements: vec![replacement],
        justification: justification("replacement forward arrow"),
    })
}

fn input_retire(record_identifier: RecordIdentifier) -> Query {
    Query::Retire(Retirement {
        record_identifier,
        justification: justification("retire this record"),
    })
}

fn input_resolve_clarification(
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
        justification: justification("resolve clarification"),
    })
}

fn record_request(entry: Entry) -> RecordRequest {
    let statement = entry.description.clone();
    RecordRequest {
        entry,
        justification: justification(&statement),
    }
}

fn justification(statement: &str) -> Justification {
    Justification {
        testimony: vec![VerbatimQuote {
            quote_text: statement.to_owned(),
            optional_antecedent: None,
        }],
        reasoning: statement.to_owned(),
    }
}
