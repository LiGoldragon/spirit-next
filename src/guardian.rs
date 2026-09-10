use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    time::Duration,
};

use signal_spirit_judge::{
    AdmissionJudgeOperation, AdmissionJudgePacket, AdmissionJudgeResponse, AdmissionJudgeVerdict,
    AdmissionRejectionReason, ByteViewable, JudgeDiagnostic, Query as JudgeQuery,
    Response as JudgeResponse, Restorable, Signal, Signalizable, SpiritJudgeRequestRejection,
    SpiritJudgeRequestRejectionReason,
};
use thiserror::Error;

use crate::{
    guardian_journal::{GuardianDecision, GuardianOperation},
    schema::{
        nexus::{GuardianVerdict, Reject},
        signal::{
            DatabaseMarker, Explanation, GuardianRejection, GuardianRejectionReason, RecordSet,
        },
    },
};

#[derive(Clone, Debug, PartialEq)]
pub struct AgentGuardianConfiguration {
    socket_path: PathBuf,
    timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct AgentGuardian {
    configuration: AgentGuardianConfiguration,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgentGuardianRejection {
    reason: GuardianRejectionReason,
    records: RecordSet,
    explanation: Explanation,
    database_marker: DatabaseMarker,
}

#[derive(Clone, Debug, PartialEq)]
pub struct AgentGuardianDecision {
    verdict: GuardianVerdict,
    records: RecordSet,
    database_marker: DatabaseMarker,
}

#[derive(Debug, Error)]
pub enum AgentGuardianError {
    #[error("spirit judge socket unavailable: {0}")]
    Socket(std::io::Error),

    #[error("spirit judge frame failed: {0}")]
    Frame(String),

    #[error("spirit judge replied with the wrong operation: {0}")]
    WrongReply(&'static str),
}

impl AgentGuardianConfiguration {
    pub const LOCAL_OPENAI_COMPATIBLE_PROVIDER: &'static str = "local-openai";
    pub const LOCAL_OPENAI_COMPATIBLE_MODEL: &'static str = "gpt-5.4-mini";
    pub const LOCAL_OPENAI_COMPATIBLE_ENDPOINT: &'static str = "http://127.0.0.1:18080/v1";
    pub const DEFAULT_TIMEOUT_MILLISECONDS: u64 = 180_000;

    pub fn new(
        socket_path: impl Into<PathBuf>,
        _provider_name: Option<String>,
        _model_name: Option<String>,
        timeout: Duration,
        _maximum_output_tokens: Option<u64>,
    ) -> Self {
        Self {
            socket_path: socket_path.into(),
            timeout,
        }
    }

    pub fn local_openai_compatible(socket_path: impl Into<PathBuf>) -> Self {
        Self::new(
            socket_path,
            None,
            None,
            Duration::from_millis(Self::DEFAULT_TIMEOUT_MILLISECONDS),
            None,
        )
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn provider_name(&self) -> Option<&str> {
        None
    }

    pub fn model_name(&self) -> Option<&str> {
        None
    }
}

impl AgentGuardian {
    pub fn new(configuration: AgentGuardianConfiguration) -> Self {
        Self { configuration }
    }

    pub(crate) fn guard(
        &self,
        operation: &GuardianOperation,
        records: RecordSet,
        database_marker: DatabaseMarker,
    ) -> AgentGuardianDecision {
        let packet = AdmissionJudgePacket {
            admission_judge_operation: AdmissionJudgeOperationProjection::new(operation)
                .into_contract(),
            record_set: records.clone(),
            database_marker: database_marker.clone(),
        };
        let verdict = match self.call_judge(JudgeQuery::JudgeAdmission(packet)) {
            Ok(JudgeResponse::AdmissionJudged(response)) => {
                GuardianVerdict::from_admission_response(response)
            }
            Ok(JudgeResponse::RequestRejected(rejection)) => {
                GuardianVerdict::from_request_rejection(rejection)
            }
            Err(error) => GuardianVerdict::from_judge_error(error),
        };
        AgentGuardianDecision::new(verdict, records, database_marker)
    }

    fn call_judge(&self, request: JudgeQuery) -> Result<JudgeResponse, AgentGuardianError> {
        let mut stream = UnixStream::connect(self.configuration.socket_path())
            .map_err(AgentGuardianError::Socket)?;
        stream
            .set_read_timeout(Some(self.configuration.timeout()))
            .map_err(AgentGuardianError::Socket)?;
        stream
            .set_write_timeout(Some(self.configuration.timeout()))
            .map_err(AgentGuardianError::Socket)?;
        let signal = request
            .signalize()
            .map_err(|error| AgentGuardianError::Frame(error.to_string()))?;
        let bytes = signal.bytes();
        let length = u32::try_from(bytes.len())
            .map_err(|_| AgentGuardianError::Frame("judge signal exceeds u32 length".into()))?;
        stream
            .write_all(&length.to_be_bytes())
            .map_err(AgentGuardianError::Socket)?;
        stream
            .write_all(bytes)
            .map_err(AgentGuardianError::Socket)?;
        stream.flush().map_err(AgentGuardianError::Socket)?;
        FrameReader::new(&mut stream).read_reply_signal()
    }
}

struct FrameReader<'stream> {
    stream: &'stream mut UnixStream,
}

impl<'stream> FrameReader<'stream> {
    fn new(stream: &'stream mut UnixStream) -> Self {
        Self { stream }
    }

    fn read_reply_signal(&mut self) -> Result<JudgeResponse, AgentGuardianError> {
        let mut prefix = [0_u8; 4];
        self.stream
            .read_exact(&mut prefix)
            .map_err(AgentGuardianError::Socket)?;
        let length = u32::from_be_bytes(prefix) as usize;
        let mut bytes = vec![0; length];
        self.stream
            .read_exact(&mut bytes)
            .map_err(AgentGuardianError::Socket)?;
        Signal::<JudgeResponse>::from(bytes)
            .restore()
            .map_err(|error| AgentGuardianError::Frame(error.to_string()))
    }
}

struct AdmissionJudgeOperationProjection<'operation> {
    operation: &'operation GuardianOperation,
}

impl<'operation> AdmissionJudgeOperationProjection<'operation> {
    fn new(operation: &'operation GuardianOperation) -> Self {
        Self { operation }
    }

    fn into_contract(self) -> AdmissionJudgeOperation {
        match self.operation {
            GuardianOperation::Record(request) => AdmissionJudgeOperation::Record(request.clone()),
            GuardianOperation::Propose(proposal) => {
                AdmissionJudgeOperation::Propose(signal_spirit::RecordRequest {
                    entry: proposal.entry.clone(),
                    justification: proposal.justification.clone(),
                })
            }
            GuardianOperation::Clarify(clarification) => {
                AdmissionJudgeOperation::Clarify(signal_spirit::ClarificationRequest {
                    record_identifier: clarification.record_identifier.clone(),
                    description: clarification.description.clone(),
                    justification: clarification.justification.clone(),
                })
            }
            GuardianOperation::ResolveClarification(resolution) => {
                AdmissionJudgeOperation::ResolveClarification(resolution.clone())
            }
            GuardianOperation::Supersede(supersession) => {
                AdmissionJudgeOperation::Supersede(supersession.clone())
            }
            GuardianOperation::Retire(retirement) => {
                AdmissionJudgeOperation::Retire(retirement.clone())
            }
            GuardianOperation::ChangeRecord(change) => {
                AdmissionJudgeOperation::ChangeRecord(change.clone())
            }
        }
    }
}

impl GuardianVerdict {
    fn from_admission_response(response: AdmissionJudgeResponse) -> Self {
        match response.admission_judge_verdict {
            AdmissionJudgeVerdict::Accept => Self::Accept,
            AdmissionJudgeVerdict::Reject(reason) => Self::reject(Reject {
                guardian_rejection_reason: AdmissionRejectionProjection::new(reason).into_signal(),
                explanation: JudgeDiagnosticProjection::new(response.judge_diagnostic)
                    .into_explanation(),
            }),
        }
    }

    fn from_request_rejection(rejection: SpiritJudgeRequestRejection) -> Self {
        Self::reject(Reject {
            guardian_rejection_reason: RequestRejectionProjection::new(
                rejection.spirit_judge_request_rejection_reason,
            )
            .to_guardian_reason(),
            explanation: JudgeDiagnosticProjection::new(rejection.judge_diagnostic)
                .into_explanation(),
        })
    }

    fn from_judge_error(error: AgentGuardianError) -> Self {
        Self::reject(Reject {
            guardian_rejection_reason: error.guardian_rejection_reason(),
            explanation: error.to_string(),
        })
    }
}

struct JudgeDiagnosticProjection {
    diagnostic: JudgeDiagnostic,
}

impl JudgeDiagnosticProjection {
    fn new(diagnostic: JudgeDiagnostic) -> Self {
        Self { diagnostic }
    }

    fn into_explanation(self) -> Explanation {
        if self.diagnostic.content_hashes.is_empty() {
            self.diagnostic.redacted_text.clone()
        } else {
            let hashes = self
                .diagnostic
                .content_hashes
                .iter()
                .map(|hash| hash.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            format!("{} [{}]", &self.diagnostic.redacted_text, hashes)
        }
    }
}

struct AdmissionRejectionProjection {
    reason: AdmissionRejectionReason,
}

impl AdmissionRejectionProjection {
    fn new(reason: AdmissionRejectionReason) -> Self {
        Self { reason }
    }

    fn into_signal(self) -> GuardianRejectionReason {
        match self.reason {
            AdmissionRejectionReason::Duplicate => GuardianRejectionReason::Duplicate,
            AdmissionRejectionReason::Contradiction => GuardianRejectionReason::Contradiction,
            AdmissionRejectionReason::Compound => GuardianRejectionReason::Compound,
            AdmissionRejectionReason::NonIntent => GuardianRejectionReason::NonIntent,
            AdmissionRejectionReason::NegativeGuideline => {
                GuardianRejectionReason::NegativeGuideline
            }
            AdmissionRejectionReason::Matter => GuardianRejectionReason::Matter,
            AdmissionRejectionReason::UnclearDomain => GuardianRejectionReason::UnclearDomain,
            AdmissionRejectionReason::ClarifyTramples => GuardianRejectionReason::ClarifyTramples,
            AdmissionRejectionReason::ClarifyLosesMeaning => {
                GuardianRejectionReason::ClarifyLosesMeaning
            }
            AdmissionRejectionReason::SupersedeTargetMissing => {
                GuardianRejectionReason::SupersedeTargetMissing
            }
            AdmissionRejectionReason::RetrievalInsufficient => {
                GuardianRejectionReason::RetrievalInsufficient
            }
            AdmissionRejectionReason::MissingTestimony => GuardianRejectionReason::MissingTestimony,
            AdmissionRejectionReason::TestimonyFabricated => {
                GuardianRejectionReason::TestimonyFabricated
            }
            AdmissionRejectionReason::InsufficientWarrant => {
                GuardianRejectionReason::InsufficientWarrant
            }
            AdmissionRejectionReason::ImportanceUnsupported => {
                GuardianRejectionReason::ImportanceUnsupported
            }
            AdmissionRejectionReason::JudgeUnavailable => {
                GuardianRejectionReason::HarnessUnavailable
            }
            AdmissionRejectionReason::JudgeMalformed => GuardianRejectionReason::HarnessMalformed,
            AdmissionRejectionReason::JudgeTimedOut => GuardianRejectionReason::HarnessTimedOut,
        }
    }
}

struct RequestRejectionProjection {
    reason: SpiritJudgeRequestRejectionReason,
}

impl RequestRejectionProjection {
    fn new(reason: SpiritJudgeRequestRejectionReason) -> Self {
        Self { reason }
    }

    fn to_guardian_reason(&self) -> GuardianRejectionReason {
        match self.reason {
            SpiritJudgeRequestRejectionReason::InvalidRequest
            | SpiritJudgeRequestRejectionReason::ConfigurationUnavailable
            | SpiritJudgeRequestRejectionReason::ResponseFormatFailure => {
                GuardianRejectionReason::HarnessMalformed
            }
            SpiritJudgeRequestRejectionReason::ProviderUnavailable
            | SpiritJudgeRequestRejectionReason::ProviderRejected => {
                GuardianRejectionReason::HarnessUnavailable
            }
        }
    }
}

impl AgentGuardianError {
    fn guardian_rejection_reason(&self) -> GuardianRejectionReason {
        match self {
            Self::Socket(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                ) =>
            {
                GuardianRejectionReason::HarnessTimedOut
            }
            Self::Socket(_) | Self::Frame(_) | Self::WrongReply(_) => {
                GuardianRejectionReason::HarnessUnavailable
            }
        }
    }
}

impl AgentGuardianDecision {
    fn new(verdict: GuardianVerdict, records: RecordSet, database_marker: DatabaseMarker) -> Self {
        Self {
            verdict,
            records,
            database_marker,
        }
    }

    pub(crate) fn journal_decision(&self, operation: GuardianOperation) -> GuardianDecision {
        GuardianDecision::record(
            operation,
            self.records.clone(),
            self.verdict.clone(),
            self.database_marker.clone(),
        )
    }

    pub(crate) fn into_guardian_rejection(self) -> Option<GuardianRejection> {
        match self.verdict {
            GuardianVerdict::Accept => None,
            GuardianVerdict::Reject(rejection) => Some(
                AgentGuardianRejection::from_reject(rejection, self.records, self.database_marker)
                    .into_guardian_rejection(),
            ),
        }
    }
}

impl AgentGuardianRejection {
    fn from_reject(rejection: Reject, records: RecordSet, database_marker: DatabaseMarker) -> Self {
        Self {
            reason: rejection.guardian_rejection_reason,
            records,
            explanation: rejection.explanation,
            database_marker,
        }
    }

    pub fn into_guardian_rejection(self) -> GuardianRejection {
        GuardianRejection {
            guardian_rejection_reason: self.reason,
            record_set: self.records,
            explanation: self.explanation,
        }
    }
}

pub type AgentJudge = AgentGuardian;
pub type AgentJudgeConfiguration = AgentGuardianConfiguration;
pub type AgentJudgeDecision = AgentGuardianDecision;
pub type AgentJudgeError = AgentGuardianError;
pub type AgentJudgeRejection = AgentGuardianRejection;
