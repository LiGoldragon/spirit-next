//! Spirit's actor-boundary trace sink.
//!
//! Every execution center (Signal admission, Nexus, the SEMA store) records its
//! boundary crossings into a shared [`TraceLog`]. The log keeps two
//! destinations at once (report 716):
//!
//! - an in-memory recording of spirit's own [`TraceEvent`]s, which spirit's
//!   tests read back verbatim via [`TraceLog::events`]; and
//! - an optional push socket that carries the SHARED wire contract
//!   [`signal_introspect::ComponentTraceEvent`], so the introspect daemon
//!   decodes the exact archived noun without ever depending on `spirit`.
//!
//! Call sites everywhere still hand the sink a `TraceEvent`; the projection onto
//! the contract type and the per-engine identity/sequence stamping happen here,
//! at the push boundary, so no actor-boundary emission point changes.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use signal_introspect::{ByteViewable, ComponentTraceEvent, Signalizable};
use signal_persona::EngineIdentifier;

use crate::TraceEvent;
pub use triad_runtime::trace::{TraceError, TraceEventFrame};

/// The push target for the contract trace events, carried only when the daemon
/// is configured with a trace socket path.
#[derive(Clone, Debug)]
struct ComponentTraceSink {
    engine: EngineIdentifier,
    sequence: Arc<AtomicU64>,
    socket_path: std::path::PathBuf,
}

impl ComponentTraceSink {
    fn new(engine: EngineIdentifier, path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            engine,
            sequence: Arc::new(AtomicU64::new(0)),
            socket_path: path.into(),
        }
    }

    /// Project the spirit event onto the shared contract, stamping the emitting
    /// engine identity and the next monotonic per-emitter sequence, then push.
    fn push(&self, event: TraceEvent) {
        let mut projected = ComponentTraceEvent::from(event);
        projected.engine_identifier = self.engine.clone();
        projected.trace_sequence = self.sequence.fetch_add(1, Ordering::Relaxed) as i64;
        let Ok(signal) = projected.signalize() else {
            return;
        };
        let Ok(mut stream) = std::os::unix::net::UnixStream::connect(&self.socket_path) else {
            return;
        };
        let bytes = signal.bytes();
        let Ok(length) = u32::try_from(bytes.len()) else {
            return;
        };
        let _ = std::io::Write::write_all(&mut stream, &length.to_le_bytes());
        let _ = std::io::Write::write_all(&mut stream, bytes);
    }
}

/// Spirit's trace sink: an in-memory recording of [`TraceEvent`] for spirit's
/// own tests, plus an optional contract-typed push socket for introspect.
#[derive(Clone, Debug)]
pub struct TraceLog {
    recording: triad_runtime::trace::TraceLog<TraceEvent>,
    component_sink: Option<ComponentTraceSink>,
}

impl Default for TraceLog {
    fn default() -> Self {
        Self::recording()
    }
}

impl TraceLog {
    /// In-memory only: record spirit's own events, push nothing.
    pub fn recording() -> Self {
        Self {
            recording: triad_runtime::trace::TraceLog::recording(),
            component_sink: None,
        }
    }

    /// In-memory recording PLUS a contract-typed push socket. The `engine`
    /// identity is stamped onto every pushed [`ComponentTraceEvent`].
    pub fn socket(engine: EngineIdentifier, path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            recording: triad_runtime::trace::TraceLog::recording(),
            component_sink: Some(ComponentTraceSink::new(engine, path)),
        }
    }

    /// Record one actor-boundary event: always into the in-memory recording,
    /// and, when a push socket is configured, projected onto the shared
    /// contract and pushed to introspect.
    pub fn record(&self, event: TraceEvent) {
        self.recording.record(event);
        if let Some(sink) = &self.component_sink {
            sink.push(event);
        }
    }

    /// The in-memory recording of spirit's own events, for spirit's tests.
    pub fn events(&self) -> Vec<TraceEvent> {
        self.recording.events()
    }
}

impl TraceEventFrame for TraceEvent {
    fn to_trace_archive(&self) -> Result<Vec<u8>, TraceError> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self)
            .map(|archive| archive.to_vec())
            .map_err(|_| TraceError::ArchiveEncode)
    }

    fn from_trace_archive(archive: &[u8]) -> Result<Self, TraceError> {
        rkyv::from_bytes::<Self, rkyv::rancor::Error>(archive)
            .map_err(|_| TraceError::ArchiveDecode)
    }
}

#[cfg(feature = "datom-cli")]
impl std::fmt::Display for TraceEvent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use datom_codec::Datomizable;
        use protos::{Protosizable, Textualizable};
        formatter.write_str(&self.datomize(vec![]).protosize().textualize())
    }
}

#[cfg(feature = "datom-cli")]
impl std::str::FromStr for TraceEvent {
    type Err = datom_codec::Error;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        use datom_codec::{Actualizing, Budget, Potential};
        use protos::ReaderBudget;
        let mut pending = Potential::<Self>::from(source.to_owned());
        pending.actualize(&mut Budget {
            remaining: 1024,
            reader: ReaderBudget { remaining: 1024 },
            depth: 0,
            maximum_depth: 1024,
        })
    }
}

#[cfg(test)]
mod tests {
    use signal_introspect::{IntrospectionTarget, TraceLayer};

    use super::*;
    use crate::{
        ObjectName, SignalObjectName,
        schema::nexus::{NexusObjectName, NexusWorkRoute},
    };

    #[test]
    fn projects_signal_admitted_onto_contract_event() {
        let event = TraceEvent::new(ObjectName::Signal(SignalObjectName::Admitted));
        let projected = ComponentTraceEvent::from(event);

        assert_eq!(projected.introspection_target, IntrospectionTarget::Signal);
        assert_eq!(projected.trace_layer, TraceLayer::Signal);
        assert_eq!(projected.trace_event_name, "SignalAdmitted");
    }

    #[test]
    fn socket_trace_events_preserve_engine_identity_and_monotonic_sequence() {
        use signal_introspect::{Restorable, Signal};
        use std::{io::Read, os::unix::net::UnixListener, thread};

        let directory = tempfile::tempdir().expect("temporary trace directory");
        let socket_path = directory.path().join("trace.sock");
        let listener = UnixListener::bind(&socket_path).expect("bind trace listener");
        let reader = thread::spawn(move || {
            let mut received = Vec::new();
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().expect("accept trace event");
                let mut header = [0_u8; 4];
                stream.read_exact(&mut header).expect("read frame length");
                let length = u32::from_le_bytes(header) as usize;
                let mut bytes = vec![0_u8; length];
                stream.read_exact(&mut bytes).expect("read trace archive");
                received.push(
                    Signal::<ComponentTraceEvent>::from(bytes)
                        .restore()
                        .expect("restore fresh trace signal"),
                );
            }
            received
        });

        let trace = TraceLog::socket("engine-under-test".to_owned(), &socket_path);
        trace.record(TraceEvent::new(ObjectName::Signal(
            SignalObjectName::Admitted,
        )));
        trace.record(TraceEvent::new(ObjectName::Signal(
            SignalObjectName::Replied,
        )));
        let events = reader.join().expect("trace reader joins");

        assert!(
            events
                .iter()
                .all(|event| !event.engine_identifier.is_empty())
        );
        assert_eq!(events[0].engine_identifier, "engine-under-test");
        assert_eq!(events[0].trace_sequence, 0);
        assert_eq!(events[1].trace_sequence, 1);
        assert!(events[0].trace_sequence < events[1].trace_sequence);
    }

    #[test]
    fn nested_trace_route_round_trips_through_datom() {
        let event = TraceEvent::new(ObjectName::Nexus(NexusObjectName::Work(
            NexusWorkRoute::SignalArrived,
        )));
        let text = event.to_string();
        assert_eq!(text.parse::<TraceEvent>().expect("restore trace"), event);
    }

    #[test]
    fn malformed_trace_text_is_rejected() {
        assert!("Nexus.Work.Unknown".parse::<TraceEvent>().is_err());
    }
}
