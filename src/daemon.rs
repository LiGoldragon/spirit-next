//! Spirit's daemon hooks — the only daemon code spirit hand-writes.
//!
//! The uniform daemon skeleton (the `DaemonCommand` argv parsing, async task-backed
//! multi-listener binding, accepted-connection context, decode -> execute ->
//! encode spine, emitted subscription registry + retained-writer publish
//! wiring, and `ExitReport`-based entry) is emitted into
//! authored daemon runtime. Spirit fills
//! only the record-1488 escape hatches through `impl ComponentDaemon for
//! SpiritDaemon`: how to load its binary `Configuration`, how to open its
//! Store/Engine (`build_runtime`), how one working `Query` becomes one
//! `Response`, the owner-only meta request hook, and the stream filter + event
//! policy.

use thiserror::Error;
use tokio::io::AsyncWriteExt;
use triad_runtime::{
    AcceptedConnection, EngineRequestError, FrameBody as LengthPrefixedFrameBody, FrameError,
    LengthPrefixedCodec, ListenerError,
};

use meta_signal_spirit::{ByteViewable as _, Restorable as _, Signalizable as _};

use crate::{
    Configuration, ConfigurationError, Engine, StoreError,
    component_daemon::{ComponentDaemon, DaemonBinder, DaemonError},
    meta_transport::MetaTransportError,
    schema::nexus::{EngineStartFailure, EngineStopFailure},
    schema::signal::{IntentEvent, Query, Response},
    store::{EntryStoreExt, Store},
    subscription::IntentSubscriptionToken,
    transport::TransportError,
};

#[cfg(feature = "testing-trace")]
use crate::TraceLog;

/// The type-level selector for spirit's emitted daemon. It carries no runtime
/// data — it is the marker the emitted `DaemonCommand<SpiritDaemon>` and the
/// generated runtime dispatch on, selecting spirit's `Configuration` / `Engine`
/// / `Error` and the stream token/filter/event types through the
/// `ComponentDaemon` associated types.
#[derive(Debug)]
pub struct SpiritDaemon;

/// Spirit's daemon error: the engine-facing variants the emitted spine needs
/// (`From<FrameError>` / `From<SignalFrameError>` / `From<ListenerError>`) plus
/// spirit's domain errors. The emitted `DaemonError<SpiritDaemon>` wraps this
/// under its `Component` arm.
#[derive(Debug, Error)]
pub enum SpiritDaemonError {
    #[error("daemon IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("daemon frame error: {0}")]
    Frame(#[from] FrameError),

    #[error("daemon listener error: {0}")]
    Listener(#[from] ListenerError),

    #[error("daemon signal frame error: {0}")]
    Signal(String),

    #[error("daemon transport error: {0}")]
    Transport(#[from] TransportError),

    #[error("daemon meta transport error: {0}")]
    MetaTransport(#[from] MetaTransportError),

    #[error("daemon sema store error: {0}")]
    Store(#[from] StoreError),

    #[error("daemon engine start error: {0}")]
    EngineStart(#[from] EngineStartFailure),

    #[error("daemon engine stop error: {0}")]
    EngineStop(#[from] EngineStopFailure),

    #[error("daemon engine request error: {0}")]
    EngineRequest(#[from] EngineRequestError),
}

impl From<String> for SpiritDaemonError {
    fn from(error: String) -> Self {
        Self::Signal(error)
    }
}

impl ComponentDaemon for SpiritDaemon {
    type Configuration = Configuration;
    type ConfigurationError = ConfigurationError;
    type Engine = Engine;
    type Error = SpiritDaemonError;
    type SubscriptionToken = IntentSubscriptionToken;
    type SubscriptionFilter = signal_spirit::Selection;
    type StreamEvent = IntentEvent;

    const PROCESS_NAME: &'static str = "spirit-nexus";

    fn default_configuration() -> Result<Self::Configuration, Self::ConfigurationError> {
        let database_path = Configuration::stable_database_path();
        // Opening the stable store before listener binding both seeds a fresh
        // Sema and recovers persisted desired configuration on restart. The
        // running daemon retains this immutable active snapshot; Configure
        // writes desired state for the next zero-argument start.
        let state = Store::open_with_configuration(
            &database_path,
            Configuration::default_nexus_configuration(),
        )
        .map_err(|_| ConfigurationError::ArchiveDecode)?
        .nexus_configuration_state()
        .map_err(|_| ConfigurationError::ArchiveDecode)?;
        Configuration::checked_from_raw_at_database(state.desired_configuration, database_path)
    }

    /// Open the engine and run its lifecycle start hooks. Engine startup needs
    /// exclusive `&mut` access (the SEMA → Nexus → Signal `on_start` chain), so
    /// it runs here at owned construction — before the engine is handed to the
    /// schema-emitted `EngineActor`, whose mailbox serialises every later
    /// request behind a shared `ActorRef`. The emitted `ComponentDaemon::start`
    /// / `stop` hooks take a shared `&Self::Engine` and stay the trait no-op
    /// default; the durable SEMA store releases on engine drop at shutdown.
    fn build_runtime(configuration: &Self::Configuration) -> Result<Self::Engine, Self::Error> {
        #[cfg(feature = "testing-trace")]
        let mut engine = {
            // The pushed `ComponentTraceEvent`s are stamped with this engine's
            // identity so introspect can key its store per emitter. The daemon
            // socket path uniquely identifies this running spirit instance.
            let engine_identity = configuration.socket_path().to_string_lossy().into_owned();
            let trace_log = configuration
                .trace_socket_path()
                .map(|path| TraceLog::socket(engine_identity.clone(), path))
                .unwrap_or_default();
            let store = Store::open_with_trace(configuration.database_path(), trace_log.clone())?;
            Engine::new_with_trace(store, trace_log)
        };
        #[cfg(not(feature = "testing-trace"))]
        let mut engine = {
            let store = Store::open(configuration.database_path())?;
            Engine::new(store)
        };
        #[cfg(feature = "agent-guardian")]
        if let Some(guardian) = configuration
            .guardian_agent_configuration()
            .map(crate::guardian::AgentGuardian::new)
        {
            engine.set_guardian(guardian);
        } else {
            engine.require_guardian();
        }
        engine.set_authorization_mode(configuration.authorization_mode());
        engine.start().map_err(Self::Error::from)?;
        // §3.8: an occupied durable staging slot is a crash window awaiting
        // resolution. The gate target is runtime owner policy (Configure),
        // so resolution runs there; until then every head advance is
        // refused, fail-closed, and the daemon says so loudly at start.
        #[cfg(feature = "criome-gate")]
        if let Ok(Some(parked)) = engine.store().engine_handle().staged_group() {
            eprintln!(
                "spirit daemon opened with an OCCUPIED staging slot {}: head advances are \
                 refused until an owner Configure enables the criome gate and recovery \
                 resolves the parked group",
                parked.prospective_head()
            );
        }
        Ok(engine)
    }

    /// The single-turn lane: reads and other non-advancing inputs (and, when
    /// the `criome-gate` feature is out of the build, everything). Under the
    /// everywhere-gate a head-advancing input never runs here with the gate
    /// Enabled — it takes the staged lane (`working_input_lane`), where
    /// acceptance waits on the cluster grant and the ship mail fires only
    /// after materialization.
    async fn handle_working_input(
        engine: &mut Self::Engine,
        input: Query,
        _connection: &triad_runtime::ConnectionContext,
    ) -> Result<Response, Self::Error> {
        Ok(engine.handle_async(input).await.root().clone())
    }

    /// THE CLOSED INTAKE CLASSIFICATION (§3.5.5): effect commands and
    /// sema-writes — everything whose processing appends to the log — are
    /// head-advancing and take the staged lane; queries, observations,
    /// lookups, counts, subscriptions, taps, `Version`, and `Marker` pass
    /// ungated on the immediate lane and never wait on a round.
    /// `ApplyAuthorizedRecord` is NOT intake-gated: it carries an
    /// authorization that already happened (§4) and today answers
    /// fail-closed without a write. The match is exhaustive on purpose: a
    /// new Query variant must choose its lane here before spirit compiles.
    #[cfg(feature = "criome-gate")]
    fn working_input_lane(input: &Query) -> crate::component_daemon::WorkingQueryLane {
        match input {
            Query::Configure(_)
            | Query::State(_)
            | Query::Record(_)
            | Query::Propose(_)
            | Query::Clarify(_)
            | Query::ResolveClarification(_)
            | Query::Supersede(_)
            | Query::Retire(_)
            | Query::BumpImportance(_)
            | Query::ChangeRecord(_) => crate::component_daemon::WorkingQueryLane::Staged,
            Query::Observe(_)
            | Query::Intent(_)
            | Query::TextSearch(_)
            | Query::Lookup(_)
            | Query::Count(_)
            | Query::LookupStash(_)
            | Query::Tap(_)
            | Query::Untap(_)
            | Query::SubscribeIntent(_)
            | Query::Version
            | Query::Marker
            | Query::ApplyAuthorizedRecord(_) => {
                crate::component_daemon::WorkingQueryLane::Immediate
            }
        }
    }

    /// The staged lane's fast first engine turn: stage the operation group
    /// (§3.5.1) and either complete outright or hand back the staged advance
    /// whose `resolve` the connection task awaits outside this mailbox.
    #[cfg(feature = "criome-gate")]
    async fn stage_working_input(
        engine: &mut Self::Engine,
        input: Query,
        _connection: &triad_runtime::ConnectionContext,
    ) -> Result<crate::component_daemon::StagedWorkingTurn<Self>, Self::Error> {
        Ok(match engine.stage_working_input(input).await {
            crate::engine::StagedIntake::Completed(output) => {
                crate::component_daemon::StagedWorkingTurn::Completed(output)
            }
            crate::engine::StagedIntake::Parked(advance) => {
                crate::component_daemon::StagedWorkingTurn::Awaiting(Box::new(advance))
            }
        })
    }

    /// Share the engine's first-in first-out advance gate with the emitted
    /// spine, so staged working turns, the ship drain's passes, and any
    /// residue reconcile round all serialize through ONE queue (§3.5.3).
    #[cfg(feature = "criome-gate")]
    fn shared_advance_gate(
        engine: &Self::Engine,
    ) -> Option<std::sync::Arc<tokio::sync::Mutex<()>>> {
        Some(engine.advance_gate())
    }

    /// Serve one owner-only meta request: decode a `Configure` meta `Query`,
    /// apply it through `Engine::configure` (a configuration effect, not a SEMA
    /// log write), and write the `Configured` / `Rejected` meta `Response` back.
    /// `Configure` is request/reply, not a stream — no subscription handling.
    async fn handle_meta_connection(
        engine: &mut Self::Engine,
        mut connection: AcceptedConnection,
    ) -> Result<(), Self::Error> {
        let frame = LengthPrefixedCodec::default()
            .read_body_async(connection.stream_mut())
            .await?
            .into_bytes();
        let input = meta_signal_spirit::Signal::<meta_signal_spirit::Query>::from(frame)
            .restore()
            .map_err(|error| SpiritDaemonError::Signal(error.to_string()))?;
        let reply = match input {
            meta_signal_spirit::Query::Configure(request) => engine.configure_async(request).await,
            meta_signal_spirit::Query::ReverseMetaConfiguration => {
                engine.reverse_meta_configuration_async().await
            }
            meta_signal_spirit::Query::Import(request) => engine.import_async(request).await,
            meta_signal_spirit::Query::ObserveHead => engine.observe_head_async().await,
            meta_signal_spirit::Query::ObserveHeadObject => {
                engine.observe_head_object_async().await
            }
        };
        LengthPrefixedCodec::default()
            .write_body_async(
                connection.stream_mut(),
                &LengthPrefixedFrameBody::new(
                    reply
                        .signalize()
                        .map_err(|error| SpiritDaemonError::Signal(error.to_string()))?
                        .bytes()
                        .to_vec(),
                ),
            )
            .await?;
        connection
            .stream_mut()
            .flush()
            .await
            .map_err(FrameError::from)?;
        Ok(())
    }

    fn subscription_filter(input: &Query) -> Option<Self::SubscriptionFilter> {
        match input {
            Query::SubscribeIntent(query) => Some(query.clone()),
            Query::Configure(_)
            | Query::State(_)
            | Query::Record(_)
            | Query::Propose(_)
            | Query::Clarify(_)
            | Query::ResolveClarification(_)
            | Query::Supersede(_)
            | Query::Retire(_)
            | Query::Observe(_)
            | Query::Intent(_)
            | Query::TextSearch(_)
            | Query::Lookup(_)
            | Query::Count(_)
            | Query::BumpImportance(_)
            | Query::ChangeRecord(_)
            | Query::LookupStash(_)
            | Query::Tap(_)
            | Query::Untap(_)
            | Query::Version
            | Query::ApplyAuthorizedRecord(_)
            | Query::Marker => None,
        }
    }

    fn subscription_token(output: &Response) -> Option<Self::SubscriptionToken> {
        match output {
            Response::SubscriptionStarted(subscription) => Some(
                IntentSubscriptionToken::from_signal_token(subscription.subscription_token),
            ),
            _ => None,
        }
    }

    async fn published_event(
        engine: &Self::Engine,
        output: &Response,
    ) -> Result<Option<Self::StreamEvent>, Self::Error> {
        match output {
            Response::RecordAccepted(record_identifier) => Ok(engine
                .intent_recorded_event_async(record_identifier)
                .await?),
            Response::Proposed(record_identifier) => Ok(engine
                .intent_recorded_event_async(record_identifier)
                .await?),
            Response::Clarified(receipt) => {
                Ok(engine.intent_clarified_event_async(receipt).await?)
            }
            Response::Superseded(receipt) => {
                Ok(engine.intent_superseded_event_async(receipt).await?)
            }
            Response::Retired(receipt) => Ok(Some(engine.intent_retired_event(receipt))),
            _ => Ok(None),
        }
    }

    fn event_matches_filter(filter: &Self::SubscriptionFilter, event: &Self::StreamEvent) -> bool {
        match event {
            IntentEvent::IntentRecorded(recorded) => recorded.entry.matches(filter),
            IntentEvent::IntentClarified(clarified) => clarified.entry.matches(filter),
            IntentEvent::IntentSuperseded(_) | IntentEvent::IntentRetired(_) => false,
        }
    }
}

/// The staged advance crossing the emitted daemon spine (§3.5.3): `resolve`
/// is the quorum wait, run on the connection task with NO engine borrow so
/// the engine mailbox keeps serving reads; `conclude` is one fast engine
/// turn that materializes on the grant or discards on any other verdict.
#[cfg(feature = "criome-gate")]
impl crate::component_daemon::StagedAdvance<SpiritDaemon>
    for crate::criome_gate::StagedHeadAdvance
{
    fn resolve<'advance>(
        &'advance mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'advance>> {
        Box::pin(crate::criome_gate::StagedHeadAdvance::resolve(self))
    }

    fn conclude<'engine>(
        self: Box<Self>,
        engine: &'engine mut Engine,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Response, SpiritDaemonError>> + Send + 'engine>,
    > {
        Box::pin(async move { Ok(engine.conclude_staged_advance(*self).await) })
    }
}

/// A thin convenience wrapper so callers (tests, in-process launchers) keep the
/// familiar `Daemon::new(configuration).run()` surface over the emitted
/// `ComponentDaemon` binder. The bin uses the emitted `DaemonEntry` directly.
pub struct Daemon {
    configuration: Configuration,
}

impl Daemon {
    pub fn new(configuration: Configuration) -> Self {
        Self { configuration }
    }

    pub fn run(self) -> Result<(), DaemonError<SpiritDaemon>> {
        tokio::runtime::Runtime::new()
            .map_err(DaemonError::Runtime)?
            .block_on(async {
                SpiritDaemon::bind(self.configuration)?
                    .run()
                    .await
                    .map_err(DaemonError::from)
            })
    }
}
