#[rustfmt::skip]
use thiserror::Error;
#[rustfmt::skip]
use triad_runtime::{
    AcceptedConnection, AsyncListenerError, AsyncListenerSocket,
    AsyncMultiConnectionRuntime, AsyncMultiListenerDaemon, AsyncMultiListenerDaemonError,
    SocketMode, ArgumentError, ComponentCommand, BindingSurface,
    ExitReport, RequestErrorLog,
};
#[rustfmt::skip]
use triad_runtime::EngineRequestError;
#[rustfmt::skip]
use triad_runtime::kameo::Actor;
#[rustfmt::skip]
use triad_runtime::kameo::actor::{ActorRef, Spawn, WeakActorRef};
#[rustfmt::skip]
use triad_runtime::kameo::error::{ActorStopReason, HookError, SendError};
#[rustfmt::skip]
use triad_runtime::kameo::message::{Context, Message};
#[rustfmt::skip]
use tokio::io::AsyncWriteExt;
#[rustfmt::skip]
use triad_runtime::{FrameBody, FrameError, LengthPrefixedCodec};
#[rustfmt::skip]
use signal_spirit::{ByteViewable, Query, Response, Restorable, Signal, Signalizable};
#[rustfmt::skip]
/// The lane one decoded working `Query` runs on. `Immediate` is the
/// single-turn engine ask every component starts with; `Staged` runs
/// the three-phase staged turn — stage under the daemon's advance
/// gate, resolve on the connection task with no engine borrow, then
/// conclude in one more engine turn. Components without a staged
/// intake never return `Staged`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkingQueryLane {
    Immediate,
    Staged,
}
#[rustfmt::skip]
/// The component-facing stage verdict: the stage turn either
/// completed the input outright (a read, a refusal, a mode without
/// staging) or parked a staged advance awaiting external resolution.
pub enum StagedWorkingTurn<Daemon: ComponentDaemon> {
    Completed(Response),
    Awaiting(Box<dyn StagedAdvance<Daemon>>),
}
#[rustfmt::skip]
/// One staged advance crossing the daemon spine. `resolve` runs on
/// the connection task with NO engine borrow — the external wait
/// (for example a cluster authorization round) — storing its verdict
/// internally; `conclude` then runs as one fast engine turn and
/// produces the final `Response`.
pub trait StagedAdvance<Daemon: ComponentDaemon>: Send {
    fn resolve<'advance>(
        &'advance mut self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'advance>>;
    fn conclude<'engine>(
        self: Box<Self>,
        engine: &'engine mut Daemon::Engine,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                Output = Result<Response, Daemon::Error>,
            > + Send + 'engine,
        >,
    >;
}
#[rustfmt::skip]
/// The component hook surface for the emitted daemon — the only daemon
/// code the component hand-writes (record 1488 escape hatches).
///
/// The component declares its `Configuration` / `Engine` / `Error` types
/// and `PROCESS_NAME`, and provides the REQUIRED `build_runtime` (the
/// emitter cannot know how to open the component's Store/Engine) plus the
/// typed working-input handler.
pub trait ComponentDaemon: Sized + 'static {
    type Configuration: BindingSurface;
    type ConfigurationError: std::error::Error;
    type Engine: Send + Sync + 'static;
    type Error: std::fmt::Debug
        + std::fmt::Display
        + From<FrameError>
        + From<String>
        + From<EngineRequestError>
        + Send
        + Sync
        + 'static;
    type SubscriptionToken: Copy + Eq + std::hash::Hash + Send + Sync + 'static;
    type SubscriptionFilter: Clone + Send + Sync + 'static;
    type StreamEvent: Clone
        + rkyv::Archive
        + for<'archive> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'archive>,
                rkyv::rancor::Error,
            >,
        >
        + Send
        + Sync
        + Into<signal_spirit::IntentEvent>
        + 'static;
    const PROCESS_NAME: &'static str;
    /// Build the executable-owned startup snapshot for a zero-argument Nexus.
    /// Persistent desired configuration is read from its stable Sema after this
    /// default supplies a fresh-store seed.
    fn default_configuration() -> Result<Self::Configuration, Self::ConfigurationError>;
    /// Validate the loaded configuration before any runtime, listener,
    /// or store is built. Components that carry only already-validated
    /// typed configuration keep the default no-op; components with decoded
    /// path records override this hook so bad startup shape fails before
    /// socket preparation or state mutation.
    fn validate_configuration(
        configuration: &Self::Configuration,
    ) -> Result<(), Self::ConfigurationError> {
        let _ = configuration;
        Ok(())
    }
    /// Open the component's durable Store and construct its Engine.
    fn build_runtime(
        configuration: &Self::Configuration,
    ) -> Result<Self::Engine, Self::Error>;
    /// Lifecycle: called once before the listener serves, once after it stops.
    fn start(engine: &Self::Engine) -> Result<(), Self::Error> {
        let _ = engine;
        Ok(())
    }
    fn stop(engine: &Self::Engine) -> Result<(), Self::Error> {
        let _ = engine;
        Ok(())
    }
    /// Run one decoded working `Query` through the engine and return the
    /// `Response` root to encode back to the caller.
    ///
    /// `connection` carries the accepted stream's kernel-vouched peer
    /// credentials (uid / gid / pid via `SO_PEERCRED`), so the component can
    /// mint an origin from the operating-system trust boundary rather than
    /// trusting a payload claim. Components that do not classify by origin
    /// take it as `_connection`.
    fn handle_working_input<'connection>(
        engine: &'connection mut Self::Engine,
        input: Query,
        connection: &'connection triad_runtime::ConnectionContext,
    ) -> impl std::future::Future<
        Output = Result<Response, Self::Error>,
    > + Send + 'connection;
    /// The lane a decoded working `Query` runs on. The default keeps
    /// every input on the single-turn `Immediate` ask, so components
    /// without a staged intake are unaffected.
    fn working_input_lane(input: &Query) -> WorkingQueryLane {
        let _ = input;
        WorkingQueryLane::Immediate
    }
    /// Stage one working `Query` — the fast first engine turn of the
    /// staged lane. The default completes immediately through
    /// `handle_working_input`, so a component that never returns
    /// `WorkingQueryLane::Staged` never stages.
    fn stage_working_input<'connection>(
        engine: &'connection mut Self::Engine,
        input: Query,
        connection: &'connection triad_runtime::ConnectionContext,
    ) -> impl std::future::Future<
        Output = Result<StagedWorkingTurn<Self>, Self::Error>,
    > + Send + 'connection {
        async move {
            Ok(
                StagedWorkingTurn::Completed(
                    Self::handle_working_input(engine, input, connection).await?,
                ),
            )
        }
    }
    /// The component's shared advance gate, when it owns one: the
    /// daemon's staged lane serializes staged turns first-in first-out
    /// through this queue-fair lock, and a component can share the same
    /// gate with its own background passes. `None` lets the runtime own
    /// a private gate.
    fn shared_advance_gate(
        engine: &Self::Engine,
    ) -> Option<std::sync::Arc<tokio::sync::Mutex<()>>> {
        let _ = engine;
        None
    }
    /// The subscription filter an `Query` opens, if any. `None` means the
    /// input does not open a stream.
    fn subscription_filter(input: &Query) -> Option<Self::SubscriptionFilter>;
    /// The stream token an `Response` carries when it acknowledges a new
    /// subscription, if any.
    fn subscription_token(output: &Response) -> Option<Self::SubscriptionToken>;
    /// The stream event a committed `Response` publishes, if any.
    fn published_event<'event>(
        engine: &'event Self::Engine,
        output: &'event Response,
    ) -> impl std::future::Future<
        Output = Result<Option<Self::StreamEvent>, Self::Error>,
    > + Send + 'event;
    /// Whether a stream event matches a registered subscription filter.
    fn event_matches_filter(
        filter: &Self::SubscriptionFilter,
        event: &Self::StreamEvent,
    ) -> bool;
    fn handle_meta_connection(
        engine: &mut Self::Engine,
        connection: AcceptedConnection,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send + '_;
}
#[rustfmt::skip]
/// A zero-argument executable entrypoint -> default configuration -> bound daemon.
/// Configuration archives are offline migration artifacts, never daemon startup input.
pub struct DaemonCommand<Daemon: ComponentDaemon> {
    command: ComponentCommand,
    daemon: std::marker::PhantomData<fn() -> Daemon>,
}
#[rustfmt::skip]
impl<Daemon: ComponentDaemon> DaemonCommand<Daemon> {
    pub fn from_environment() -> Self {
        Self {
            command: ComponentCommand::from_environment(),
            daemon: std::marker::PhantomData,
        }
    }
    pub fn from_arguments<Arguments, Argument>(arguments: Arguments) -> Self
    where
        Arguments: IntoIterator<Item = Argument>,
        Argument: Into<String>,
    {
        Self {
            command: ComponentCommand::from_arguments(arguments),
            daemon: std::marker::PhantomData,
        }
    }
    pub fn configuration(&self) -> Result<Daemon::Configuration, DaemonError<Daemon>> {
        let count = self.command.argument_count();
        if count != 0 {
            return Err(DaemonError::Argument(ArgumentError::ArgumentCount { count }));
        }
        let configuration = Daemon::default_configuration()
            .map_err(DaemonError::Configuration)?;
        Daemon::validate_configuration(&configuration)
            .map_err(DaemonError::Configuration)?;
        Ok(configuration)
    }
    pub fn run(&self) -> Result<(), DaemonError<Daemon>> {
        tokio::runtime::Runtime::new()
            .map_err(DaemonError::Runtime)?
            .block_on(async {
                Daemon::bind(self.configuration()?)?
                    .run()
                    .await
                    .map_err(DaemonError::from)
            })
    }
}
#[rustfmt::skip]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ListenerTier {
    Working,
    Meta,
}
#[rustfmt::skip]
impl std::fmt::Display for ListenerTier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Working => formatter.write_str("working"),
            Self::Meta => formatter.write_str("meta"),
        }
    }
}
#[rustfmt::skip]
/// The bound daemon constructor on the component trait: builds the engine,
/// wraps it in the generated actor connection runtime, and returns the
/// async task-backed listener shell the `DaemonCommand` drives. The component
/// never writes this by hand — it is emitted as a default method on
/// `ComponentDaemon`.
pub trait DaemonBinder: ComponentDaemon {
    fn bind(
        configuration: Self::Configuration,
    ) -> Result<
        AsyncMultiListenerDaemon<GeneratedDaemonRuntime<Self>>,
        DaemonError<Self>,
    > {
        let engine = Self::build_runtime(&configuration)
            .map_err(DaemonError::Component)?;
        let runtime = GeneratedDaemonRuntime::<Self>::new(engine);
        Ok({
            let working_socket = AsyncListenerSocket::new(
                ListenerTier::Working,
                configuration.socket_path().to_path_buf(),
            );
            let working_socket = match configuration.socket_mode() {
                Some(socket_mode) => working_socket.with_socket_mode(socket_mode),
                None => working_socket,
            };
            let mut listener_sockets = std::vec![working_socket];
            let meta_socket_path = configuration
                .meta_socket_path()
                .ok_or(DaemonError::MissingMetaSocket)?
                .to_path_buf();
            listener_sockets
                .push(
                    AsyncListenerSocket::new(ListenerTier::Meta, meta_socket_path)
                        .with_socket_mode(SocketMode::new(0o600)),
                );
            AsyncMultiListenerDaemon::new(
                    listener_sockets,
                    runtime.clone(),
                    RequestErrorLog::new(Self::PROCESS_NAME),
                )
                .with_concurrency_limit(configuration.request_concurrency_limit())
        })
    }
}
#[rustfmt::skip]
impl<Daemon: ComponentDaemon> DaemonBinder for Daemon {}
#[rustfmt::skip]
type SubscriptionWriter = Box<dyn tokio::io::AsyncWrite + Unpin + Send>;
#[rustfmt::skip]
/// The stream-aware working-tier transport over one accepted Tokio stream:
/// a length-prefixed envelope around the schema-emitted signal frame codec,
/// plus an owned writer half that can remain registered for pushed events.
struct WorkingTransport<Reader, Writer> {
    reader: Reader,
    writer: Writer,
    context: triad_runtime::ConnectionContext,
}
#[rustfmt::skip]
impl<Stream> WorkingTransport<tokio::io::ReadHalf<Stream>, tokio::io::WriteHalf<Stream>>
where
    Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    fn from_connection(connection: AcceptedConnection<Stream>) -> Self {
        let (stream, context) = connection.into_parts();
        let (reader, writer) = tokio::io::split(stream);
        Self { reader, writer, context }
    }
}
#[rustfmt::skip]
impl<Reader, Writer> WorkingTransport<Reader, Writer>
where
    Reader: tokio::io::AsyncRead + Unpin,
    Writer: tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    fn context(&self) -> &triad_runtime::ConnectionContext {
        &self.context
    }
    async fn read_frame(&mut self) -> Result<Vec<u8>, FrameError> {
        Ok(
            LengthPrefixedCodec::default()
                .read_body_async(&mut self.reader)
                .await?
                .into_bytes(),
        )
    }
    async fn write_frame(&mut self, frame: Vec<u8>) -> Result<(), FrameError> {
        LengthPrefixedCodec::default()
            .write_body_async(&mut self.writer, &FrameBody::new(frame))
            .await?;
        self.writer.flush().await?;
        Ok(())
    }
    fn into_writer(self) -> SubscriptionWriter {
        Box::new(self.writer)
    }
}
#[rustfmt::skip]
/// Async task-backed subscription plumbing over retained Tokio writer halves.
///
/// The component supplies the stream vocabulary and filter policy through
/// `ComponentDaemon`; the generated runtime owns the common registry and
/// length-prefixed event delivery mechanics.
type SubscriptionState<Daemon> = std::collections::HashMap<
    <Daemon as ComponentDaemon>::SubscriptionToken,
    (<Daemon as ComponentDaemon>::SubscriptionFilter, SubscriptionWriter),
>;

pub struct EmittedSubscriptions<Daemon: ComponentDaemon> {
    state: tokio::sync::Mutex<SubscriptionState<Daemon>>,
}

impl<Daemon: ComponentDaemon> Default for EmittedSubscriptions<Daemon> {
    fn default() -> Self {
        Self {
            state: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
}

impl<Daemon: ComponentDaemon> EmittedSubscriptions<Daemon> {
    async fn register(
        &self,
        token: Daemon::SubscriptionToken,
        filter: Daemon::SubscriptionFilter,
        writer: SubscriptionWriter,
    ) {
        self.state.lock().await.insert(token, (filter, writer));
    }

    async fn publish(&self, event: Daemon::StreamEvent) -> Result<usize, Daemon::Error> {
        let mut subscriptions = self.state.lock().await;
        let response = Response::Event(event.clone().into());
        let bytes = response
            .signalize()
            .map_err(|error| Daemon::Error::from(error.to_string()))?
            .bytes()
            .to_vec();
        let mut delivered = 0;
        let mut stale = Vec::new();
        for (token, (filter, writer)) in subscriptions.iter_mut() {
            if !Daemon::event_matches_filter(filter, &event) {
                continue;
            }
            let delivery = async {
                LengthPrefixedCodec::default()
                    .write_body_async(writer, &FrameBody::new(bytes.clone()))
                    .await?;
                writer.flush().await.map_err(FrameError::from)
            }
            .await;
            match delivery {
                Ok(()) => delivered += 1,
                Err(_) => stale.push(*token),
            }
        }
        for token in stale {
            subscriptions.remove(&token);
        }
        Ok(delivered)
    }
}

/// The kameo actor that owns the component engine. The mailbox
/// serialises every request, giving each handler exclusive `&mut`
/// access to the engine without a component-internal lock.
pub struct EngineActor<Daemon: ComponentDaemon> {
    engine: Daemon::Engine,
}
#[rustfmt::skip]
impl<Daemon: ComponentDaemon> Actor for EngineActor<Daemon> {
    type Args = Self;
    type Error = Daemon::Error;
    async fn on_start(
        actor: Self::Args,
        _actor_reference: ActorRef<Self>,
    ) -> Result<Self, Self::Error> {
        Daemon::start(&actor.engine)?;
        Ok(actor)
    }
    async fn on_stop(
        &mut self,
        _actor_reference: WeakActorRef<Self>,
        _reason: ActorStopReason,
    ) -> Result<(), Self::Error> {
        Daemon::stop(&self.engine)
    }
}
#[rustfmt::skip]
/// The stream actor's working reply: the encoded `Response` plus the
/// stream event the committed output published, computed together
/// inside the engine actor's exclusive `&mut` handler.
pub struct WorkingOutcome<Daemon: ComponentDaemon> {
    output: Response,
    event: Option<Daemon::StreamEvent>,
}
#[rustfmt::skip]
#[derive(Debug)]
pub struct WorkingQuery {
    input: Query,
    context: triad_runtime::ConnectionContext,
}
#[rustfmt::skip]
impl<Daemon: ComponentDaemon> Message<WorkingQuery> for EngineActor<Daemon> {
    type Reply = Result<WorkingOutcome<Daemon>, Daemon::Error>;
    async fn handle(
        &mut self,
        message: WorkingQuery,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let output = Daemon::handle_working_input(
                &mut self.engine,
                message.input,
                &message.context,
            )
            .await?;
        let event = Daemon::published_event(&self.engine, &output).await?;
        Ok(WorkingOutcome { output, event })
    }
}
#[rustfmt::skip]
/// The engine actor's stage reply: `Completed` carries the finished
/// outcome; `Awaiting` carries the staged advance the connection task
/// resolves before the concluding engine turn.
pub enum StagedWorkingReply<Daemon: ComponentDaemon> {
    Completed(WorkingOutcome<Daemon>),
    Awaiting(Box<dyn StagedAdvance<Daemon>>),
}
#[rustfmt::skip]
pub struct StageWorkingQuery {
    input: Query,
    context: triad_runtime::ConnectionContext,
}
#[rustfmt::skip]
impl<Daemon: ComponentDaemon> Message<StageWorkingQuery> for EngineActor<Daemon> {
    type Reply = Result<StagedWorkingReply<Daemon>, Daemon::Error>;
    async fn handle(
        &mut self,
        message: StageWorkingQuery,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match Daemon::stage_working_input(
                &mut self.engine,
                message.input,
                &message.context,
            )
            .await?
        {
            StagedWorkingTurn::Completed(output) => {
                let event = Daemon::published_event(&self.engine, &output).await?;
                Ok(StagedWorkingReply::Completed(WorkingOutcome { output, event }))
            }
            StagedWorkingTurn::Awaiting(advance) => {
                Ok(StagedWorkingReply::Awaiting(advance))
            }
        }
    }
}
#[rustfmt::skip]
pub struct ConcludeWorkingQuery<Daemon: ComponentDaemon> {
    advance: Box<dyn StagedAdvance<Daemon>>,
}
#[rustfmt::skip]
impl<Daemon: ComponentDaemon> Message<ConcludeWorkingQuery<Daemon>>
for EngineActor<Daemon> {
    type Reply = Result<WorkingOutcome<Daemon>, Daemon::Error>;
    async fn handle(
        &mut self,
        message: ConcludeWorkingQuery<Daemon>,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let output = message.advance.conclude(&mut self.engine).await?;
        let event = Daemon::published_event(&self.engine, &output).await?;
        Ok(WorkingOutcome { output, event })
    }
}
#[rustfmt::skip]
pub struct MetaConnection {
    connection: AcceptedConnection,
}
#[rustfmt::skip]
impl<Daemon: ComponentDaemon> Message<MetaConnection> for EngineActor<Daemon> {
    type Reply = Result<(), Daemon::Error>;
    async fn handle(
        &mut self,
        message: MetaConnection,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Daemon::handle_meta_connection(&mut self.engine, message.connection).await
    }
}
#[rustfmt::skip]
/// The generated runtime struct holds an `ActorRef` to the engine
/// actor. Its `handle_connection` IS the async decode -> ask -> encode
/// spine; the engine state lives behind the actor mailbox. The advance
/// gate serializes staged working turns first-in first-out across
/// their stage, resolve, and conclude phases.
pub struct GeneratedDaemonRuntime<Daemon: ComponentDaemon> {
    engine: ActorRef<EngineActor<Daemon>>,
    advance_gate: std::sync::Arc<tokio::sync::Mutex<()>>,
    subscriptions: std::sync::Arc<EmittedSubscriptions<Daemon>>,
}
#[rustfmt::skip]
impl<Daemon: ComponentDaemon> GeneratedDaemonRuntime<Daemon> {
    fn new(engine: Daemon::Engine) -> Self {
        let advance_gate = Daemon::shared_advance_gate(&engine).unwrap_or_default();
        Self {
            engine: EngineActor::<Daemon>::spawn(EngineActor { engine }),
            advance_gate,
            subscriptions: std::sync::Arc::new(EmittedSubscriptions::default()),
        }
    }
    /// Translate a kameo `SendError` from an engine `ask` into the
    /// component's typed `Error` via `EngineRequestError`.
    fn engine_send_error<Request>(
        error: SendError<Request, Daemon::Error>,
    ) -> Daemon::Error {
        match error {
            SendError::HandlerError(error) => error,
            SendError::ActorNotRunning(_) => {
                EngineRequestError::new("engine actor is not running").into()
            }
            SendError::ActorStopped => {
                EngineRequestError::new("engine actor stopped before replying").into()
            }
            SendError::MailboxFull(_) => {
                EngineRequestError::new("engine actor mailbox is full").into()
            }
            SendError::Timeout(_) => {
                EngineRequestError::new("engine actor request timed out").into()
            }
        }
    }
    async fn handle_working_connection<Stream>(
        &self,
        connection: AcceptedConnection<Stream>,
    ) -> Result<(), Daemon::Error>
    where
        Stream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let mut transport = WorkingTransport::from_connection(connection);
        let frame = transport.read_frame().await?;
        let input = Signal::<Query>::from(frame).restore().map_err(|error| Daemon::Error::from(error.to_string()))?;
        let filter = Daemon::subscription_filter(&input);
        let context = *transport.context();
        let outcome = match Daemon::working_input_lane(&input) {
            WorkingQueryLane::Immediate => {
                match self.engine.ask(WorkingQuery { input, context }).await {
                    Ok(outcome) => outcome,
                    Err(error) => return Err(Self::engine_send_error(error)),
                }
            }
            WorkingQueryLane::Staged => {
                let _advance_turn = self.advance_gate.lock().await;
                let staged = match self
                    .engine
                    .ask(StageWorkingQuery {
                        input,
                        context,
                    })
                    .await
                {
                    Ok(staged) => staged,
                    Err(error) => return Err(Self::engine_send_error(error)),
                };
                match staged {
                    StagedWorkingReply::Completed(outcome) => outcome,
                    StagedWorkingReply::Awaiting(mut advance) => {
                        advance.resolve().await;
                        match self.engine.ask(ConcludeWorkingQuery { advance }).await {
                            Ok(outcome) => outcome,
                            Err(error) => {
                                return Err(Self::engine_send_error(error));
                            }
                        }
                    }
                }
            }
        };
        let signal = outcome.output.signalize().map_err(|error| Daemon::Error::from(error.to_string()))?;
        transport.write_frame(signal.bytes().to_vec()).await?;
        if let (Some(filter), Some(token)) = (
            filter,
            Daemon::subscription_token(&outcome.output),
        ) {
            self.subscriptions.register(token, filter, transport.into_writer()).await;
        }
        if let Some(event) = outcome.event {
            self.subscriptions.publish(event).await?;
        }
        Ok(())
    }
    async fn handle_meta_connection(
        &self,
        connection: AcceptedConnection,
    ) -> Result<(), Daemon::Error> {
        match self.engine.ask(MetaConnection { connection }).await {
            Ok(()) => Ok(()),
            Err(SendError::HandlerError(error)) => Err(error),
            Err(SendError::ActorNotRunning(_)) => {
                Err(EngineRequestError::new("engine actor is not running").into())
            }
            Err(SendError::ActorStopped) => {
                Err(
                    EngineRequestError::new("engine actor stopped before replying")
                        .into(),
                )
            }
            Err(SendError::MailboxFull(_)) => {
                Err(EngineRequestError::new("engine actor mailbox is full").into())
            }
            Err(SendError::Timeout(_)) => {
                Err(EngineRequestError::new("engine actor request timed out").into())
            }
        }
    }
}
#[rustfmt::skip]
impl<Daemon: ComponentDaemon> Clone for GeneratedDaemonRuntime<Daemon> {
    fn clone(&self) -> Self {
        Self {
            engine: self.engine.clone(),
            advance_gate: self.advance_gate.clone(),
            subscriptions: self.subscriptions.clone(),
        }
    }
}
#[rustfmt::skip]
impl<Daemon: ComponentDaemon> AsyncMultiConnectionRuntime
for GeneratedDaemonRuntime<Daemon> {
    type Listener = ListenerTier;
    type Error = Daemon::Error;
    async fn start(&self) -> Result<(), Daemon::Error> {
        self.engine
            .wait_for_startup_with_result(|result| match result {
                Ok(()) => Ok(()),
                Err(HookError::Error(error)) => {
                    Err(
                        EngineRequestError::new(
                                format!("engine actor failed to start: {error:?}"),
                            )
                            .into(),
                    )
                }
                Err(HookError::Panicked(_)) => {
                    Err(
                        EngineRequestError::new("engine actor panicked during startup")
                            .into(),
                    )
                }
            })
            .await
    }
    async fn stop(&self) -> Result<(), Daemon::Error> {
        let _ = self.engine.stop_gracefully().await;
        self.engine.wait_for_shutdown().await;
        Ok(())
    }
    async fn handle_connection(
        &self,
        listener: Self::Listener,
        connection: AcceptedConnection,
    ) -> Result<(), Self::Error> {
        match listener {
            ListenerTier::Working => self.handle_working_connection(connection).await,
            ListenerTier::Meta => self.handle_meta_connection(connection).await,
        }
    }
}
#[rustfmt::skip]
/// The emitted daemon error: argv, configuration, listener, and the
/// component error. The component's own error rides the `Component` arm.
#[derive(Debug, Error)]
pub enum DaemonError<Daemon: ComponentDaemon> {
    #[error("daemon argument error: {0}")]
    Argument(ArgumentError),
    #[error("daemon configuration error: {0}")]
    Configuration(Daemon::ConfigurationError),
    #[error("daemon runtime error: {0}")]
    Runtime(std::io::Error),
    #[error("daemon listener error: {0}")]
    Listener(AsyncListenerError),
    #[error("daemon meta socket path missing from configuration")]
    MissingMetaSocket,
    #[error("component error: {0}")]
    Component(Daemon::Error),
}
#[rustfmt::skip]
impl<Daemon: ComponentDaemon> From<ArgumentError> for DaemonError<Daemon> {
    fn from(error: ArgumentError) -> Self {
        Self::Argument(error)
    }
}
#[rustfmt::skip]
impl<Daemon: ComponentDaemon> From<AsyncMultiListenerDaemonError<Daemon::Error>>
for DaemonError<Daemon> {
    fn from(error: AsyncMultiListenerDaemonError<Daemon::Error>) -> Self {
        match error {
            AsyncMultiListenerDaemonError::Listener(error) => Self::Listener(error),
            AsyncMultiListenerDaemonError::Start(error)
            | AsyncMultiListenerDaemonError::Stop(error) => Self::Component(error),
        }
    }
}
#[rustfmt::skip]
/// The component-agnostic exit body. The component's binary calls
/// `<SpiritDaemon as DaemonEntry>::run_to_exit_code()` from `fn main`.
pub trait DaemonEntry: ComponentDaemon {
    fn run_to_exit_code() -> std::process::ExitCode {
        ExitReport::new(Self::PROCESS_NAME)
            .from_result(DaemonCommand::<Self>::from_environment().run())
    }
}
#[rustfmt::skip]
impl<Daemon: ComponentDaemon> DaemonEntry for Daemon {}
