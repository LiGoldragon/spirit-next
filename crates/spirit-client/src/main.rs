use std::{env, io::ErrorKind, os::unix::net::UnixStream};

use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use protos::{Protosizable, ReaderBudget, Textualizable};
use signal_spirit::{Query, Response};
use spirit::{SignalTransport, TransportError};
use thiserror::Error;

fn main() {
    if let Err(error) = SpiritCli::from_environment().run() {
        eprintln!("spirit: {error}");
        std::process::exit(1);
    }
}

struct SpiritCli {
    text: String,
}

impl SpiritCli {
    fn from_environment() -> Self {
        let arguments = env::args().skip(1).collect::<Vec<_>>();
        let text = match arguments.as_slice() {
            [text] => text.clone(),
            _ => String::new(),
        };
        Self { text }
    }

    fn run(&self) -> Result<(), SpiritCliError> {
        if self.text.is_empty() {
            return Err(SpiritCliError::ArgumentCount);
        }
        let input = self.parse_input()?;
        let socket_path =
            env::var("SPIRIT_SOCKET").unwrap_or_else(|_| String::from("/tmp/spirit.sock"));
        #[cfg(feature = "testing-trace")]
        let trace_listener = env::var("SPIRIT_TRACE_SOCKET")
            .ok()
            .map(TraceListener::bind)
            .transpose()
            .map_err(SpiritCliError::TraceIo)?;
        let opens_subscription = matches!(&input, Query::SubscribeIntent(_));
        let mut transport = SignalTransport::connect(socket_path)?;
        let output = transport.exchange(&input)?;
        Self::print_response(output)?;
        if opens_subscription {
            self.print_subscription_events(&mut transport)?;
        }
        #[cfg(feature = "testing-trace")]
        if let Some(listener) = trace_listener {
            for event in listener.collect_until(
                tokio::time::Instant::now() + std::time::Duration::from_millis(200),
            )? {
                println!("{}", event.datomize(vec![]).protosize().textualize());
            }
        }
        Ok(())
    }

    fn parse_input(&self) -> Result<Query, SpiritCliError> {
        let mut potential = Potential::<Query>::from(self.text.clone());
        potential
            .actualize(&mut Budget {
                remaining: 1024,
                reader: ReaderBudget { remaining: 1024 },
                depth: 0,
                maximum_depth: 1024,
            })
            .map_err(|error| SpiritCliError::Datom(format!("{error:?}")))
    }

    fn print_response(response: Response) -> Result<(), SpiritCliError> {
        let text = response.datomize(vec![]).protosize().textualize();
        println!("{text}");
        Ok(())
    }

    fn print_subscription_events(
        &self,
        transport: &mut SignalTransport<UnixStream>,
    ) -> Result<(), SpiritCliError> {
        loop {
            match transport.read_output() {
                Ok(response) => Self::print_response(response)?,
                Err(TransportError::Frame(triad_runtime::FrameError::Io(error)))
                    if error.kind() == ErrorKind::UnexpectedEof =>
                {
                    return Ok(());
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
}

#[derive(Debug, Error)]
enum SpiritCliError {
    #[error("spirit requires exactly one Datom query")]
    ArgumentCount,
    #[error("invalid Datom query: {0}")]
    Datom(String),
    #[error("transport error: {0}")]
    Transport(#[from] TransportError),
    #[cfg(feature = "testing-trace")]
    #[error("trace I/O: {0}")]
    TraceIo(#[from] std::io::Error),
}

#[cfg(feature = "testing-trace")]
struct TraceListener {
    listener: std::os::unix::net::UnixListener,
    path: std::path::PathBuf,
}
#[cfg(feature = "testing-trace")]
trait TraceBindable {
    fn bind(path: impl AsRef<std::path::Path>) -> std::io::Result<Self>
    where
        Self: Sized;
}

#[cfg(feature = "testing-trace")]
impl TraceBindable for TraceListener {
    fn bind(path: impl AsRef<std::path::Path>) -> std::io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let _ = std::fs::remove_file(&path);
        let listener = std::os::unix::net::UnixListener::bind(&path)?;
        listener.set_nonblocking(true)?;
        Ok(Self { listener, path })
    }
}

#[cfg(feature = "testing-trace")]
impl Drop for TraceListener {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(feature = "testing-trace")]
trait TraceCollecting {
    fn collect_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> std::io::Result<Vec<signal_introspect::ComponentTraceEvent>>;
}
#[cfg(feature = "testing-trace")]
impl TraceCollecting for TraceListener {
    fn collect_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> std::io::Result<Vec<signal_introspect::ComponentTraceEvent>> {
        let listener = self.listener.try_clone()?;
        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async move {
            use signal_introspect::{Restorable, Signal};
            use tokio::io::AsyncReadExt;
            let listener = tokio::net::UnixListener::from_std(listener)?;
            let mut events = Vec::new();
            while let Ok(Ok((mut stream, _))) =
                tokio::time::timeout_at(deadline, listener.accept()).await
            {
                let mut header = [0; 4];
                stream.read_exact(&mut header).await?;
                let length = u32::from_le_bytes(header) as usize;
                if length > 1024 * 1024 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "trace frame too large",
                    ));
                };
                let mut bytes = vec![0; length];
                stream.read_exact(&mut bytes).await?;
                events.push(
                    Signal::<signal_introspect::ComponentTraceEvent>::from(bytes)
                        .restore()
                        .map_err(|_| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "invalid trace archive",
                            )
                        })?,
                );
            }
            Ok(events)
        })
    }
}
