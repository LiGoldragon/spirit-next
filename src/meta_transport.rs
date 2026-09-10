use meta_signal_spirit::{ByteViewable, Query, Response, Restorable, Signal, Signalizable};
use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
    path::Path,
};
use thiserror::Error;
use triad_runtime::{FrameBody as LengthPrefixedFrameBody, FrameError, LengthPrefixedCodec};

#[derive(Debug, Error)]
pub enum MetaTransportError {
    #[error("meta transport IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("meta signal archive error: {0}")]
    Signal(String),
    #[error("meta transport frame error: {0}")]
    Frame(#[from] FrameError),
}
pub struct MetaSignalTransport<Stream> {
    stream: Stream,
}
impl MetaSignalTransport<UnixStream> {
    pub fn connect(socket_path: impl AsRef<Path>) -> Result<Self, MetaTransportError> {
        Ok(Self::new(UnixStream::connect(socket_path)?))
    }
}
impl<Stream: Read + Write> MetaSignalTransport<Stream> {
    pub fn new(stream: Stream) -> Self {
        Self { stream }
    }
    pub fn exchange(&mut self, query: &Query) -> Result<Response, MetaTransportError> {
        self.write_input(query)?;
        self.read_output()
    }
    pub fn configure(
        &mut self,
        request: meta_signal_spirit::ConfigureRequest,
    ) -> Result<Response, MetaTransportError> {
        self.exchange(&Query::Configure(request))
    }
    pub fn write_input(&mut self, query: &Query) -> Result<(), MetaTransportError> {
        self.write_signal(query)
    }
    pub fn read_input(&mut self) -> Result<Query, MetaTransportError> {
        self.read_signal()
    }
    pub fn write_output(&mut self, response: &Response) -> Result<(), MetaTransportError> {
        self.write_signal(response)
    }
    pub fn read_output(&mut self) -> Result<Response, MetaTransportError> {
        self.read_signal()
    }
    fn write_signal<T: Signalizable>(&mut self, value: &T) -> Result<(), MetaTransportError> {
        let signal = value
            .signalize()
            .map_err(|e| MetaTransportError::Signal(e.to_string()))?;
        LengthPrefixedCodec::default().write_body(
            &mut self.stream,
            &LengthPrefixedFrameBody::new(signal.bytes().to_vec()),
        )?;
        self.stream.flush()?;
        Ok(())
    }
    fn read_signal<T>(&mut self) -> Result<T, MetaTransportError>
    where
        Signal<T>: Restorable<T>,
    {
        let bytes = LengthPrefixedCodec::default()
            .read_body(&mut self.stream)?
            .into_bytes();
        Signal::<T>::from(bytes)
            .restore()
            .map_err(|e| MetaTransportError::Signal(e.to_string()))
    }
}
