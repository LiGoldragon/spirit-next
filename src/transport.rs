use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
    path::Path,
};

use thiserror::Error;
use triad_runtime::{FrameBody as LengthPrefixedFrameBody, FrameError, LengthPrefixedCodec};

use signal_spirit::{ByteViewable, Query, Response, Restorable, Signal, Signalizable};

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("transport IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("signal archive error: {0}")]
    Signal(String),
    #[error("transport frame error: {0}")]
    Frame(#[from] FrameError),
}

pub struct SignalTransport<Stream> {
    stream: Stream,
}
impl SignalTransport<UnixStream> {
    pub fn connect(socket_path: impl AsRef<Path>) -> Result<Self, TransportError> {
        Ok(Self::new(UnixStream::connect(socket_path)?))
    }
}
impl<Stream: Read + Write> SignalTransport<Stream> {
    pub fn new(stream: Stream) -> Self {
        Self { stream }
    }
    pub fn exchange(&mut self, query: &Query) -> Result<Response, TransportError> {
        self.write_input(query)?;
        self.read_output()
    }
    pub fn write_input(&mut self, query: &Query) -> Result<(), TransportError> {
        self.write_signal(query)
    }
    pub fn read_input(&mut self) -> Result<Query, TransportError> {
        self.read_signal()
    }
    pub fn write_output(&mut self, response: &Response) -> Result<(), TransportError> {
        self.write_signal(response)
    }
    pub fn read_output(&mut self) -> Result<Response, TransportError> {
        self.read_signal()
    }
    fn write_signal<T: Signalizable>(&mut self, value: &T) -> Result<(), TransportError> {
        let signal = value
            .signalize()
            .map_err(|error| TransportError::Signal(error.to_string()))?;
        LengthPrefixedCodec::default().write_body(
            &mut self.stream,
            &LengthPrefixedFrameBody::new(signal.bytes().to_vec()),
        )?;
        self.stream.flush()?;
        Ok(())
    }
    fn read_signal<T>(&mut self) -> Result<T, TransportError>
    where
        Signal<T>: Restorable<T>,
    {
        let bytes = LengthPrefixedCodec::default()
            .read_body(&mut self.stream)?
            .into_bytes();
        Signal::<T>::from(bytes)
            .restore()
            .map_err(|error| TransportError::Signal(error.to_string()))
    }
}
