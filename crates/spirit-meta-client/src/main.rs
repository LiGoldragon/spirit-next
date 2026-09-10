use std::env;

use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use meta_signal_spirit::Query;
use protos::{Protosizable, ReaderBudget, Textualizable};
use spirit::{MetaSignalTransport, MetaTransportError};
use thiserror::Error;

fn main() {
    if let Err(error) = MetaSpiritCli::from_environment().run() {
        eprintln!("spirit-meta: {error}");
        std::process::exit(1);
    }
}

struct MetaSpiritCli {
    text: String,
}

impl MetaSpiritCli {
    fn from_environment() -> Self {
        let arguments = env::args().skip(1).collect::<Vec<_>>();
        Self {
            text: match arguments.as_slice() {
                [text] => text.clone(),
                _ => String::new(),
            },
        }
    }
    fn run(&self) -> Result<(), MetaSpiritCliError> {
        if self.text.is_empty() {
            return Err(MetaSpiritCliError::ArgumentCount);
        }
        let mut pending = Potential::<Query>::from(self.text.clone());
        let input = pending
            .actualize(&mut Budget {
                remaining: 1024,
                reader: ReaderBudget { remaining: 1024 },
                depth: 0,
                maximum_depth: 1024,
            })
            .map_err(|error| MetaSpiritCliError::Datom(format!("{error:?}")))?;
        let socket_path = env::var("SPIRIT_META_SOCKET")
            .unwrap_or_else(|_| String::from("/tmp/meta-spirit.sock"));
        let mut transport = MetaSignalTransport::connect(socket_path)?;
        let output = transport.exchange(&input)?;
        println!("{}", output.datomize(vec![]).protosize().textualize());
        Ok(())
    }
}

#[derive(Debug, Error)]
enum MetaSpiritCliError {
    #[error("spirit-meta requires exactly one Datom query")]
    ArgumentCount,
    #[error("invalid Datom query: {0}")]
    Datom(String),
    #[error("meta transport error: {0}")]
    Transport(#[from] MetaTransportError),
}
