use std::env;

use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use protos::{Protosizable, ReaderBudget, Textualizable};
use spirit::{StoreMigration, StoreMigrationError, StoreMigrationRequest};
use thiserror::Error;

fn main() {
    if let Err(error) = StoreMigrationCli::from_environment().run() {
        eprintln!("spirit-migrate-store: {error}");
        std::process::exit(1);
    }
}

struct StoreMigrationCli {
    text: String,
}

impl StoreMigrationCli {
    fn from_environment() -> Self {
        let arguments = env::args().skip(1).collect::<Vec<_>>();
        Self {
            text: match arguments.as_slice() {
                [text] => text.clone(),
                _ => String::new(),
            },
        }
    }
    fn run(&self) -> Result<(), StoreMigrationCliError> {
        if self.text.is_empty() {
            return Err(StoreMigrationCliError::ArgumentCount);
        }
        let mut pending = Potential::<StoreMigrationRequest>::from(self.text.clone());
        let request = pending
            .actualize(&mut Budget {
                remaining: 1024,
                reader: ReaderBudget { remaining: 1024 },
                depth: 0,
                maximum_depth: 1024,
            })
            .map_err(|error| StoreMigrationCliError::Datom(format!("{error:?}")))?;
        let output = StoreMigration::new(request).run()?;
        println!("{}", output.datomize(vec![]).protosize().textualize());
        Ok(())
    }
}

#[derive(Debug, Error)]
enum StoreMigrationCliError {
    #[error("spirit-migrate-store requires exactly one Datom request")]
    ArgumentCount,
    #[error("invalid Datom migration request: {0}")]
    Datom(String),
    #[error(transparent)]
    Migration(#[from] StoreMigrationError),
}
