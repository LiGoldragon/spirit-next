use std::{
    env, fs,
    path::{Path, PathBuf},
};

use datom_codec::{Actualizing, Budget, Compositional, Datomizable, Potential};
use protos::{Protosizable, ReaderBudget, Textualizable};
use signal_spirit::{
    AuthorizationMode, ConfigurationPath, SpiritGuardianAgentConfiguration,
    SpiritNexusConfiguration,
};
use thiserror::Error;

fn main() {
    if let Err(error) = ConfigurationWriterCli::from_environment().run() {
        eprintln!("spirit-write-configuration: {error}");
        std::process::exit(1);
    }
}

struct ConfigurationWriterCli {
    text: String,
}

#[derive(Debug, Clone, PartialEq, Datomizable, Compositional)]
struct ConfigurationWriteRequest {
    socket_path: ConfigurationWriterPath,
    meta_socket_path: Option<ConfigurationWriterPath>,
    trace_socket_path: Option<ConfigurationWriterPath>,
    authorization_mode: ConfigurationWriterAuthorizationMode,
    guardian_agent_configuration: Option<ConfigurationWriterGuardianAgent>,
    output_path: ConfigurationWriterPath,
}

#[derive(Debug, Clone, PartialEq, Datomizable, Compositional)]
enum ConfigurationWriterInput {
    ConfigurationWriteRequest(ConfigurationWriteRequest),
}

#[derive(Debug, Clone, PartialEq, Datomizable, Compositional)]
struct ConfigurationWriterPath(String);

#[derive(Debug, Clone, PartialEq, Datomizable, Compositional)]
enum ConfigurationWriterAuthorizationMode {
    Gating,
    Observing,
}

#[derive(Debug, Clone, PartialEq, Datomizable, Compositional)]
struct ConfigurationWriterProviderName(String);

#[derive(Debug, Clone, PartialEq, Datomizable, Compositional)]
struct ConfigurationWriterModelName(String);

#[derive(Debug, Clone, PartialEq, Datomizable, Compositional)]
struct ConfigurationWriterTimeoutMilliseconds(i64);

#[derive(Debug, Clone, PartialEq, Datomizable, Compositional)]
struct ConfigurationWriterMaximumOutputTokens(i64);

#[derive(Debug, Clone, PartialEq, Datomizable, Compositional)]
struct ConfigurationWriterGuardianAgent {
    agent_socket_path: ConfigurationWriterPath,
    provider_name: Option<ConfigurationWriterProviderName>,
    model_name: Option<ConfigurationWriterModelName>,
    timeout_milliseconds: ConfigurationWriterTimeoutMilliseconds,
    maximum_output_tokens: Option<ConfigurationWriterMaximumOutputTokens>,
}

#[derive(Debug, Clone, PartialEq, Datomizable, Compositional)]
enum ConfigurationWriteOutput {
    ConfigurationWritten(ConfigurationWriterPath),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn displays_current_request_shape() {
        let request =
            ConfigurationWriterInput::ConfigurationWriteRequest(ConfigurationWriteRequest {
                socket_path: ConfigurationWriterPath("/tmp/spirit.sock".into()),
                meta_socket_path: Some(ConfigurationWriterPath("/tmp/meta.sock".into())),
                trace_socket_path: None,
                authorization_mode: ConfigurationWriterAuthorizationMode::Gating,
                guardian_agent_configuration: Some(ConfigurationWriterGuardianAgent {
                    agent_socket_path: ConfigurationWriterPath("/tmp/judge.sock".into()),
                    provider_name: Some(ConfigurationWriterProviderName("provider".into())),
                    model_name: Some(ConfigurationWriterModelName("model".into())),
                    timeout_milliseconds: ConfigurationWriterTimeoutMilliseconds(5000),
                    maximum_output_tokens: Some(ConfigurationWriterMaximumOutputTokens(42)),
                }),
                output_path: ConfigurationWriterPath("/tmp/config.rkyv".into()),
            });
        let text = request.datomize(vec![]).protosize().textualize();
        assert!(matches!(
            ConfigurationWriterInputSource::new(text).parse_request(),
            Ok(ConfigurationWriteRequest {
                guardian_agent_configuration: Some(_),
                ..
            })
        ));
    }
}

impl ConfigurationWriterCli {
    fn from_environment() -> Self {
        let arguments = env::args().skip(1).collect::<Vec<_>>();
        Self {
            text: match arguments.as_slice() {
                [text] => text.clone(),
                _ => String::new(),
            },
        }
    }

    fn run(&self) -> Result<(), ConfigurationWriterCliError> {
        if self.text.is_empty() {
            return Err(ConfigurationWriterCliError::ArgumentCount);
        }
        let request = ConfigurationWriterInputSource::new(self.text.clone()).parse_request()?;
        let output = request.write()?;
        println!("{}", output.datomize(vec![]).protosize().textualize());
        Ok(())
    }
}

struct ConfigurationWriterInputSource {
    text: String,
}

impl ConfigurationWriterInputSource {
    fn new(text: String) -> Self {
        Self { text }
    }

    fn parse_request(&self) -> Result<ConfigurationWriteRequest, ConfigurationWriterCliError> {
        let mut pending = Potential::<ConfigurationWriterInput>::from(self.text.clone());
        pending
            .actualize(&mut Budget {
                remaining: 1024,
                reader: ReaderBudget { remaining: 1024 },
                depth: 0,
                maximum_depth: 1024,
            })
            .map(ConfigurationWriterInput::into_request)
            .map_err(|error| ConfigurationWriterCliError::Datom(format!("{error:?}")))
    }
}

impl ConfigurationWriterInput {
    fn into_request(self) -> ConfigurationWriteRequest {
        match self {
            Self::ConfigurationWriteRequest(request) => request,
        }
    }
}

impl ConfigurationWriteRequest {
    fn write(self) -> Result<ConfigurationWriteOutput, ConfigurationWriterCliError> {
        let output_path = self.output_path.clone();
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&self.configuration()?)?;
        fs::write(output_path.as_path(), bytes).map_err(|source| {
            ConfigurationWriterCliError::WriteArchive {
                path: output_path.path_buf(),
                source,
            }
        })?;
        Ok(ConfigurationWriteOutput::ConfigurationWritten(output_path))
    }

    fn configuration(self) -> Result<SpiritNexusConfiguration, ConfigurationWriterCliError> {
        let guardian_agent_configuration = self
            .guardian_agent_configuration
            .map(ConfigurationWriterGuardianAgent::into_guardian_agent_configuration)
            .transpose()?;
        Ok(SpiritNexusConfiguration {
            socket_path: self.socket_path.into_configuration_path(),
            optional_meta_socket_path: self
                .meta_socket_path
                .map(ConfigurationWriterPath::into_configuration_path),
            optional_trace_socket_path: self
                .trace_socket_path
                .map(ConfigurationWriterPath::into_configuration_path),
            authorization_mode: self.authorization_mode.into_authorization_mode(),
            optional_spirit_guardian_agent_configuration: guardian_agent_configuration,
        })
    }
}

impl ConfigurationWriterPath {
    fn as_path(&self) -> &Path {
        Path::new(&self.0)
    }
    fn path_buf(&self) -> PathBuf {
        self.as_path().to_path_buf()
    }
    fn into_configuration_path(self) -> ConfigurationPath {
        self.0
    }
}

impl ConfigurationWriterAuthorizationMode {
    fn into_authorization_mode(self) -> AuthorizationMode {
        match self {
            Self::Gating => AuthorizationMode::Gating,
            Self::Observing => AuthorizationMode::Observing,
        }
    }
}

impl ConfigurationWriterGuardianAgent {
    fn into_guardian_agent_configuration(
        self,
    ) -> Result<SpiritGuardianAgentConfiguration, ConfigurationWriterCliError> {
        if self.timeout_milliseconds.0 < 0 {
            return Err(ConfigurationWriterCliError::NegativeGuardianTimeout(
                self.timeout_milliseconds.0,
            ));
        }
        if let Some(maximum_output_tokens) = &self.maximum_output_tokens
            && maximum_output_tokens.0 < 0
        {
            return Err(
                ConfigurationWriterCliError::NegativeGuardianMaximumOutputTokens(
                    maximum_output_tokens.0,
                ),
            );
        }
        Ok(SpiritGuardianAgentConfiguration {
            agent_socket_path: self.agent_socket_path.into_configuration_path(),
            optional_spirit_guardian_provider_name: self.provider_name.map(|name| name.0),
            optional_spirit_guardian_model_name: self.model_name.map(|name| name.0),
            spirit_guardian_timeout_milliseconds: self.timeout_milliseconds.0,
            optional_spirit_guardian_maximum_output_tokens: self
                .maximum_output_tokens
                .map(|tokens| tokens.0),
        })
    }
}

#[derive(Debug, Error)]
enum ConfigurationWriterCliError {
    #[error("spirit-write-configuration requires exactly one Datom request")]
    ArgumentCount,
    #[error("invalid Datom configuration request: {0}")]
    Datom(String),
    #[error(transparent)]
    Archive(#[from] rkyv::rancor::Error),
    #[error("guardian timeout must be non-negative, got {0}")]
    NegativeGuardianTimeout(i64),
    #[error("guardian maximum output tokens must be non-negative, got {0}")]
    NegativeGuardianMaximumOutputTokens(i64),
    #[error("write binary configuration archive {path}: {source}")]
    WriteArchive {
        path: PathBuf,
        source: std::io::Error,
    },
}
