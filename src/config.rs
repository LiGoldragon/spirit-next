//! Daemon-side runtime wrapper for the contract-owned configuration.
//!
//! The archived daemon configuration type lives in `signal-spirit`; this
//! wrapper holds the imported wire object plus daemon-local `PathBuf` views and
//! implements the `triad_runtime::BindingSurface` trait the emitted daemon spine
//! reads to bind listeners and open the store. No NOTA is linked here.

use std::{
    fs,
    path::{Path, PathBuf},
};

use signal_spirit::{AuthorizationMode, SpiritNexusConfiguration};
use thiserror::Error;
use triad_runtime::BindingSurface;

#[derive(Clone, Debug, PartialEq)]
pub struct Configuration {
    raw: SpiritNexusConfiguration,
    socket_path: PathBuf,
    meta_socket_path: Option<PathBuf>,
    database_path: PathBuf,
    trace_socket_path: Option<PathBuf>,
    authorization_mode: AuthorizationMode,
}

impl Configuration {
    pub fn new(socket_path: impl AsRef<Path>, database_path: impl AsRef<Path>) -> Self {
        Self::from_raw_at_database(
            SpiritNexusConfiguration {
                socket_path: socket_path.as_ref().to_string_lossy().into_owned(),
                optional_meta_socket_path: None,
                optional_trace_socket_path: None,
                authorization_mode: AuthorizationMode::Gating,
                optional_spirit_guardian_agent_configuration: None,
            },
            database_path.as_ref().to_path_buf(),
        )
    }

    pub fn new_with_trace(
        socket_path: impl AsRef<Path>,
        database_path: impl AsRef<Path>,
        trace_socket_path: impl AsRef<Path>,
    ) -> Self {
        Self::new(socket_path, database_path).with_trace_socket_path(trace_socket_path)
    }

    pub fn with_meta_socket_path(self, meta_socket_path: impl AsRef<Path>) -> Self {
        let mut raw = self.raw;
        raw.optional_meta_socket_path =
            Some(meta_socket_path.as_ref().to_string_lossy().into_owned());
        Self::from_raw_at_database(raw, self.database_path.clone())
    }

    pub fn with_trace_socket_path(self, trace_socket_path: impl AsRef<Path>) -> Self {
        let mut raw = self.raw;
        raw.optional_trace_socket_path =
            Some(trace_socket_path.as_ref().to_string_lossy().into_owned());
        Self::from_raw_at_database(raw, self.database_path.clone())
    }

    pub fn with_authorization_mode(self, authorization_mode: AuthorizationMode) -> Self {
        let mut raw = self.raw;
        raw.authorization_mode = authorization_mode;
        Self::from_raw_at_database(raw, self.database_path.clone())
    }

    /// Construct a runtime snapshot with an explicit stable Sema location.
    /// The location is executable-owned and never travels in `SpiritNexusConfiguration`.
    pub fn from_raw_at_database(
        raw: SpiritNexusConfiguration,
        database_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            socket_path: PathBuf::from(&raw.socket_path),
            meta_socket_path: raw.optional_meta_socket_path.as_ref().map(PathBuf::from),
            database_path: database_path.into(),
            trace_socket_path: raw.optional_trace_socket_path.as_ref().map(PathBuf::from),
            authorization_mode: raw.authorization_mode.clone(),
            raw,
        }
    }

    pub(crate) fn checked_from_raw_at_database(
        raw: SpiritNexusConfiguration,
        database_path: impl Into<PathBuf>,
    ) -> Result<Self, ConfigurationError> {
        Self::validate_nexus_configuration(&raw)?;
        Ok(Self::from_raw_at_database(raw, database_path))
    }

    /// The established zero-argument Spirit Sema location. This location is
    /// deliberately outside mutable configuration so Configure cannot point a
    /// running component at a fresh empty database.
    pub fn stable_database_path() -> PathBuf {
        Self::stable_state_directory().join("spirit.sema")
    }

    /// Resolve the state directory without touching it. Production follows
    /// XDG state-home resolution and defaults to `$HOME/.local/state/spirit`.
    /// Tests isolate this established base-directory mechanism with
    /// `XDG_STATE_HOME`; the executable has no component-specific state-path
    /// override.
    pub(crate) fn stable_state_directory() -> PathBuf {
        let state_home = std::env::var_os("XDG_STATE_HOME")
            .map(PathBuf::from)
            .or_else(|| {
                std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".local/state"))
            })
            .unwrap_or_else(|| PathBuf::from(".local/state"));
        state_home.join("spirit")
    }

    /// The executable-owned default persisted into a fresh Sema. The database
    /// location is deliberately absent: it is discovered separately above.
    pub fn default_nexus_configuration() -> SpiritNexusConfiguration {
        let spirit_state = Self::stable_state_directory();
        SpiritNexusConfiguration {
            socket_path: spirit_state
                .join("spirit.sock")
                .to_string_lossy()
                .into_owned(),
            optional_meta_socket_path: Some(
                spirit_state
                    .join("meta-spirit.sock")
                    .to_string_lossy()
                    .into_owned(),
            ),
            optional_trace_socket_path: None,
            authorization_mode: AuthorizationMode::Gating,
            optional_spirit_guardian_agent_configuration: None,
        }
    }

    pub fn from_raw(raw: SpiritNexusConfiguration) -> Self {
        Self::from_raw_at_database(raw, Self::stable_database_path())
    }

    pub fn raw(&self) -> &SpiritNexusConfiguration {
        &self.raw
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    pub fn meta_socket_path(&self) -> Option<&Path> {
        self.meta_socket_path.as_deref()
    }

    pub fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub fn trace_socket_path(&self) -> Option<&Path> {
        self.trace_socket_path.as_deref()
    }

    pub fn authorization_mode(&self) -> AuthorizationMode {
        self.authorization_mode.clone()
    }

    #[cfg(feature = "agent-guardian")]
    pub fn guardian_agent_configuration(
        &self,
    ) -> Option<crate::guardian::AgentGuardianConfiguration> {
        let configuration = self
            .raw
            .optional_spirit_guardian_agent_configuration
            .as_ref()?;
        Some(crate::guardian::AgentGuardianConfiguration::new(
            configuration.agent_socket_path.clone(),
            configuration.optional_spirit_guardian_provider_name.clone(),
            configuration.optional_spirit_guardian_model_name.clone(),
            std::time::Duration::from_millis(
                u64::try_from(configuration.spirit_guardian_timeout_milliseconds)
                    .expect("validated guardian timeout"),
            ),
            configuration
                .optional_spirit_guardian_maximum_output_tokens
                .map(|value| u64::try_from(value).expect("validated guardian output budget")),
        ))
    }

    pub(crate) fn validate_nexus_configuration(
        raw: &SpiritNexusConfiguration,
    ) -> Result<(), ConfigurationError> {
        let Some(guardian) = raw.optional_spirit_guardian_agent_configuration.as_ref() else {
            return Ok(());
        };
        if guardian.spirit_guardian_timeout_milliseconds < 0 {
            return Err(ConfigurationError::NegativeGuardianTimeout(
                guardian.spirit_guardian_timeout_milliseconds,
            ));
        }
        if let Some(maximum_output_tokens) = guardian.optional_spirit_guardian_maximum_output_tokens
            && maximum_output_tokens < 0
        {
            return Err(ConfigurationError::NegativeGuardianMaximumOutputTokens(
                maximum_output_tokens,
            ));
        }
        Ok(())
    }

    pub fn from_binary_path(path: impl AsRef<Path>) -> Result<Self, ConfigurationError> {
        let bytes = fs::read(path).map_err(ConfigurationError::Read)?;
        Self::from_binary_bytes(&bytes)
    }

    pub fn from_binary_bytes(bytes: &[u8]) -> Result<Self, ConfigurationError> {
        let raw = rkyv::from_bytes::<SpiritNexusConfiguration, rkyv::rancor::Error>(bytes)
            .map_err(|_| ConfigurationError::ArchiveDecode)?;
        Self::validate_nexus_configuration(&raw)?;
        Ok(Self::from_raw(raw))
    }

    pub fn to_binary_bytes(&self) -> Result<Vec<u8>, ConfigurationError> {
        Self::validate_nexus_configuration(&self.raw)?;
        rkyv::to_bytes::<rkyv::rancor::Error>(&self.raw)
            .map(|bytes| bytes.to_vec())
            .map_err(|_| ConfigurationError::ArchiveEncode)
    }

    pub fn write_binary_file(&self, path: impl AsRef<Path>) -> Result<(), ConfigurationError> {
        fs::write(path, self.to_binary_bytes()?).map_err(ConfigurationError::Write)
    }
}

impl BindingSurface for Configuration {
    fn socket_path(&self) -> &Path {
        Configuration::socket_path(self)
    }

    fn meta_socket_path(&self) -> Option<&Path> {
        Configuration::meta_socket_path(self)
    }

    fn database_path(&self) -> &Path {
        Configuration::database_path(self)
    }

    fn trace_socket_path(&self) -> Option<&Path> {
        Configuration::trace_socket_path(self)
    }
}

#[derive(Debug, Error)]
pub enum ConfigurationError {
    #[error("failed to read binary configuration: {0}")]
    Read(std::io::Error),

    #[error("failed to write binary configuration: {0}")]
    Write(std::io::Error),

    #[error("failed to encode binary configuration")]
    ArchiveEncode,

    #[error("failed to decode binary configuration")]
    ArchiveDecode,

    #[error("guardian timeout must be non-negative, got {0}")]
    NegativeGuardianTimeout(i64),

    #[error("guardian maximum output tokens must be non-negative, got {0}")]
    NegativeGuardianMaximumOutputTokens(i64),
}

#[cfg(all(test, feature = "agent-guardian"))]
mod tests {
    use super::*;

    fn raw_with_guardian(
        timeout: i64,
        maximum_output_tokens: Option<i64>,
    ) -> SpiritNexusConfiguration {
        let mut raw = Configuration::new("/tmp/spirit.sock", "/tmp/spirit.sema")
            .raw()
            .clone();
        raw.optional_spirit_guardian_agent_configuration =
            Some(signal_spirit::SpiritGuardianAgentConfiguration {
                agent_socket_path: "/tmp/judge.sock".into(),
                optional_spirit_guardian_provider_name: None,
                optional_spirit_guardian_model_name: None,
                spirit_guardian_timeout_milliseconds: timeout,
                optional_spirit_guardian_maximum_output_tokens: maximum_output_tokens,
            });
        raw
    }

    #[test]
    fn rejects_negative_guardian_numbers_before_archiving_or_runtime_use() {
        let configuration = Configuration::from_raw(raw_with_guardian(-1, Some(-2)));
        assert!(matches!(
            configuration.to_binary_bytes(),
            Err(ConfigurationError::NegativeGuardianTimeout(-1))
        ));
    }

    #[test]
    fn rejects_negative_guardian_numbers_from_an_archived_configuration() {
        let raw = raw_with_guardian(1, Some(-2));
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&raw)
            .expect("encode deliberately malformed archive");
        assert!(matches!(
            Configuration::from_binary_bytes(&bytes),
            Err(ConfigurationError::NegativeGuardianMaximumOutputTokens(-2))
        ));
    }
}
