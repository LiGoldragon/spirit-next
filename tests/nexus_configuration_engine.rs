//! Ordinary and meta configuration behavior through the Spirit engine boundary.

use meta_signal_spirit::{ArchiveDatabaseTarget, ConfigureRequest, Response as MetaResponse};
use signal_spirit::{ConfigurationRejectionReason, Query, Response};
use spirit::{Configuration, Engine, Store};
use tempfile::TempDir;

fn desired(socket: &str) -> signal_spirit::SpiritNexusConfiguration {
    let mut configuration = Configuration::default_nexus_configuration();
    configuration.socket_path = socket.into();
    configuration
}

fn invalid_desired(socket: &str) -> signal_spirit::SpiritNexusConfiguration {
    let mut configuration = desired(socket);
    configuration.optional_spirit_guardian_agent_configuration =
        Some(signal_spirit::SpiritGuardianAgentConfiguration {
            agent_socket_path: String::from("guardian.sock"),
            optional_spirit_guardian_provider_name: None,
            optional_spirit_guardian_model_name: None,
            spirit_guardian_timeout_milliseconds: -1,
            optional_spirit_guardian_maximum_output_tokens: Some(-2),
        });
    configuration
}

#[test]
fn ordinary_meta_and_reverse_configuration_transitions_preserve_state_and_refusal() {
    let sandbox = TempDir::new().expect("isolated Sema");
    let mut engine = Engine::new(
        Store::open_with_configuration(sandbox.path().join("spirit.sema"), desired("default.sock"))
            .expect("open isolated store"),
    );
    let ordinary = engine.handle(Query::Configure(desired("ordinary.sock")));
    assert!(
        matches!(ordinary.root(), Response::ConfigurationAccepted(receipt)
        if receipt.spirit_nexus_configuration.socket_path == "ordinary.sock" && !receipt.meta_configure_done)
    );
    let meta = engine.configure(ConfigureRequest {
        spirit_nexus_configuration: desired("meta.sock"),
        archive_database_target: ArchiveDatabaseTarget::Default,
        selected_mirror_target: None,
        selected_criome_gate_target: None,
        selected_guardian_prompt_target: None,
    });
    assert!(matches!(meta, MetaResponse::Configured(receipt)
        if receipt.spirit_nexus_configuration.socket_path == "meta.sock" && receipt.meta_configure_done));
    let refused = engine.handle(Query::Configure(desired("must-not-write.sock")));
    assert!(
        matches!(refused.root(), Response::ConfigurationRefused(rejection)
        if rejection.configuration_rejection_reason == ConfigurationRejectionReason::MetaConfigureOccurred)
    );
    let state = engine
        .store()
        .nexus_configuration_state()
        .expect("persisted state");
    assert!(state.meta_configure_occurred);
    assert_eq!(state.desired_configuration.socket_path, "meta.sock");
    let reopened = engine.reverse_meta_configuration();
    assert!(
        matches!(reopened, MetaResponse::OrdinaryConfigurationReopened(receipt)
        if !receipt.meta_configure_done && receipt.spirit_nexus_configuration.socket_path == "meta.sock")
    );
    assert!(
        matches!(engine.handle(Query::Configure(desired("ordinary-after-reverse.sock"))).root(),
        Response::ConfigurationAccepted(receipt) if receipt.spirit_nexus_configuration.socket_path == "ordinary-after-reverse.sock")
    );
}

#[test]
fn invalid_ordinary_and_meta_configure_leave_persisted_lifecycle_unchanged() {
    let sandbox = TempDir::new().expect("isolated Sema");
    let mut engine = Engine::new(
        Store::open_with_configuration(sandbox.path().join("spirit.sema"), desired("default.sock"))
            .expect("open isolated store"),
    );
    let ordinary = engine.handle(Query::Configure(invalid_desired("invalid-ordinary.sock")));
    assert!(
        matches!(ordinary.root(), Response::ConfigurationRefused(rejection)
        if rejection.configuration_rejection_reason == ConfigurationRejectionReason::InvalidConfiguration)
    );
    let before_meta = engine
        .store()
        .nexus_configuration_state()
        .expect("state after ordinary rejection");
    assert_eq!(
        before_meta.desired_configuration.socket_path,
        "default.sock"
    );
    assert!(!before_meta.meta_configure_occurred);

    let meta = engine.configure(ConfigureRequest {
        spirit_nexus_configuration: invalid_desired("invalid-meta.sock"),
        archive_database_target: ArchiveDatabaseTarget::Default,
        selected_mirror_target: None,
        selected_criome_gate_target: None,
        selected_guardian_prompt_target: None,
    });
    assert!(matches!(meta, MetaResponse::Rejected(_)));
    let after_meta = engine
        .store()
        .nexus_configuration_state()
        .expect("state after meta rejection");
    assert_eq!(
        after_meta, before_meta,
        "rejected Configure changes neither desired state nor marker"
    );
}
