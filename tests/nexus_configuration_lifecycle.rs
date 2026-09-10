//! Persisted universal lifecycle semantics at Spirit's Sema boundary.

use nexus::Configurable as _;
use signal_spirit::SpiritNexusConfiguration;
use spirit::{Configuration, Store};
use tempfile::TempDir;

fn desired(socket: &str) -> SpiritNexusConfiguration {
    let mut configuration = Configuration::default_nexus_configuration();
    configuration.socket_path = socket.into();
    configuration
}

#[test]
fn ordinary_meta_reverse_and_restart_preserve_the_truthful_marker_and_desired_value() {
    let directory = TempDir::new().expect("isolated state directory");
    let path = directory.path().join("spirit.sema");
    let store = Store::open_with_configuration(&path, desired("ordinary-default.sock"))
        .expect("fresh store");
    let mut state = store
        .nexus_configuration_state()
        .expect("fresh lifecycle state");
    state
        .ordinary_configure_if_unset(desired("ordinary-one.sock"))
        .expect("ordinary Configure remains open before meta Configure");
    store
        .replace_nexus_configuration(state)
        .expect("persist ordinary desired value");
    let mut state = store
        .nexus_configuration_state()
        .expect("read ordinary state");
    state
        .ordinary_configure_if_unset(desired("ordinary-two.sock"))
        .expect("ordinary Configure is not single-use");
    store
        .replace_nexus_configuration(state)
        .expect("persist second ordinary desired value");
    let mut state = store
        .nexus_configuration_state()
        .expect("read state for meta Configure");
    state.meta_configure(desired("meta.sock"));
    store
        .replace_nexus_configuration(state)
        .expect("persist meta Configure");
    drop(store);

    let reopened = Store::open_with_configuration(&path, desired("must-not-reseed.sock"))
        .expect("reopen stable store");
    let mut state = reopened
        .nexus_configuration_state()
        .expect("reopened state");
    assert!(state.meta_configure_occurred());
    assert_eq!(state.desired_configuration().socket_path, "meta.sock");
    assert!(
        state
            .ordinary_configure_if_unset(desired("refused.sock"))
            .is_err()
    );
    assert_eq!(
        state.desired_configuration().socket_path,
        "meta.sock",
        "rejection leaves desired configuration unchanged"
    );
    state.meta_reverse();
    assert!(!state.meta_configure_occurred());
    state
        .ordinary_configure_if_unset(desired("reopened.sock"))
        .expect("only meta reversal reopens ordinary Configure");
    reopened
        .replace_nexus_configuration(state)
        .expect("persist reopened state");
    drop(reopened);

    let restarted = Store::open_with_configuration(&path, desired("must-not-reseed-again.sock"))
        .expect("restart with same Sema");
    let state = restarted
        .nexus_configuration_state()
        .expect("state after restart");
    assert!(!state.meta_configure_occurred());
    assert_eq!(state.desired_configuration().socket_path, "reopened.sock");
}
