//! Zero-argument daemon command contract.

use spirit::{Configuration, DaemonCommand, DaemonError, SpiritDaemon};
use triad_runtime::ArgumentError;

#[test]
fn daemon_command_rejects_any_startup_operand() {
    let one = DaemonCommand::<SpiritDaemon>::from_arguments(["configuration.rkyv"]);
    let extra = DaemonCommand::<SpiritDaemon>::from_arguments(["one", "two"]);
    assert!(matches!(
        one.configuration(),
        Err(DaemonError::Argument(ArgumentError::ArgumentCount {
            count: 1
        }))
    ));
    assert!(matches!(
        extra.configuration(),
        Err(DaemonError::Argument(ArgumentError::ArgumentCount {
            count: 2
        }))
    ));
}

#[test]
fn explicit_configuration_wrapper_does_not_create_a_database() {
    let sandbox = tempfile::tempdir().expect("temporary state directory");
    // This wrapper is pure: it does not alter XDG/HOME resolution or open a
    // daemon, so it cannot touch the established live Sema location.
    let database = sandbox.path().join("spirit.sema");
    let configuration = Configuration::from_raw_at_database(
        Configuration::default_nexus_configuration(),
        &database,
    );
    assert_eq!(configuration.database_path(), database.as_path());
    assert!(!database.exists());
}
