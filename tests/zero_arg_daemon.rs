//! Isolated zero-argument daemon startup and stable-Sema restart witness.

use std::{
    path::Path,
    process::{Child, Command},
    thread,
    time::{Duration, Instant},
};

use signal_spirit::{Entry, Kind, Magnitude, Query, Response};
use spirit::{Configuration, SignalTransport, Store};
use tempfile::TempDir;

fn spirit_nexus_binary() -> std::path::PathBuf {
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target"));
    let binary = target.join("debug").join("spirit-nexus");
    assert!(
        binary.is_file(),
        "workspace executable {} is not built; run cargo build --workspace first",
        binary.display()
    );
    binary
}

fn wait_for_socket(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if path.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("daemon did not bind {}", path.display());
}

fn spawn(state: &Path) -> Child {
    Command::new(spirit_nexus_binary())
        .env("XDG_STATE_HOME", state)
        .spawn()
        .expect("spawn isolated zero-argument daemon")
}

#[test]
fn zero_argument_daemon_uses_persisted_socket_and_retains_existing_domain_rows() {
    let sandbox = TempDir::new().expect("isolated state root");
    let state_home = sandbox.path().join("state-home");
    let state = state_home.join("spirit");
    let socket = state.join("persisted.sock");
    let database = state.join("spirit.sema");
    let mut configuration = Configuration::default_nexus_configuration();
    configuration.socket_path = socket.to_string_lossy().into_owned();
    let store = Store::open_with_configuration(&database, configuration).expect("seed stable Sema");
    store
        .import_record(
            "persisted-row".into(),
            Entry {
                domains: vec![signal_domain::Domain::Information(
                    signal_domain::InformationDomain::Documentation,
                )],
                kind: Kind::Decision,
                description: "a record survives startup configuration recovery".into(),
                importance: Magnitude::Medium,
            },
        )
        .expect("seed domain row");
    drop(store);

    let mut daemon = spawn(&state_home);
    wait_for_socket(&socket);
    let reply = SignalTransport::connect(&socket)
        .expect("connect working socket")
        .exchange(&Query::Lookup("persisted-row".into()))
        .expect("typed exchange");
    assert!(matches!(reply, Response::RecordFound(found)
        if found.record_identifier == "persisted-row" && found.entry.description == "a record survives startup configuration recovery"));
    daemon.kill().expect("stop isolated daemon");
    daemon.wait().expect("reap isolated daemon");
}
