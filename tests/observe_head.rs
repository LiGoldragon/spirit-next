//! Owner-only `ObserveHead` reports the actual versioned-log content head.

use meta_signal_spirit::{ImportReceipt, ImportRequest, ImportedRecord, Response as MetaResponse};
use signal_domain::{Domain, ProgrammingLeaf, SoftwareDomain, TechnologyDomain};
use signal_spirit::{Entry, Kind, Magnitude};
use spirit::{Engine, Store};
use tempfile::TempDir;

const SYNTHETIC_STAND_IN: &str = "cdc1c22fea273efbade8385bfa0e5c73899bb66632b96949e0952fc77891b718";

fn open_engine() -> (TempDir, Engine) {
    let directory = tempfile::tempdir().expect("create sandbox");
    let store = Store::open(directory.path().join("spirit.sema")).expect("open store");
    (directory, Engine::new(store))
}

fn seed_witness_record(engine: &mut Engine) {
    let receipt = engine.import(ImportRequest {
        imported_records: vec![ImportedRecord {
            record_identifier: "witness-record-1".into(),
            entry: Entry {
                domains: vec![Domain::Technology(TechnologyDomain::Software(
                    SoftwareDomain::Programming(ProgrammingLeaf::CodeGeneration),
                ))],
                kind: Kind::Decision,
                description: "criome auth witness record".into(),
                importance: Magnitude::Low,
            },
        }],
    });
    assert!(matches!(
        receipt,
        MetaResponse::Imported(ImportReceipt {
            record_count: 1,
            ..
        })
    ));
}

fn observed_head_hex(engine: &Engine) -> Option<String> {
    let MetaResponse::HeadObserved(observed) = engine.observe_head() else {
        panic!("ObserveHead must reply HeadObserved");
    };
    observed.selected_head_digest
}

#[test]
fn empty_store_reports_no_head() {
    let (_directory, engine) = open_engine();
    assert_eq!(observed_head_hex(&engine), None);
}

#[test]
fn observe_head_returns_the_real_stored_content_head() {
    let (_directory, mut engine) = open_engine();
    seed_witness_record(&mut engine);
    let observed = observed_head_hex(&engine).expect("a seeded store reports its head");
    let stored_head = engine
        .store()
        .versioned_log_head()
        .expect("read versioned-log head")
        .expect("seeded store has a head");
    assert_eq!(observed, stored_head.to_string());
    assert_eq!(observed.len(), 64);
    assert!(
        observed
            .chars()
            .all(|character| character.is_ascii_digit() || ('a'..='f').contains(&character))
    );
    assert_ne!(observed, SYNTHETIC_STAND_IN);
}

#[test]
fn the_head_is_content_deterministic() {
    let (_first_directory, mut first) = open_engine();
    seed_witness_record(&mut first);
    let (_second_directory, mut second) = open_engine();
    seed_witness_record(&mut second);
    assert_eq!(observed_head_hex(&first), observed_head_hex(&second));
}
