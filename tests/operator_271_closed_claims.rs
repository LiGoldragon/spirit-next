//! Static policy witnesses for the authored operational contracts.
//!
//! Runtime semantics are proved by the socket, process, migration, and store
//! suites. These assertions deliberately cover only the retired-source boundary
//! left by the schema-rust removal.

const NEXUS_CONTRACT: &str = include_str!("../schema/nexus.ethos");
const SEMA_CONTRACT: &str = include_str!("../schema/sema.ethos");
const NEXUS_RUNTIME: &str = include_str!("../src/component_nexus.rs");
const SEMA_RUNTIME: &str = include_str!("../src/component_sema.rs");

#[test]
fn authored_operational_contracts_retain_current_operations() {
    for declaration in [
        "CommandSemaWrite.[Record.Entry BumpImportance.ImportanceBump ChangeRecord.RecordChange]",
        "GuardRecord.RecordRequest",
        "ResolveClarification.ClarificationResolution",
        "WriteInput.[Record.Entry BumpImportance.ImportanceBump ChangeRecord.RecordChange]",
        "ReadInput.[",
        "Observe.Query",
        "Intent.DomainScopes",
    ] {
        assert!(
            NEXUS_CONTRACT.contains(declaration) || SEMA_CONTRACT.contains(declaration),
            "missing current operational declaration {declaration}"
        );
    }
}

#[test]
fn operational_modules_are_authored_runtime_spines() {
    for source in [NEXUS_RUNTIME, SEMA_RUNTIME] {
        assert!(source.contains("Runtime operational spine"));
        for retired in [
            "schema-rust",
            "schema_language",
            "signal-frame",
            "NotaDecode",
            "NotaEncode",
        ] {
            assert!(
                !source.contains(retired),
                "operational runtime retains retired boundary {retired}"
            );
        }
    }
}

#[test]
fn generated_signal_public_root_is_current() {
    assert!(matches!(
        signal_spirit::Query::Version,
        signal_spirit::Query::Version
    ));
}
