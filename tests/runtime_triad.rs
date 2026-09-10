mod support;

use signal_domain::{
    DataLeaf, Domain, DomainScope, DomainScopes, SoftwareDomain, TechnologyDomain,
};
use spirit::{
    Engine, Store,
    schema::{
        nexus::NexusWork,
        sema::{ReadInput as SemaReadInput, ReadOutput as SemaReadOutput},
        signal::{
            DomainMatch, Entry, ImportanceBump, ImportanceSelection, Justification, KeywordMatch,
            Kind, Magnitude, Query, RecordIdentifier, RecordRequest, Response, Selection,
            TextMatch, VerbatimQuote,
        },
    },
};
use support::domain_fixtures;
use tempfile::TempDir;

struct Fixture {
    _directory: TempDir,
    path: std::path::PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let directory = TempDir::new().expect("tempdir");
        let path = directory.path().join("runtime-triad.sema");
        Self {
            _directory: directory,
            path,
        }
    }
    fn engine(&self) -> Engine {
        Engine::new(Store::open(&self.path).expect("open store"))
    }
    fn store(&self) -> Store {
        Store::open(&self.path).expect("open store")
    }
}

fn entry(description: &str, importance: Magnitude) -> Entry {
    Entry {
        domains: domain_fixtures::domains(&["runtime-triad"]),
        kind: Kind::Decision,
        description: description.into(),
        importance,
    }
}
fn data_entry(domain: DataLeaf, description: &str, importance: Magnitude) -> Entry {
    Entry {
        domains: vec![Domain::Technology(TechnologyDomain::Software(
            SoftwareDomain::Data(domain),
        ))],
        ..entry(description, importance)
    }
}
fn justification(statement: &str) -> Justification {
    Justification {
        testimony: vec![VerbatimQuote {
            quote_text: statement.into(),
            optional_antecedent: None,
        }],
        reasoning: statement.into(),
    }
}
fn record_query(entry: Entry) -> Query {
    let statement = entry.description.clone();
    Query::Record(RecordRequest {
        entry,
        justification: justification(&statement),
    })
}
fn selection(importance_selection: ImportanceSelection) -> Selection {
    Selection {
        domain_match: DomainMatch::Any,
        keyword_match: KeywordMatch::Any,
        text_match: TextMatch::Any,
        selected_kind: Some(Kind::Decision),
        importance_selection,
    }
}
fn accepted_identifier(response: Response) -> RecordIdentifier {
    match response {
        Response::RecordAccepted(identifier) => identifier,
        other => panic!("expected RecordAccepted, got {other:?}"),
    }
}
fn observed_descriptions(response: Response) -> Vec<String> {
    let Response::RecordsObserved(records) = response else {
        panic!("expected RecordsObserved, got {response:?}")
    };
    records
        .record_set
        .into_iter()
        .map(|record| record.entry.description)
        .collect()
}

#[test]
fn generated_roots_implement_the_shared_runtime_roles() {
    fn nexus_work<Work: triad_runtime::NexusWork>() {}
    fn sema_read_input<Input: triad_runtime::SemaReadInput>() {}
    fn sema_read_output<Output: triad_runtime::SemaReadOutput>() {}
    nexus_work::<NexusWork>();
    sema_read_input::<SemaReadInput>();
    sema_read_output::<SemaReadOutput>();
}

#[test]
fn record_observe_lookup_and_count_cross_the_full_runtime() {
    let fixture = Fixture::new();
    let mut engine = fixture.engine();
    let first = accepted_identifier(
        engine
            .handle(record_query(entry("first current record", Magnitude::Low)))
            .into_root(),
    );
    accepted_identifier(
        engine
            .handle(record_query(entry(
                "second current record",
                Magnitude::High,
            )))
            .into_root(),
    );
    let observed = engine.handle(Query::Observe(selection(ImportanceSelection::Any)));
    let stash = match observed.root() {
        Response::RecordsStashed(stashed) => {
            assert_eq!(stashed.record_count, 2);
            stashed.stash_handle
        }
        other => panic!("expected RecordsStashed, got {other:?}"),
    };
    assert_eq!(
        observed_descriptions(engine.handle(Query::LookupStash(stash)).into_root()),
        vec!["second current record", "first current record"]
    );
    match engine.handle(Query::Lookup(first.clone())).into_root() {
        Response::RecordFound(record) => {
            assert_eq!(record.record_identifier, first);
            assert_eq!(record.entry.description, "first current record");
        }
        other => panic!("expected RecordFound, got {other:?}"),
    }
    match engine
        .handle(Query::Count(selection(ImportanceSelection::Any)))
        .into_root()
    {
        Response::RecordsCounted(count) => assert_eq!(count.record_count, 2),
        other => panic!("expected RecordsCounted, got {other:?}"),
    }
}

#[test]
fn text_search_and_intent_read_every_current_record_in_scope() {
    let fixture = Fixture::new();
    let mut engine = fixture.engine();
    for record in [
        data_entry(
            DataLeaf::All,
            "routing protocol architecture",
            Magnitude::Maximum,
        ),
        data_entry(
            DataLeaf::Persistence,
            "routing fallback note",
            Magnitude::Medium,
        ),
        data_entry(
            DataLeaf::SchemaEvolution,
            "schema evolution note",
            Magnitude::Low,
        ),
    ] {
        accepted_identifier(engine.handle(record_query(record)).into_root());
    }
    assert_eq!(
        observed_descriptions(
            engine
                .handle(Query::TextSearch("routing protocol".into()))
                .into_root()
        ),
        vec!["routing protocol architecture", "routing fallback note"]
    );
    let scopes: DomainScopes = vec![
        DomainScope {
            domain: Domain::Technology(TechnologyDomain::Software(SoftwareDomain::Data(
                DataLeaf::Persistence,
            ))),
        },
        DomainScope {
            domain: Domain::Technology(TechnologyDomain::Software(SoftwareDomain::Data(
                DataLeaf::SchemaEvolution,
            ))),
        },
    ];
    assert_eq!(
        observed_descriptions(engine.handle(Query::Intent(scopes)).into_root()),
        vec![
            "routing protocol architecture",
            "routing fallback note",
            "schema evolution note"
        ]
    );
}

#[test]
fn importance_filtering_and_bump_are_the_only_magnitude_state_path() {
    let fixture = Fixture::new();
    let store = fixture.store();
    let low = store
        .record_entry(entry("bump target", Magnitude::Low))
        .expect("record low entry")
        .record_identifier;
    store
        .record_entry(entry("already high", Magnitude::High))
        .expect("record high entry");
    drop(store);
    let mut engine = fixture.engine();
    let high_only = selection(ImportanceSelection::AtLeastImportance(Magnitude::High));
    let Response::RecordsStashed(stashed) = engine.handle(Query::Observe(high_only)).into_root()
    else {
        panic!("expected stashed observation")
    };
    assert_eq!(stashed.observed_records.record_set.len(), 1);
    assert_eq!(
        stashed.observed_records.record_set[0].entry.description,
        "already high"
    );
    assert!(matches!(
        engine
            .handle(Query::BumpImportance(ImportanceBump {
                record_identifier: low.clone()
            }))
            .into_root(),
        Response::ImportanceBumped(_)
    ));
    match engine.handle(Query::Lookup(low)).into_root() {
        Response::RecordFound(record) => assert_eq!(record.entry.importance, Magnitude::Medium),
        other => panic!("expected RecordFound, got {other:?}"),
    }
}
