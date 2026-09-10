//! Ported `Tap` / `Untap` observer surface, library level.
//!
//! Old spirit's `OperationKind` carried `Tap` and `Untap`: a meta-observation
//! stream that filtered `OperationReceived` events by an `ObserverFilter`
//! (`All` / `OperationsOnly` / `EffectsOnly`). The new spirit had dropped both.
//! This ports them as a request/reply observer surface: `Tap(ObserverFilter)`
//! mints an observer subscription at the current operation revision and
//! `Untap(token)` returns its filtered observations since that revision. The
//! registry retains operations only while an active tap can consume them.
//! `Watch`/`Unwatch` reconciliation: `SubscribeIntent` already covers old
//! `Watch` (records subscription), so the un-covered half — token-based
//! cancellation — is what `Untap` restores.

mod support;

use spirit::schema::signal::{
    DomainMatch, Entry, ImportanceSelection, Justification, Kind, Magnitude, ObserverFilter,
    OperationKind, Query, RecordRequest, Response, Selection, VerbatimQuote,
};
use spirit::{Engine, Store};
use support::domain_fixtures;
use tempfile::TempDir;

fn entry(description: &str) -> Entry {
    Entry {
        domains: domain_fixtures::domains(&["observer-tap"]),
        kind: Kind::Decision,
        description: description.into(),
        importance: Magnitude::Minimum,
    }
}

fn record_request(description: &str) -> RecordRequest {
    RecordRequest {
        entry: entry(description),
        justification: Justification {
            testimony: vec![VerbatimQuote {
                quote_text: description.into(),
                optional_antecedent: None,
            }],
            reasoning: description.into(),
        },
    }
}

fn observe_query() -> Selection {
    Selection {
        domain_match: DomainMatch::Full(domain_fixtures::scopes(&["observer-tap"])),
        keyword_match: spirit::schema::signal::KeywordMatch::Any,
        text_match: spirit::schema::signal::TextMatch::Any,
        selected_kind: Some(Kind::Decision),
        importance_selection: ImportanceSelection::Any,
    }
}

fn engine() -> (TempDir, Engine) {
    let temp = TempDir::new().expect("tempdir");
    let database = temp.path().join("observer-tap.sema");
    let mut engine = Engine::new(Store::open(&database).expect("open store"));
    engine.start().expect("engine start");
    (temp, engine)
}

#[test]
fn tap_starts_at_the_current_revision_without_replaying_inactive_history() {
    let (_temp, mut engine) = engine();

    // No tap exists, so this daemon-lifetime traffic has no consumer and must
    // not become replay history for a later tap.
    let _ = engine
        .handle(Query::Record(record_request("first intent")))
        .into_root();
    let _ = engine
        .handle(Query::Record(record_request("second intent")))
        .into_root();
    let _ = engine.handle(Query::Observe(observe_query())).into_root();
    assert_eq!(
        engine.observer_tap_table().retained_operation_count(),
        0,
        "operations without a tap retain no observer history"
    );

    let reply = engine.handle(Query::Tap(ObserverFilter::All)).into_root();
    let Response::ObservationTapped(subscription) = reply else {
        panic!("expected ObservationTapped, got {reply:?}")
    };
    assert!(
        subscription.subscription_token >= 1,
        "the tap minted a subscription token"
    );
    assert_eq!(
        subscription.observer_filter,
        ObserverFilter::All,
        "the reply echoes the requested observer filter"
    );
    assert!(
        subscription.observed_operations.is_empty(),
        "a new tap starts from its opening revision rather than replaying history"
    );

    let _ = engine
        .handle(Query::Record(record_request("observed after tap")))
        .into_root();
    let untapped = engine
        .handle(Query::Untap(subscription.subscription_token))
        .into_root();
    let Response::ObservationUntapped(retraction) = untapped else {
        panic!("expected ObservationUntapped, got {untapped:?}")
    };
    let kinds: Vec<OperationKind> = retraction
        .observed_operations
        .iter()
        .map(|operation| operation.operation_kind.clone())
        .collect();
    assert_eq!(
        kinds,
        vec![OperationKind::Record, OperationKind::Untap],
        "the tap returns only operations from its active lifetime"
    );
    assert_eq!(
        engine.observer_tap_table().retained_operation_count(),
        0,
        "closing the final tap clears its consumed operation history"
    );
}

#[test]
fn observer_retention_is_bounded_by_active_tap_lag() {
    let (_temp, mut engine) = engine();

    for _ in 0..1024 {
        let _ = engine.handle(Query::Version).into_root();
    }
    assert_eq!(
        engine.observer_tap_table().retained_operation_count(),
        0,
        "high-volume traffic without taps retains no operation history"
    );

    let first = engine.handle(Query::Tap(ObserverFilter::All)).into_root();
    let Response::ObservationTapped(first) = first else {
        panic!("expected first ObservationTapped, got {first:?}")
    };
    for _ in 0..1024 {
        let _ = engine.handle(Query::Version).into_root();
    }
    assert_eq!(
        engine.observer_tap_table().retained_operation_count(),
        1024,
        "the first tap retains exactly its outstanding operation lag"
    );

    let second = engine.handle(Query::Tap(ObserverFilter::All)).into_root();
    let Response::ObservationTapped(second) = second else {
        panic!("expected second ObservationTapped, got {second:?}")
    };
    for _ in 0..128 {
        let _ = engine.handle(Query::Version).into_root();
    }

    let first_retraction = engine
        .handle(Query::Untap(first.subscription_token))
        .into_root();
    let Response::ObservationUntapped(first_retraction) = first_retraction else {
        panic!("expected first ObservationUntapped, got {first_retraction:?}")
    };
    assert_eq!(
        first_retraction.observed_operations.len(),
        1154,
        "the lagging tap receives every operation from its opening revision"
    );
    assert_eq!(
        engine.observer_tap_table().retained_operation_count(),
        129,
        "closing the lagging tap reclaims its prefix while preserving the later tap's lag"
    );

    let second_retraction = engine
        .handle(Query::Untap(second.subscription_token))
        .into_root();
    let Response::ObservationUntapped(second_retraction) = second_retraction else {
        panic!("expected second ObservationUntapped, got {second_retraction:?}")
    };
    assert_eq!(
        second_retraction.observed_operations.len(),
        130,
        "the later tap receives only its own outstanding lag before final reclamation"
    );
    assert_eq!(
        engine.observer_tap_table().retained_operation_count(),
        0,
        "closing every tap clears the operation log regardless of prior traffic volume"
    );
}

#[test]
fn untap_retires_the_subscription_and_returns_its_observations() {
    let (_temp, mut engine) = engine();

    let _ = engine
        .handle(Query::Record(record_request("intent")))
        .into_root();
    let tapped = engine
        .handle(Query::Tap(ObserverFilter::OperationsOnly))
        .into_root();
    let Response::ObservationTapped(subscription) = tapped else {
        panic!("expected ObservationTapped, got {tapped:?}")
    };
    let token = subscription.subscription_token;

    let untapped = engine.handle(Query::Untap(token)).into_root();
    let Response::ObservationUntapped(retraction) = untapped else {
        panic!("expected ObservationUntapped, got {untapped:?}")
    };
    assert_eq!(
        retraction.subscription_token, token,
        "the retraction names the closed subscription token"
    );

    // Untapping the same token again returns an empty observation set, proving
    // the subscription was retired.
    let again = engine.handle(Query::Untap(token)).into_root();
    let Response::ObservationUntapped(retraction_again) = again else {
        panic!("expected ObservationUntapped, got {again:?}")
    };
    assert!(
        retraction_again.observed_operations.is_empty(),
        "a retired subscription has no further observations"
    );
}

#[test]
fn effects_only_filter_observes_no_operations() {
    let (_temp, mut engine) = engine();

    let _ = engine
        .handle(Query::Record(record_request("intent")))
        .into_root();

    // `EffectsOnly` observes effect events, not operations, so an operation-only
    // log yields an empty observation set under this filter.
    let reply = engine
        .handle(Query::Tap(ObserverFilter::EffectsOnly))
        .into_root();
    let Response::ObservationTapped(subscription) = reply else {
        panic!("expected ObservationTapped, got {reply:?}")
    };
    assert!(
        subscription.observed_operations.is_empty(),
        "the EffectsOnly filter observes no operation events"
    );
}
