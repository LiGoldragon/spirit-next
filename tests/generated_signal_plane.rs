mod support;

use spirit::schema::signal::{
    ByteViewable, Entry, Justification, Kind, Magnitude, Query, RecordRequest, Response,
    Restorable, Signal, Signalizable, VerbatimQuote,
};
use support::domain_fixtures;

fn entry() -> Entry {
    Entry {
        domains: domain_fixtures::domains(&["schema"]),
        kind: Kind::Constraint,
        description: "Ethos creates the signal plane".into(),
        importance: Magnitude::Medium,
    }
}
fn record_request() -> RecordRequest {
    RecordRequest {
        entry: entry(),
        justification: Justification {
            testimony: vec![VerbatimQuote {
                quote_text: "Ethos creates the signal plane".into(),
                optional_antecedent: None,
            }],
            reasoning: "generated contract witness".into(),
        },
    }
}

#[test]
fn generated_record_query_round_trips_fresh_received_bytes() {
    let query = Query::Record(record_request());
    let sent = query.signalize().expect("archive query");
    let received: Signal<Query> = Signal::from(sent.bytes().to_vec());
    assert_eq!(received.restore().expect("restore query"), query);
}

#[test]
fn generated_read_queries_round_trip_fresh_received_bytes() {
    for query in [
        Query::TextSearch("Ethos".into()),
        Query::Intent(domain_fixtures::scopes(&["schema"])),
    ] {
        let sent = query.signalize().expect("archive query");
        let received: Signal<Query> = Signal::from(sent.bytes().to_vec());
        assert_eq!(received.restore().expect("restore query"), query);
    }
}

#[test]
fn generated_response_round_trips_fresh_received_bytes() {
    let response = Response::RecordAccepted("003g".into());
    let sent = response.signalize().expect("archive response");
    let received: Signal<Response> = Signal::from(sent.bytes().to_vec());
    assert_eq!(received.restore().expect("restore response"), response);
}

#[test]
fn malformed_archive_is_rejected_at_the_restore_boundary() {
    let received: Signal<Query> = Signal::from(vec![0xFF, 0x00, 0xAA]);
    assert!(received.restore().is_err());
}
