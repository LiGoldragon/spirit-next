use std::io::Cursor;

use signal_spirit::{Restorable as _, Signal};
use spirit::{SignalTransport, schema::signal::Query};
use triad_runtime::{FrameBody, LengthPrefixedCodec};

#[test]
fn transport_rejects_length_prefixed_raw_nota_text() {
    let nota =
        b"(Record (([(Technology (Software (Intelligence AgentSystems)))] Decision [text must not be daemon wire] Maximum Minimum Zero []) ([text must not be daemon wire] None)))";
    let bytes = LengthPrefixedCodec::default()
        .encode_body(&FrameBody::new(nota.to_vec()))
        .expect("length-prefixed frame");
    let mut transport = SignalTransport::new(Cursor::new(bytes));

    assert!(
        transport.read_input().is_err(),
        "daemon wire transport must reject length-prefixed raw NOTA bytes"
    );
}

#[test]
fn transport_rejects_length_prefixed_garbage_bytes() {
    let bytes = LengthPrefixedCodec::default()
        .encode_body(&FrameBody::new([0_u8; 16]))
        .expect("length-prefixed frame");
    let mut transport = SignalTransport::new(Cursor::new(bytes));

    assert!(
        transport.read_input().is_err(),
        "daemon wire transport must reject arbitrary bytes"
    );
}

#[test]
fn generated_input_decoder_rejects_raw_nota_text_directly() {
    let nota =
        b"(Record (([(Technology (Software (Intelligence AgentSystems)))] Decision [text must not be signal frame] Maximum Minimum Zero []) ([text must not be signal frame] None)))";

    assert!(
        Signal::<Query>::from(nota.to_vec()).restore().is_err(),
        "schema-emitted binary decoder must reject raw NOTA text"
    );
}
