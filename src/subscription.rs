//! Spirit's stream subscription token.
//!
//! The daemon-local subscription table owns token storage and writes typed
//! `Signal<Response>` event bytes directly to the length-prefixed socket.
//! This small newtype keeps the public signal integer distinct from the
//! daemon's registration key without importing a frame protocol.

use crate::schema::signal::SubscriptionToken as SignalSubscriptionToken;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct IntentSubscriptionToken(i64);

impl IntentSubscriptionToken {
    pub fn from_signal_token(token: SignalSubscriptionToken) -> Self {
        Self(token)
    }
}
