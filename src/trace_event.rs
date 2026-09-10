use crate::{
    engine::SignalObjectName,
    schema::{nexus::NexusObjectName, sema::SemaObjectName},
};
#[cfg(feature = "testing-trace")]
use signal_introspect::{ComponentTraceEvent, IntrospectionTarget, TraceLayer};

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(
    feature = "datom-cli",
    derive(datom_codec::Datomizable, datom_codec::Compositional)
)]
pub enum ObjectName {
    Signal(SignalObjectName),
    Nexus(NexusObjectName),
    Sema(SemaObjectName),
    Authorization(AuthorizationObjectName),
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(
    feature = "datom-cli",
    derive(datom_codec::Datomizable, datom_codec::Compositional)
)]
pub enum AuthorizationObjectName {
    Observed,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(
    feature = "datom-cli",
    derive(datom_codec::Datomizable, datom_codec::Compositional)
)]
pub struct TraceEvent(pub ObjectName);

impl ObjectName {
    pub fn name(self) -> &'static str {
        match self {
            Self::Signal(object_name) => object_name.name(),
            Self::Nexus(object_name) => object_name.name(),
            Self::Sema(object_name) => object_name.name(),
            Self::Authorization(object_name) => object_name.name(),
        }
    }
}

impl AuthorizationObjectName {
    pub fn name(self) -> &'static str {
        match self {
            Self::Observed => "AuthorizationObserved",
        }
    }
}

impl TraceEvent {
    pub fn new(object_name: ObjectName) -> Self {
        Self(object_name)
    }

    pub fn object_name(&self) -> ObjectName {
        self.0
    }

    pub fn name(&self) -> &'static str {
        self.0.name()
    }
}

/// The actor-boundary layer of one of spirit's three execution centers,
/// projected onto the shared `signal-introspect` classification axis.
#[cfg(feature = "testing-trace")]
impl From<ObjectName> for TraceLayer {
    fn from(object_name: ObjectName) -> Self {
        match object_name {
            ObjectName::Signal(_) => TraceLayer::Signal,
            ObjectName::Nexus(_) => TraceLayer::Nexus,
            ObjectName::Sema(_) => TraceLayer::Sema,
            ObjectName::Authorization(_) => TraceLayer::Authorization,
        }
    }
}

/// Project spirit's own actor-boundary `TraceEvent` onto the shared wire
/// contract `signal_introspect::ComponentTraceEvent` (report 716). The layer
/// and the schema-derived event name come from the event itself; the emitting
/// `engine` identity and the monotonic per-emitter `sequence` are not carried
/// by a `TraceEvent`, so this conversion stamps the empty engine and sequence
/// zero. The socket sink in `crate::trace` owns the engine identity and the
/// shared monotonic counter and overwrites both at the push boundary.
#[cfg(feature = "testing-trace")]
impl From<TraceEvent> for ComponentTraceEvent {
    fn from(event: TraceEvent) -> Self {
        let object_name = event.object_name();
        ComponentTraceEvent {
            engine_identifier: String::new(),
            introspection_target: IntrospectionTarget::from(object_name),
            trace_layer: TraceLayer::from(object_name),
            trace_event_name: object_name.name().to_owned(),
            trace_sequence: 0,
        }
    }
}

#[cfg(feature = "testing-trace")]
impl From<ObjectName> for IntrospectionTarget {
    fn from(object_name: ObjectName) -> Self {
        match object_name {
            ObjectName::Authorization(_) => IntrospectionTarget::Spirit,
            ObjectName::Signal(_) | ObjectName::Nexus(_) | ObjectName::Sema(_) => {
                IntrospectionTarget::Signal
            }
        }
    }
}
