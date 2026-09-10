use signal_spirit::{AuthorizationMode, ConfigurationPath, DataLeaf, Description, Domain, Domains, Entry, GuardianAgentConfiguration, Importance, Kind, Magnitude, RecordIdentifier, Software, SpiritDaemonConfiguration, Technology};

fn main() {
    let entry = Entry {
        domains: Domains::new(vec![Domain::Technology(Technology::Software(Software::Data(DataLeaf::SchemaEvolution)))]),
        kind: Kind::Constraint,
        description: Description::new(String::from("fixture emitted by b37fc963")),
        importance: Importance::new(Magnitude::VeryHigh),
    };
    let record = (RecordIdentifier::new(String::from("b37-fixture")), entry);
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&record).expect("serialize exact old producer payload");
    let destination = std::env::var("V14_FIXTURE_OUT").expect("V14_FIXTURE_OUT");
    std::fs::write(destination, bytes).expect("write fixture");

    let configuration = SpiritDaemonConfiguration {
        socket_path: ConfigurationPath::new("/tmp/old-spirit.sock"),
        meta_socket_path: Some(ConfigurationPath::new("/tmp/old-meta.sock")),
        database_path: ConfigurationPath::new("/home/li/.local/state/spirit/spirit.sema"),
        trace_socket_path: Some(ConfigurationPath::new("/tmp/old-trace.sock")),
        authorization_mode: AuthorizationMode::Observing,
        guardian_agent_configuration: GuardianAgentConfiguration::new(None),
    };
    let configuration_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&configuration).expect("serialize exact old configuration");
    let configuration_destination = std::env::var("V14_CONFIGURATION_FIXTURE_OUT").expect("V14_CONFIGURATION_FIXTURE_OUT");
    std::fs::write(configuration_destination, configuration_bytes).expect("write configuration fixture");
}
