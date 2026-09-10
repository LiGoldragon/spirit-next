//! Frozen reader for the immediately preceding deployed Spirit v14 Sema layout.
//!
//! This module is available only to the offline cutover tool.  It reproduces
//! the two v14 family identities and rkyv rows before the v15 configuration
//! family existed; no daemon path can decode or write this representation.

use sema_engine::{
    Engine as SemaDatabase, EngineOpen, FamilyName, QueryPlan, RecordKey, SchemaHash,
    SchemaVersion, TableDescriptor, TableName, TableReference, VersionedStoreName,
    VersioningPolicy,
};
use thiserror::Error;

pub const SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(14);
pub const STORE_NAME: &str = "spirit:sema:v14";
pub const RECORDS_FAMILY: [u8; 32] = [
    169, 167, 27, 203, 113, 158, 12, 113, 89, 93, 195, 166, 134, 208, 34, 40, 178, 38, 203, 139,
    155, 209, 108, 101, 12, 183, 180, 233, 6, 84, 230, 177,
];
pub const MIGRATIONS_FAMILY: [u8; 32] = [
    230, 253, 154, 216, 87, 227, 13, 141, 82, 16, 203, 108, 170, 143, 69, 87, 143, 191, 234, 25,
    90, 168, 75, 182, 238, 134, 0, 229, 158, 24, 20, 143,
];

/// Exact frozen b37fc963 signal-spirit v0.14 payload closure.
///
/// The historical generated source used signal-domain 801e1c5 and wrapped
/// every Entry field. This offline-only copy excludes its Nota derives and
/// runtime operations; rkyv field order and variants are retained.
macro_rules! archived_unit_enum {
    ($name:ident { $($variant:ident),+ $(,)? }) => {
        #[derive(
            rkyv::Archive,
            rkyv::Serialize,
            rkyv::Deserialize,
            Clone,
            Copy,
            Debug,
            PartialEq,
            Eq,
        )]
        pub enum $name {
            $($variant),+
        }
    };
}

archived_unit_enum!(Health {
    Body,
    Mind,
    Nutrition,
    Exercise,
    Sleep,
    Medicine,
    Disease,
    Medication,
    Therapy,
    Reproduction,
    Sexuality,
    Aging,
    Disability,
    Addiction,
    Dentistry,
    Senses,
    Pain,
    Prevention,
    FirstAid,
    Rehabilitation,
});
archived_unit_enum!(Food {
    Cooking,
    Diet,
    Recipe,
    Baking,
    Preservation,
    Fermentation,
    Beverage,
    Entertaining,
    Foraging,
    Fasting,
    Dining,
});
archived_unit_enum!(Home {
    Housing,
    Maintenance,
    Renovation,
    Furnishing,
    Cleaning,
    Tidying,
    Relocation,
    Realty,
    Property,
    Utilities,
    Locksmithing,
    Appliances,
});
archived_unit_enum!(Finance {
    Budgeting,
    Saving,
    Spending,
    Debt,
    Credit,
    Investing,
    Retirement,
    Tax,
    Insurance,
    Income,
    Banking,
    Charity,
    Planning,
    Accounting,
});
archived_unit_enum!(Work {
    Career,
    JobSearch,
    Workplace,
    Vocation,
    Leadership,
    Entrepreneurship,
    Employment,
    Compensation,
    Scheduling,
    Unemployment,
    Freelancing,
    Teamwork,
    Productivity,
    Project,
});
archived_unit_enum!(Craft {
    Electronics,
    Construction,
    Carpentry,
    Metalworking,
    Sewing,
    Manufacturing,
    Repair,
    Engineering,
    Handicraft,
    Invention,
});
archived_unit_enum!(Knowledge {
    Mathematics,
    Logic,
    Physics,
    Chemistry,
    Biology,
    Astronomy,
    Geology,
    Computing,
    Physiology,
    Statistics,
    Research,
    History,
    Linguistics,
    Philosophy,
    Economics,
    Cognition,
    Taxonomy,
});
archived_unit_enum!(Education {
    Studying,
    Teaching,
    Schooling,
    Skill,
    Reading,
    Memorization,
    Pedagogy,
    Mentoring,
    Autodidacticism,
    Credential,
});
archived_unit_enum!(Language {
    Writing,
    Rhetoric,
    Translation,
    Grammar,
    Conversation,
    Correspondence,
    Listening,
    Oratory,
    Editing,
    Terminology,
    Notation,
});
archived_unit_enum!(Art {
    Fiction,
    Poetry,
    Music,
    Painting,
    Photography,
    Film,
    Theater,
    Dance,
    Design,
    Sculpture,
    Creativity,
    Storytelling,
    Publishing,
});
archived_unit_enum!(Kinship {
    Friendship,
    Romance,
    Marriage,
    Family,
    Parenting,
    Relatives,
    Reconciliation,
    Boundaries,
    Intimacy,
    Rapport,
    Caregiving,
    Grief,
    Belonging,
});
archived_unit_enum!(Selfhood {
    Growth,
    Introspection,
    Discipline,
    Emotion,
    Virtue,
    Motivation,
    Confidence,
    Identity,
    Purpose,
    Decision,
    Temperament,
    Wellbeing,
    Composure,
});
archived_unit_enum!(Spirituality {
    Worship,
    Prayer,
    Meditation,
    Ritual,
    Faith,
    Theology,
    Contemplation,
    Pilgrimage,
    Scripture,
    Ethics,
    Mortality,
    Transcendence,
    Asceticism,
    Wisdom,
});
archived_unit_enum!(Governance {
    Politics,
    Government,
    Administration,
    Citizenship,
    Elections,
    Activism,
    Policy,
    Diplomacy,
    Movements,
    Organizing,
    Services,
    Naturalization,
    War,
});
archived_unit_enum!(Law {
    Rights,
    Contract,
    Title,
    Crime,
    Litigation,
    Compliance,
    Custody,
    Liability,
    Procedure,
    Justice,
    Policing,
    Arbitration,
});
archived_unit_enum!(Community {
    Neighborliness,
    Volunteering,
    Solidarity,
    Membership,
    Gatherings,
    Reputation,
    Service,
    Hospitality,
    Institutions,
});
archived_unit_enum!(Nature {
    Agriculture,
    Gardening,
    Horticulture,
    Husbandry,
    Pets,
    Forestry,
    Fishing,
    Hunting,
    Conservation,
    Weather,
    Wilderness,
    Sustainability,
    Resources,
    Stewardship,
});
archived_unit_enum!(Travel {
    Itinerary,
    Destination,
    Transportation,
    Driving,
    Navigation,
    Commuting,
    Logistics,
    Migration,
    Tourism,
    Transit,
    Cycling,
});
archived_unit_enum!(Commerce {
    Selling,
    Buying,
    Marketing,
    Retail,
    Sourcing,
    Trade,
    Support,
    Pricing,
    Negotiation,
    Assets,
    Market,
});
archived_unit_enum!(Leisure {
    Recreation,
    Sport,
    Games,
    Hobby,
    Entertainment,
    Collecting,
    Outdoors,
    Play,
    Relaxation,
    Celebration,
    Fandom,
});
archived_unit_enum!(Appearance {
    Clothing,
    Grooming,
    Style,
    Cosmetics,
    Etiquette,
    Comportment,
});
archived_unit_enum!(Safety {
    Protection,
    Preparedness,
    Risk,
    Cybersecurity,
    Privacy,
    Disaster,
    Military,
    Deterrence,
});
archived_unit_enum!(Information {
    Curation,
    RecordKeeping,
    Documentation,
    News,
    Broadcasting,
    Archives,
    Database,
    Retrieval,
    Classification,
});
archived_unit_enum!(HardwareLeaf { All, Networking });
archived_unit_enum!(ProgrammingLeaf {
    All,
    TypeSystems,
    Compilation,
    Parsing,
    Grammars,
    CodeGeneration,
    Metaprogramming,
    Macros,
    DomainSpecificLanguages,
});
archived_unit_enum!(SystemsLeaf {
    All,
    SystemsProgramming,
    Concurrency,
});
archived_unit_enum!(DistributedLeaf {
    All,
    ProtocolDesign,
    EventDrivenArchitecture,
});
archived_unit_enum!(DataLeaf {
    All,
    Persistence,
    Serialization,
    Formats,
    Modeling,
    SchemaEvolution,
    Migration,
});
archived_unit_enum!(IntelligenceLeaf { All, AgentSystems });
archived_unit_enum!(SecurityLeaf {
    All,
    Cryptography,
    Authentication,
    Authorization,
    SecretsManagement,
    Privacy,
});
archived_unit_enum!(QualityLeaf { All, Testing });
archived_unit_enum!(OperationsLeaf {
    All,
    BuildSystem,
    ReleaseEngineering,
    DependencyManagement,
    Deployment,
    ConfigurationManagement,
});
archived_unit_enum!(ObservabilityLeaf { All, Tracing });
archived_unit_enum!(SurfacesLeaf {
    All,
    Visualization,
    CommandLineInterfaces,
});
archived_unit_enum!(EngineeringLeaf {
    All,
    Architecture,
    Design,
    ApplicationProgrammingInterfaces,
    Documentation,
    VersionControl,
    DevelopmentProcess,
    Management,
    Modularity,
});

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Domain {
    All,
    Health(Health),
    Food(Food),
    Home(Home),
    Finance(Finance),
    Work(Work),
    Craft(Craft),
    Knowledge(Knowledge),
    Education(Education),
    Language(Language),
    Art(Art),
    Kinship(Kinship),
    Selfhood(Selfhood),
    Spirituality(Spirituality),
    Governance(Governance),
    Law(Law),
    Community(Community),
    Nature(Nature),
    Travel(Travel),
    Commerce(Commerce),
    Leisure(Leisure),
    Appearance(Appearance),
    Safety(Safety),
    Information(Information),
    Technology(Technology),
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Technology {
    Hardware(HardwareLeaf),
    Software(Software),
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Software {
    Programming(ProgrammingLeaf),
    Theory,
    Systems(SystemsLeaf),
    Distributed(DistributedLeaf),
    Data(DataLeaf),
    Intelligence(IntelligenceLeaf),
    Security(SecurityLeaf),
    Quality(QualityLeaf),
    Operations(OperationsLeaf),
    Observability(ObservabilityLeaf),
    Surfaces(SurfacesLeaf),
    Engineering(EngineeringLeaf),
}

archived_unit_enum!(Kind {
    Decision,
    Principle,
    Correction,
    Clarification,
    Constraint,
});
archived_unit_enum!(Magnitude {
    Zero,
    Minimum,
    VeryLow,
    Low,
    Medium,
    High,
    VeryHigh,
    Maximum,
});

/// Capability carried by an offline frozen one-field payload. It preserves
/// the published tuple layout while keeping the migration reader trait-borne.
pub trait FrozenContentful {
    type Content;
    fn content(&self) -> &Self::Content;
    fn into_content(self) -> Self::Content;
}

macro_rules! archived_newtype {
    ($name:ident($inner:ty)) => {
        #[derive(
            rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq,
        )]
        pub struct $name($inner);

        impl From<$inner> for $name {
            fn from(value: $inner) -> Self {
                Self(value)
            }
        }

        impl FrozenContentful for $name {
            type Content = $inner;
            fn content(&self) -> &Self::Content {
                &self.0
            }
            fn into_content(self) -> Self::Content {
                self.0
            }
        }
    };
}

archived_newtype!(Domains(Vec<Domain>));
archived_newtype!(Referent(String));
archived_newtype!(Referents(Vec<Referent>));
archived_newtype!(Aliases(Referents));
archived_newtype!(Description(String));
archived_newtype!(RecordIdentifier(String));
archived_newtype!(Privacy(Magnitude));
archived_newtype!(Certainty(Magnitude));
archived_newtype!(Importance(Magnitude));
archived_newtype!(SourceSchemaVersion(u64));
archived_newtype!(MigratedRecordCount(u64));
archived_newtype!(MigratedReferentCount(u64));

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub domains: Domains,
    pub kind: Kind,
    pub description: Description,
    pub importance: Importance,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct StoredRecord {
    pub record_identifier: RecordIdentifier,
    pub entry: Entry,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Migration {
    pub source_schema_version: SourceSchemaVersion,
    pub migrated_record_count: MigratedRecordCount,
}

impl sema_engine::EngineRecord for StoredRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.record_identifier.content().clone())
    }
}
impl sema_engine::EngineRecord for Migration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!(
            "from-schema-{}",
            *self.source_schema_version.content()
        ))
    }
}

pub fn records_descriptor() -> TableDescriptor<StoredRecord> {
    TableDescriptor::new(
        TableName::new("records"),
        FamilyName::new("RecordsFamily"),
        SchemaHash::new(RECORDS_FAMILY),
    )
}
pub fn migrations_descriptor() -> TableDescriptor<Migration> {
    TableDescriptor::new(
        TableName::new("migrations"),
        FamilyName::new("MigrationsFamily"),
        SchemaHash::new(MIGRATIONS_FAMILY),
    )
}

#[derive(Debug, Error)]
pub enum ReaderError {
    #[error("v14 sema: {0}")]
    Engine(#[from] sema_engine::Error),
}
pub struct LiveReader {
    database: SemaDatabase,
    records: TableReference<StoredRecord>,
    migrations: TableReference<Migration>,
}

/// Offline-only capability for enumerating a v14 store.
pub trait V14Readable: Sized {
    fn open(path: impl AsRef<std::path::Path>) -> Result<Self, ReaderError>;
    fn records(&self) -> Result<Vec<StoredRecord>, ReaderError>;
    fn migrations(&self) -> Result<Vec<Migration>, ReaderError>;
}

impl V14Readable for LiveReader {
    fn open(path: impl AsRef<std::path::Path>) -> Result<Self, ReaderError> {
        let mut database = SemaDatabase::open(
            EngineOpen::new(path.as_ref(), SCHEMA_VERSION)
                .with_versioning(VersioningPolicy::new(VersionedStoreName::new(STORE_NAME))),
        )?;
        let records = database.register_table(records_descriptor())?;
        let migrations = database.register_table(migrations_descriptor())?;
        Ok(Self {
            database,
            records,
            migrations,
        })
    }

    fn records(&self) -> Result<Vec<StoredRecord>, ReaderError> {
        Ok(self
            .database
            .match_records(QueryPlan::all(self.records))?
            .records()
            .to_vec())
    }

    fn migrations(&self) -> Result<Vec<Migration>, ReaderError> {
        Ok(self
            .database
            .match_records(QueryPlan::all(self.migrations))?
            .records()
            .to_vec())
    }
}

/// Exact b37fc963 standalone daemon-configuration archive closure. It is
/// decoded only by the offline migration tool; `database_path` is deliberately
/// retained for validation/provenance but never becomes mutable Nexus state.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ConfigurationPath(String);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SpiritGuardianProviderName(String);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SpiritGuardianModelName(String);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SpiritGuardianTimeoutMilliseconds(u64);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SpiritGuardianMaximumOutputTokens(u64);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct AgentSocketPath(ConfigurationPath);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ProviderName(Option<SpiritGuardianProviderName>);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ModelName(Option<SpiritGuardianModelName>);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct TimeoutMilliseconds(SpiritGuardianTimeoutMilliseconds);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct MaximumOutputTokens(Option<SpiritGuardianMaximumOutputTokens>);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SpiritGuardianAgentConfiguration {
    agent_socket_path: AgentSocketPath,
    optional_spirit_guardian_provider_name: ProviderName,
    optional_spirit_guardian_model_name: ModelName,
    spirit_guardian_timeout_milliseconds: TimeoutMilliseconds,
    optional_spirit_guardian_maximum_output_tokens: MaximumOutputTokens,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GuardianAgentConfiguration(Option<SpiritGuardianAgentConfiguration>);
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorizationMode {
    Gating,
    Observing,
}
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SpiritDaemonConfiguration {
    pub socket_path: ConfigurationPath,
    pub meta_socket_path: Option<ConfigurationPath>,
    pub database_path: ConfigurationPath,
    pub trace_socket_path: Option<ConfigurationPath>,
    pub authorization_mode: AuthorizationMode,
    guardian_agent_configuration: GuardianAgentConfiguration,
}

/// Source-borne projection of the frozen configuration into the current
/// desired Nexus state. The old database path remains outside this result.
pub trait NexusConfigurationProjectable {
    fn project_nexus_configuration(self)
    -> Result<signal_spirit::SpiritNexusConfiguration, String>;
}
impl NexusConfigurationProjectable for SpiritDaemonConfiguration {
    fn project_nexus_configuration(
        self,
    ) -> Result<signal_spirit::SpiritNexusConfiguration, String> {
        let guardian = match self.guardian_agent_configuration.0 {
            None => None,
            Some(guardian) => Some(signal_spirit::SpiritGuardianAgentConfiguration {
                agent_socket_path: guardian.agent_socket_path.0.0,
                optional_spirit_guardian_provider_name: guardian
                    .optional_spirit_guardian_provider_name
                    .0
                    .map(|value| value.0),
                optional_spirit_guardian_model_name: guardian
                    .optional_spirit_guardian_model_name
                    .0
                    .map(|value| value.0),
                spirit_guardian_timeout_milliseconds: i64::try_from(
                    guardian.spirit_guardian_timeout_milliseconds.0.0,
                )
                .map_err(|_| {
                    String::from("historical guardian timeout exceeds current signed range")
                })?,
                optional_spirit_guardian_maximum_output_tokens: guardian
                    .optional_spirit_guardian_maximum_output_tokens
                    .0
                    .map(|value| {
                        i64::try_from(value.0).map_err(|_| {
                            String::from(
                                "historical guardian token maximum exceeds current signed range",
                            )
                        })
                    })
                    .transpose()?,
            }),
        };
        Ok(signal_spirit::SpiritNexusConfiguration {
            socket_path: self.socket_path.0,
            optional_meta_socket_path: self.meta_socket_path.map(|value| value.0),
            optional_trace_socket_path: self.trace_socket_path.map(|value| value.0),
            authorization_mode: match self.authorization_mode {
                AuthorizationMode::Gating => signal_spirit::AuthorizationMode::Gating,
                AuthorizationMode::Observing => signal_spirit::AuthorizationMode::Observing,
            },
            optional_spirit_guardian_agent_configuration: guardian,
        })
    }
}
