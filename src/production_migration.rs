//! One-way, offline projection from Spirit store schema 13 to schema 14.
//!
//! The migration reads the frozen v13 materialized tables, retains each
//! record's identifier, domains, kind, description, and importance, and
//! creates a fresh v14 log containing only those projected records plus one
//! v13-to-v14 receipt. The v13 referent catalogue, prior migration receipts,
//! log, and checkpoints are deliberately not replayed.
//!
//! Before either current store is exposed, the complete live and lifecycle
//! archive projections are built and reopened for validation. Exact v13 live,
//! archive, and guardian-v6 bytes are retained only as byte-for-byte copies inside a
//! private rollback directory beside the live store.

/// Frozen schema-version-13 readers. This is the only legacy decoder in the
/// release and is compiled only into the offline migration feature.
pub mod v13;
pub mod v14;

use v14::{FrozenContentful as _, NexusConfigurationProjectable as _, V14Readable as _};

mod frozen_projection {
    use super::v13;

    pub fn health(value: v13::Health) -> signal_domain::HealthDomain {
        match value {
            v13::Health::Body => signal_domain::HealthDomain::Body,
            v13::Health::Mind => signal_domain::HealthDomain::Mind,
            v13::Health::Nutrition => signal_domain::HealthDomain::Nutrition,
            v13::Health::Exercise => signal_domain::HealthDomain::Exercise,
            v13::Health::Sleep => signal_domain::HealthDomain::Sleep,
            v13::Health::Medicine => signal_domain::HealthDomain::Medicine,
            v13::Health::Disease => signal_domain::HealthDomain::Disease,
            v13::Health::Medication => signal_domain::HealthDomain::Medication,
            v13::Health::Therapy => signal_domain::HealthDomain::Therapy,
            v13::Health::Reproduction => signal_domain::HealthDomain::Reproduction,
            v13::Health::Sexuality => signal_domain::HealthDomain::Sexuality,
            v13::Health::Aging => signal_domain::HealthDomain::Aging,
            v13::Health::Disability => signal_domain::HealthDomain::Disability,
            v13::Health::Addiction => signal_domain::HealthDomain::Addiction,
            v13::Health::Dentistry => signal_domain::HealthDomain::Dentistry,
            v13::Health::Senses => signal_domain::HealthDomain::Senses,
            v13::Health::Pain => signal_domain::HealthDomain::Pain,
            v13::Health::Prevention => signal_domain::HealthDomain::Prevention,
            v13::Health::FirstAid => signal_domain::HealthDomain::FirstAid,
            v13::Health::Rehabilitation => signal_domain::HealthDomain::Rehabilitation,
        }
    }

    pub fn food(value: v13::Food) -> signal_domain::FoodDomain {
        match value {
            v13::Food::Cooking => signal_domain::FoodDomain::Cooking,
            v13::Food::Diet => signal_domain::FoodDomain::Diet,
            v13::Food::Recipe => signal_domain::FoodDomain::Recipe,
            v13::Food::Baking => signal_domain::FoodDomain::Baking,
            v13::Food::Preservation => signal_domain::FoodDomain::Preservation,
            v13::Food::Fermentation => signal_domain::FoodDomain::Fermentation,
            v13::Food::Beverage => signal_domain::FoodDomain::Beverage,
            v13::Food::Entertaining => signal_domain::FoodDomain::Entertaining,
            v13::Food::Foraging => signal_domain::FoodDomain::Foraging,
            v13::Food::Fasting => signal_domain::FoodDomain::Fasting,
            v13::Food::Dining => signal_domain::FoodDomain::Dining,
        }
    }

    pub fn home(value: v13::Home) -> signal_domain::HomeDomain {
        match value {
            v13::Home::Housing => signal_domain::HomeDomain::Housing,
            v13::Home::Maintenance => signal_domain::HomeDomain::Maintenance,
            v13::Home::Renovation => signal_domain::HomeDomain::Renovation,
            v13::Home::Furnishing => signal_domain::HomeDomain::Furnishing,
            v13::Home::Cleaning => signal_domain::HomeDomain::Cleaning,
            v13::Home::Tidying => signal_domain::HomeDomain::Tidying,
            v13::Home::Relocation => signal_domain::HomeDomain::Relocation,
            v13::Home::Realty => signal_domain::HomeDomain::Realty,
            v13::Home::Property => signal_domain::HomeDomain::Property,
            v13::Home::Utilities => signal_domain::HomeDomain::Utilities,
            v13::Home::Locksmithing => signal_domain::HomeDomain::Locksmithing,
            v13::Home::Appliances => signal_domain::HomeDomain::Appliances,
        }
    }

    pub fn finance(value: v13::Finance) -> signal_domain::FinanceDomain {
        match value {
            v13::Finance::Budgeting => signal_domain::FinanceDomain::Budgeting,
            v13::Finance::Saving => signal_domain::FinanceDomain::Saving,
            v13::Finance::Spending => signal_domain::FinanceDomain::Spending,
            v13::Finance::Debt => signal_domain::FinanceDomain::Debt,
            v13::Finance::Credit => signal_domain::FinanceDomain::Credit,
            v13::Finance::Investing => signal_domain::FinanceDomain::Investing,
            v13::Finance::Retirement => signal_domain::FinanceDomain::Retirement,
            v13::Finance::Tax => signal_domain::FinanceDomain::Tax,
            v13::Finance::Insurance => signal_domain::FinanceDomain::Insurance,
            v13::Finance::Income => signal_domain::FinanceDomain::Income,
            v13::Finance::Banking => signal_domain::FinanceDomain::Banking,
            v13::Finance::Charity => signal_domain::FinanceDomain::Charity,
            v13::Finance::Planning => signal_domain::FinanceDomain::Planning,
            v13::Finance::Accounting => signal_domain::FinanceDomain::Accounting,
        }
    }

    pub fn work(value: v13::Work) -> signal_domain::WorkDomain {
        match value {
            v13::Work::Career => signal_domain::WorkDomain::Career,
            v13::Work::JobSearch => signal_domain::WorkDomain::JobSearch,
            v13::Work::Workplace => signal_domain::WorkDomain::Workplace,
            v13::Work::Vocation => signal_domain::WorkDomain::Vocation,
            v13::Work::Leadership => signal_domain::WorkDomain::Leadership,
            v13::Work::Entrepreneurship => signal_domain::WorkDomain::Entrepreneurship,
            v13::Work::Employment => signal_domain::WorkDomain::Employment,
            v13::Work::Compensation => signal_domain::WorkDomain::Compensation,
            v13::Work::Scheduling => signal_domain::WorkDomain::Scheduling,
            v13::Work::Unemployment => signal_domain::WorkDomain::Unemployment,
            v13::Work::Freelancing => signal_domain::WorkDomain::Freelancing,
            v13::Work::Teamwork => signal_domain::WorkDomain::Teamwork,
            v13::Work::Productivity => signal_domain::WorkDomain::Productivity,
            v13::Work::Project => signal_domain::WorkDomain::Project,
        }
    }

    pub fn craft(value: v13::Craft) -> signal_domain::CraftDomain {
        match value {
            v13::Craft::Electronics => signal_domain::CraftDomain::Electronics,
            v13::Craft::Construction => signal_domain::CraftDomain::Construction,
            v13::Craft::Carpentry => signal_domain::CraftDomain::Carpentry,
            v13::Craft::Metalworking => signal_domain::CraftDomain::Metalworking,
            v13::Craft::Sewing => signal_domain::CraftDomain::Sewing,
            v13::Craft::Manufacturing => signal_domain::CraftDomain::Manufacturing,
            v13::Craft::Repair => signal_domain::CraftDomain::Repair,
            v13::Craft::Engineering => signal_domain::CraftDomain::Engineering,
            v13::Craft::Handicraft => signal_domain::CraftDomain::Handicraft,
            v13::Craft::Invention => signal_domain::CraftDomain::Invention,
        }
    }

    pub fn knowledge(value: v13::Knowledge) -> signal_domain::KnowledgeDomain {
        match value {
            v13::Knowledge::Mathematics => signal_domain::KnowledgeDomain::Mathematics,
            v13::Knowledge::Logic => signal_domain::KnowledgeDomain::Logic,
            v13::Knowledge::Physics => signal_domain::KnowledgeDomain::Physics,
            v13::Knowledge::Chemistry => signal_domain::KnowledgeDomain::Chemistry,
            v13::Knowledge::Biology => signal_domain::KnowledgeDomain::Biology,
            v13::Knowledge::Astronomy => signal_domain::KnowledgeDomain::Astronomy,
            v13::Knowledge::Geology => signal_domain::KnowledgeDomain::Geology,
            v13::Knowledge::Computing => signal_domain::KnowledgeDomain::Computing,
            v13::Knowledge::Physiology => signal_domain::KnowledgeDomain::Physiology,
            v13::Knowledge::Statistics => signal_domain::KnowledgeDomain::Statistics,
            v13::Knowledge::Research => signal_domain::KnowledgeDomain::Research,
            v13::Knowledge::History => signal_domain::KnowledgeDomain::History,
            v13::Knowledge::Linguistics => signal_domain::KnowledgeDomain::Linguistics,
            v13::Knowledge::Philosophy => signal_domain::KnowledgeDomain::Philosophy,
            v13::Knowledge::Economics => signal_domain::KnowledgeDomain::Economics,
            v13::Knowledge::Cognition => signal_domain::KnowledgeDomain::Cognition,
            v13::Knowledge::Taxonomy => signal_domain::KnowledgeDomain::Taxonomy,
        }
    }

    pub fn education(value: v13::Education) -> signal_domain::EducationDomain {
        match value {
            v13::Education::Studying => signal_domain::EducationDomain::Studying,
            v13::Education::Teaching => signal_domain::EducationDomain::Teaching,
            v13::Education::Schooling => signal_domain::EducationDomain::Schooling,
            v13::Education::Skill => signal_domain::EducationDomain::Skill,
            v13::Education::Reading => signal_domain::EducationDomain::Reading,
            v13::Education::Memorization => signal_domain::EducationDomain::Memorization,
            v13::Education::Pedagogy => signal_domain::EducationDomain::Pedagogy,
            v13::Education::Mentoring => signal_domain::EducationDomain::Mentoring,
            v13::Education::Autodidacticism => signal_domain::EducationDomain::Autodidacticism,
            v13::Education::Credential => signal_domain::EducationDomain::Credential,
        }
    }

    pub fn language(value: v13::Language) -> signal_domain::LanguageDomain {
        match value {
            v13::Language::Writing => signal_domain::LanguageDomain::Writing,
            v13::Language::Rhetoric => signal_domain::LanguageDomain::Rhetoric,
            v13::Language::Translation => signal_domain::LanguageDomain::Translation,
            v13::Language::Grammar => signal_domain::LanguageDomain::Grammar,
            v13::Language::Conversation => signal_domain::LanguageDomain::Conversation,
            v13::Language::Correspondence => signal_domain::LanguageDomain::Correspondence,
            v13::Language::Listening => signal_domain::LanguageDomain::Listening,
            v13::Language::Oratory => signal_domain::LanguageDomain::Oratory,
            v13::Language::Editing => signal_domain::LanguageDomain::Editing,
            v13::Language::Terminology => signal_domain::LanguageDomain::Terminology,
            v13::Language::Notation => signal_domain::LanguageDomain::Notation,
        }
    }

    pub fn art(value: v13::Art) -> signal_domain::ArtDomain {
        match value {
            v13::Art::Fiction => signal_domain::ArtDomain::Fiction,
            v13::Art::Poetry => signal_domain::ArtDomain::Poetry,
            v13::Art::Music => signal_domain::ArtDomain::Music,
            v13::Art::Painting => signal_domain::ArtDomain::Painting,
            v13::Art::Photography => signal_domain::ArtDomain::Photography,
            v13::Art::Film => signal_domain::ArtDomain::Film,
            v13::Art::Theater => signal_domain::ArtDomain::Theater,
            v13::Art::Dance => signal_domain::ArtDomain::Dance,
            v13::Art::Design => signal_domain::ArtDomain::Design,
            v13::Art::Sculpture => signal_domain::ArtDomain::Sculpture,
            v13::Art::Creativity => signal_domain::ArtDomain::Creativity,
            v13::Art::Storytelling => signal_domain::ArtDomain::Storytelling,
            v13::Art::Publishing => signal_domain::ArtDomain::Publishing,
        }
    }

    pub fn kinship(value: v13::Kinship) -> signal_domain::KinshipDomain {
        match value {
            v13::Kinship::Friendship => signal_domain::KinshipDomain::Friendship,
            v13::Kinship::Romance => signal_domain::KinshipDomain::Romance,
            v13::Kinship::Marriage => signal_domain::KinshipDomain::Marriage,
            v13::Kinship::Family => signal_domain::KinshipDomain::Family,
            v13::Kinship::Parenting => signal_domain::KinshipDomain::Parenting,
            v13::Kinship::Relatives => signal_domain::KinshipDomain::Relatives,
            v13::Kinship::Reconciliation => signal_domain::KinshipDomain::Reconciliation,
            v13::Kinship::Boundaries => signal_domain::KinshipDomain::Boundaries,
            v13::Kinship::Intimacy => signal_domain::KinshipDomain::Intimacy,
            v13::Kinship::Rapport => signal_domain::KinshipDomain::Rapport,
            v13::Kinship::Caregiving => signal_domain::KinshipDomain::Caregiving,
            v13::Kinship::Grief => signal_domain::KinshipDomain::Grief,
            v13::Kinship::Belonging => signal_domain::KinshipDomain::Belonging,
        }
    }

    pub fn selfhood(value: v13::Selfhood) -> signal_domain::SelfhoodDomain {
        match value {
            v13::Selfhood::Growth => signal_domain::SelfhoodDomain::Growth,
            v13::Selfhood::Introspection => signal_domain::SelfhoodDomain::Introspection,
            v13::Selfhood::Discipline => signal_domain::SelfhoodDomain::Discipline,
            v13::Selfhood::Emotion => signal_domain::SelfhoodDomain::Emotion,
            v13::Selfhood::Virtue => signal_domain::SelfhoodDomain::Virtue,
            v13::Selfhood::Motivation => signal_domain::SelfhoodDomain::Motivation,
            v13::Selfhood::Confidence => signal_domain::SelfhoodDomain::Confidence,
            v13::Selfhood::Identity => signal_domain::SelfhoodDomain::Identity,
            v13::Selfhood::Purpose => signal_domain::SelfhoodDomain::Purpose,
            v13::Selfhood::Decision => signal_domain::SelfhoodDomain::Decision,
            v13::Selfhood::Temperament => signal_domain::SelfhoodDomain::Temperament,
            v13::Selfhood::Wellbeing => signal_domain::SelfhoodDomain::Wellbeing,
            v13::Selfhood::Composure => signal_domain::SelfhoodDomain::Composure,
        }
    }

    pub fn spirituality(value: v13::Spirituality) -> signal_domain::SpiritualityDomain {
        match value {
            v13::Spirituality::Worship => signal_domain::SpiritualityDomain::Worship,
            v13::Spirituality::Prayer => signal_domain::SpiritualityDomain::Prayer,
            v13::Spirituality::Meditation => signal_domain::SpiritualityDomain::Meditation,
            v13::Spirituality::Ritual => signal_domain::SpiritualityDomain::Ritual,
            v13::Spirituality::Faith => signal_domain::SpiritualityDomain::Faith,
            v13::Spirituality::Theology => signal_domain::SpiritualityDomain::Theology,
            v13::Spirituality::Contemplation => signal_domain::SpiritualityDomain::Contemplation,
            v13::Spirituality::Pilgrimage => signal_domain::SpiritualityDomain::Pilgrimage,
            v13::Spirituality::Scripture => signal_domain::SpiritualityDomain::Scripture,
            v13::Spirituality::Ethics => signal_domain::SpiritualityDomain::Ethics,
            v13::Spirituality::Mortality => signal_domain::SpiritualityDomain::Mortality,
            v13::Spirituality::Transcendence => signal_domain::SpiritualityDomain::Transcendence,
            v13::Spirituality::Asceticism => signal_domain::SpiritualityDomain::Asceticism,
            v13::Spirituality::Wisdom => signal_domain::SpiritualityDomain::Wisdom,
        }
    }

    pub fn governance(value: v13::Governance) -> signal_domain::GovernanceDomain {
        match value {
            v13::Governance::Politics => signal_domain::GovernanceDomain::Politics,
            v13::Governance::Government => signal_domain::GovernanceDomain::Government,
            v13::Governance::Administration => signal_domain::GovernanceDomain::Administration,
            v13::Governance::Citizenship => signal_domain::GovernanceDomain::Citizenship,
            v13::Governance::Elections => signal_domain::GovernanceDomain::Elections,
            v13::Governance::Activism => signal_domain::GovernanceDomain::Activism,
            v13::Governance::Policy => signal_domain::GovernanceDomain::Policy,
            v13::Governance::Diplomacy => signal_domain::GovernanceDomain::Diplomacy,
            v13::Governance::Movements => signal_domain::GovernanceDomain::Movements,
            v13::Governance::Organizing => signal_domain::GovernanceDomain::Organizing,
            v13::Governance::Services => signal_domain::GovernanceDomain::Services,
            v13::Governance::Naturalization => signal_domain::GovernanceDomain::Naturalization,
            v13::Governance::War => signal_domain::GovernanceDomain::War,
        }
    }

    pub fn law(value: v13::Law) -> signal_domain::LawDomain {
        match value {
            v13::Law::Rights => signal_domain::LawDomain::Rights,
            v13::Law::Contract => signal_domain::LawDomain::Contract,
            v13::Law::Title => signal_domain::LawDomain::Title,
            v13::Law::Crime => signal_domain::LawDomain::Crime,
            v13::Law::Litigation => signal_domain::LawDomain::Litigation,
            v13::Law::Compliance => signal_domain::LawDomain::Compliance,
            v13::Law::Custody => signal_domain::LawDomain::Custody,
            v13::Law::Liability => signal_domain::LawDomain::Liability,
            v13::Law::Procedure => signal_domain::LawDomain::Procedure,
            v13::Law::Justice => signal_domain::LawDomain::Justice,
            v13::Law::Policing => signal_domain::LawDomain::Policing,
            v13::Law::Arbitration => signal_domain::LawDomain::Arbitration,
        }
    }

    pub fn community(value: v13::Community) -> signal_domain::CommunityDomain {
        match value {
            v13::Community::Neighborliness => signal_domain::CommunityDomain::Neighborliness,
            v13::Community::Volunteering => signal_domain::CommunityDomain::Volunteering,
            v13::Community::Solidarity => signal_domain::CommunityDomain::Solidarity,
            v13::Community::Membership => signal_domain::CommunityDomain::Membership,
            v13::Community::Gatherings => signal_domain::CommunityDomain::Gatherings,
            v13::Community::Reputation => signal_domain::CommunityDomain::Reputation,
            v13::Community::Service => signal_domain::CommunityDomain::Service,
            v13::Community::Hospitality => signal_domain::CommunityDomain::Hospitality,
            v13::Community::Institutions => signal_domain::CommunityDomain::Institutions,
        }
    }

    pub fn nature(value: v13::Nature) -> signal_domain::NatureDomain {
        match value {
            v13::Nature::Agriculture => signal_domain::NatureDomain::Agriculture,
            v13::Nature::Gardening => signal_domain::NatureDomain::Gardening,
            v13::Nature::Horticulture => signal_domain::NatureDomain::Horticulture,
            v13::Nature::Husbandry => signal_domain::NatureDomain::Husbandry,
            v13::Nature::Pets => signal_domain::NatureDomain::Pets,
            v13::Nature::Forestry => signal_domain::NatureDomain::Forestry,
            v13::Nature::Fishing => signal_domain::NatureDomain::Fishing,
            v13::Nature::Hunting => signal_domain::NatureDomain::Hunting,
            v13::Nature::Conservation => signal_domain::NatureDomain::Conservation,
            v13::Nature::Weather => signal_domain::NatureDomain::Weather,
            v13::Nature::Wilderness => signal_domain::NatureDomain::Wilderness,
            v13::Nature::Sustainability => signal_domain::NatureDomain::Sustainability,
            v13::Nature::Resources => signal_domain::NatureDomain::Resources,
            v13::Nature::Stewardship => signal_domain::NatureDomain::Stewardship,
        }
    }

    pub fn travel(value: v13::Travel) -> signal_domain::TravelDomain {
        match value {
            v13::Travel::Itinerary => signal_domain::TravelDomain::Itinerary,
            v13::Travel::Destination => signal_domain::TravelDomain::Destination,
            v13::Travel::Transportation => signal_domain::TravelDomain::Transportation,
            v13::Travel::Driving => signal_domain::TravelDomain::Driving,
            v13::Travel::Navigation => signal_domain::TravelDomain::Navigation,
            v13::Travel::Commuting => signal_domain::TravelDomain::Commuting,
            v13::Travel::Logistics => signal_domain::TravelDomain::Logistics,
            v13::Travel::Migration => signal_domain::TravelDomain::Migration,
            v13::Travel::Tourism => signal_domain::TravelDomain::Tourism,
            v13::Travel::Transit => signal_domain::TravelDomain::Transit,
            v13::Travel::Cycling => signal_domain::TravelDomain::Cycling,
        }
    }

    pub fn commerce(value: v13::Commerce) -> signal_domain::CommerceDomain {
        match value {
            v13::Commerce::Selling => signal_domain::CommerceDomain::Selling,
            v13::Commerce::Buying => signal_domain::CommerceDomain::Buying,
            v13::Commerce::Marketing => signal_domain::CommerceDomain::Marketing,
            v13::Commerce::Retail => signal_domain::CommerceDomain::Retail,
            v13::Commerce::Sourcing => signal_domain::CommerceDomain::Sourcing,
            v13::Commerce::Trade => signal_domain::CommerceDomain::Trade,
            v13::Commerce::Support => signal_domain::CommerceDomain::Support,
            v13::Commerce::Pricing => signal_domain::CommerceDomain::Pricing,
            v13::Commerce::Negotiation => signal_domain::CommerceDomain::Negotiation,
            v13::Commerce::Assets => signal_domain::CommerceDomain::Assets,
            v13::Commerce::Market => signal_domain::CommerceDomain::Market,
        }
    }

    pub fn leisure(value: v13::Leisure) -> signal_domain::LeisureDomain {
        match value {
            v13::Leisure::Recreation => signal_domain::LeisureDomain::Recreation,
            v13::Leisure::Sport => signal_domain::LeisureDomain::Sport,
            v13::Leisure::Games => signal_domain::LeisureDomain::Games,
            v13::Leisure::Hobby => signal_domain::LeisureDomain::Hobby,
            v13::Leisure::Entertainment => signal_domain::LeisureDomain::Entertainment,
            v13::Leisure::Collecting => signal_domain::LeisureDomain::Collecting,
            v13::Leisure::Outdoors => signal_domain::LeisureDomain::Outdoors,
            v13::Leisure::Play => signal_domain::LeisureDomain::Play,
            v13::Leisure::Relaxation => signal_domain::LeisureDomain::Relaxation,
            v13::Leisure::Celebration => signal_domain::LeisureDomain::Celebration,
            v13::Leisure::Fandom => signal_domain::LeisureDomain::Fandom,
        }
    }

    pub fn appearance(value: v13::Appearance) -> signal_domain::AppearanceDomain {
        match value {
            v13::Appearance::Clothing => signal_domain::AppearanceDomain::Clothing,
            v13::Appearance::Grooming => signal_domain::AppearanceDomain::Grooming,
            v13::Appearance::Style => signal_domain::AppearanceDomain::Style,
            v13::Appearance::Cosmetics => signal_domain::AppearanceDomain::Cosmetics,
            v13::Appearance::Etiquette => signal_domain::AppearanceDomain::Etiquette,
            v13::Appearance::Comportment => signal_domain::AppearanceDomain::Comportment,
        }
    }

    pub fn safety(value: v13::Safety) -> signal_domain::SafetyDomain {
        match value {
            v13::Safety::Protection => signal_domain::SafetyDomain::Protection,
            v13::Safety::Preparedness => signal_domain::SafetyDomain::Preparedness,
            v13::Safety::Risk => signal_domain::SafetyDomain::Risk,
            v13::Safety::Cybersecurity => signal_domain::SafetyDomain::Cybersecurity,
            v13::Safety::Privacy => signal_domain::SafetyDomain::Privacy,
            v13::Safety::Disaster => signal_domain::SafetyDomain::Disaster,
            v13::Safety::Military => signal_domain::SafetyDomain::Military,
            v13::Safety::Deterrence => signal_domain::SafetyDomain::Deterrence,
        }
    }

    pub fn information(value: v13::Information) -> signal_domain::InformationDomain {
        match value {
            v13::Information::Curation => signal_domain::InformationDomain::Curation,
            v13::Information::RecordKeeping => signal_domain::InformationDomain::RecordKeeping,
            v13::Information::Documentation => signal_domain::InformationDomain::Documentation,
            v13::Information::News => signal_domain::InformationDomain::News,
            v13::Information::Broadcasting => signal_domain::InformationDomain::Broadcasting,
            v13::Information::Archives => signal_domain::InformationDomain::Archives,
            v13::Information::Database => signal_domain::InformationDomain::Database,
            v13::Information::Retrieval => signal_domain::InformationDomain::Retrieval,
            v13::Information::Classification => signal_domain::InformationDomain::Classification,
        }
    }

    pub fn hardware_leaf(value: v13::HardwareLeaf) -> signal_domain::HardwareLeaf {
        match value {
            v13::HardwareLeaf::All => signal_domain::HardwareLeaf::All,
            v13::HardwareLeaf::Networking => signal_domain::HardwareLeaf::Networking,
        }
    }

    pub fn programming_leaf(value: v13::ProgrammingLeaf) -> signal_domain::ProgrammingLeaf {
        match value {
            v13::ProgrammingLeaf::All => signal_domain::ProgrammingLeaf::All,
            v13::ProgrammingLeaf::TypeSystems => signal_domain::ProgrammingLeaf::TypeSystems,
            v13::ProgrammingLeaf::Compilation => signal_domain::ProgrammingLeaf::Compilation,
            v13::ProgrammingLeaf::Parsing => signal_domain::ProgrammingLeaf::Parsing,
            v13::ProgrammingLeaf::Grammars => signal_domain::ProgrammingLeaf::Grammars,
            v13::ProgrammingLeaf::CodeGeneration => signal_domain::ProgrammingLeaf::CodeGeneration,
            v13::ProgrammingLeaf::Metaprogramming => {
                signal_domain::ProgrammingLeaf::Metaprogramming
            }
            v13::ProgrammingLeaf::Macros => signal_domain::ProgrammingLeaf::Macros,
            v13::ProgrammingLeaf::DomainSpecificLanguages => {
                signal_domain::ProgrammingLeaf::DomainSpecificLanguages
            }
        }
    }

    pub fn systems_leaf(value: v13::SystemsLeaf) -> signal_domain::SystemsLeaf {
        match value {
            v13::SystemsLeaf::All => signal_domain::SystemsLeaf::All,
            v13::SystemsLeaf::SystemsProgramming => signal_domain::SystemsLeaf::SystemsProgramming,
            v13::SystemsLeaf::Concurrency => signal_domain::SystemsLeaf::Concurrency,
        }
    }

    pub fn distributed_leaf(value: v13::DistributedLeaf) -> signal_domain::DistributedLeaf {
        match value {
            v13::DistributedLeaf::All => signal_domain::DistributedLeaf::All,
            v13::DistributedLeaf::ProtocolDesign => signal_domain::DistributedLeaf::ProtocolDesign,
            v13::DistributedLeaf::EventDrivenArchitecture => {
                signal_domain::DistributedLeaf::EventDrivenArchitecture
            }
        }
    }

    pub fn data_leaf(value: v13::DataLeaf) -> signal_domain::DataLeaf {
        match value {
            v13::DataLeaf::All => signal_domain::DataLeaf::All,
            v13::DataLeaf::Persistence => signal_domain::DataLeaf::Persistence,
            v13::DataLeaf::Serialization => signal_domain::DataLeaf::Serialization,
            v13::DataLeaf::Formats => signal_domain::DataLeaf::Formats,
            v13::DataLeaf::Modeling => signal_domain::DataLeaf::Modeling,
            v13::DataLeaf::SchemaEvolution => signal_domain::DataLeaf::SchemaEvolution,
            v13::DataLeaf::Migration => signal_domain::DataLeaf::Migration,
        }
    }

    pub fn intelligence_leaf(value: v13::IntelligenceLeaf) -> signal_domain::IntelligenceLeaf {
        match value {
            v13::IntelligenceLeaf::All => signal_domain::IntelligenceLeaf::All,
            v13::IntelligenceLeaf::AgentSystems => signal_domain::IntelligenceLeaf::AgentSystems,
        }
    }

    pub fn security_leaf(value: v13::SecurityLeaf) -> signal_domain::SecurityLeaf {
        match value {
            v13::SecurityLeaf::All => signal_domain::SecurityLeaf::All,
            v13::SecurityLeaf::Cryptography => signal_domain::SecurityLeaf::Cryptography,
            v13::SecurityLeaf::Authentication => signal_domain::SecurityLeaf::Authentication,
            v13::SecurityLeaf::Authorization => signal_domain::SecurityLeaf::Authorization,
            v13::SecurityLeaf::SecretsManagement => signal_domain::SecurityLeaf::SecretsManagement,
            v13::SecurityLeaf::Privacy => signal_domain::SecurityLeaf::Privacy,
        }
    }

    pub fn quality_leaf(value: v13::QualityLeaf) -> signal_domain::QualityLeaf {
        match value {
            v13::QualityLeaf::All => signal_domain::QualityLeaf::All,
            v13::QualityLeaf::Testing => signal_domain::QualityLeaf::Testing,
        }
    }

    pub fn operations_leaf(value: v13::OperationsLeaf) -> signal_domain::OperationsLeaf {
        match value {
            v13::OperationsLeaf::All => signal_domain::OperationsLeaf::All,
            v13::OperationsLeaf::BuildSystem => signal_domain::OperationsLeaf::BuildSystem,
            v13::OperationsLeaf::ReleaseEngineering => {
                signal_domain::OperationsLeaf::ReleaseEngineering
            }
            v13::OperationsLeaf::DependencyManagement => {
                signal_domain::OperationsLeaf::DependencyManagement
            }
            v13::OperationsLeaf::Deployment => signal_domain::OperationsLeaf::Deployment,
            v13::OperationsLeaf::ConfigurationManagement => {
                signal_domain::OperationsLeaf::ConfigurationManagement
            }
        }
    }

    pub fn observability_leaf(value: v13::ObservabilityLeaf) -> signal_domain::ObservabilityLeaf {
        match value {
            v13::ObservabilityLeaf::All => signal_domain::ObservabilityLeaf::All,
            v13::ObservabilityLeaf::Tracing => signal_domain::ObservabilityLeaf::Tracing,
        }
    }

    pub fn surfaces_leaf(value: v13::SurfacesLeaf) -> signal_domain::SurfacesLeaf {
        match value {
            v13::SurfacesLeaf::All => signal_domain::SurfacesLeaf::All,
            v13::SurfacesLeaf::Visualization => signal_domain::SurfacesLeaf::Visualization,
            v13::SurfacesLeaf::CommandLineInterfaces => {
                signal_domain::SurfacesLeaf::CommandLineInterfaces
            }
        }
    }

    pub fn engineering_leaf(value: v13::EngineeringLeaf) -> signal_domain::EngineeringLeaf {
        match value {
            v13::EngineeringLeaf::All => signal_domain::EngineeringLeaf::All,
            v13::EngineeringLeaf::Architecture => signal_domain::EngineeringLeaf::Architecture,
            v13::EngineeringLeaf::Design => signal_domain::EngineeringLeaf::Design,
            v13::EngineeringLeaf::ApplicationProgrammingInterfaces => {
                signal_domain::EngineeringLeaf::ApplicationProgrammingInterfaces
            }
            v13::EngineeringLeaf::Documentation => signal_domain::EngineeringLeaf::Documentation,
            v13::EngineeringLeaf::VersionControl => signal_domain::EngineeringLeaf::VersionControl,
            v13::EngineeringLeaf::DevelopmentProcess => {
                signal_domain::EngineeringLeaf::DevelopmentProcess
            }
            v13::EngineeringLeaf::Management => signal_domain::EngineeringLeaf::Management,
            v13::EngineeringLeaf::Modularity => signal_domain::EngineeringLeaf::Modularity,
        }
    }

    pub fn kind(value: v13::Kind) -> signal_spirit::Kind {
        match value {
            v13::Kind::Decision => signal_spirit::Kind::Decision,
            v13::Kind::Principle => signal_spirit::Kind::Principle,
            v13::Kind::Correction => signal_spirit::Kind::Correction,
            v13::Kind::Clarification => signal_spirit::Kind::Clarification,
            v13::Kind::Constraint => signal_spirit::Kind::Constraint,
        }
    }

    pub fn magnitude(value: v13::Magnitude) -> signal_spirit::Magnitude {
        match value {
            v13::Magnitude::Zero => signal_spirit::Magnitude::Zero,
            v13::Magnitude::Minimum => signal_spirit::Magnitude::Minimum,
            v13::Magnitude::VeryLow => signal_spirit::Magnitude::VeryLow,
            v13::Magnitude::Low => signal_spirit::Magnitude::Low,
            v13::Magnitude::Medium => signal_spirit::Magnitude::Medium,
            v13::Magnitude::High => signal_spirit::Magnitude::High,
            v13::Magnitude::VeryHigh => signal_spirit::Magnitude::VeryHigh,
            v13::Magnitude::Maximum => signal_spirit::Magnitude::Maximum,
        }
    }

    pub fn domain(value: v13::Domain) -> signal_domain::Domain {
        match value {
            v13::Domain::All => signal_domain::Domain::All,
            v13::Domain::Health(value) => signal_domain::Domain::Health(health(value)),
            v13::Domain::Food(value) => signal_domain::Domain::Food(food(value)),
            v13::Domain::Home(value) => signal_domain::Domain::Home(home(value)),
            v13::Domain::Finance(value) => signal_domain::Domain::Finance(finance(value)),
            v13::Domain::Work(value) => signal_domain::Domain::Work(work(value)),
            v13::Domain::Craft(value) => signal_domain::Domain::Craft(craft(value)),
            v13::Domain::Knowledge(value) => signal_domain::Domain::Knowledge(knowledge(value)),
            v13::Domain::Education(value) => signal_domain::Domain::Education(education(value)),
            v13::Domain::Language(value) => signal_domain::Domain::Language(language(value)),
            v13::Domain::Art(value) => signal_domain::Domain::Art(art(value)),
            v13::Domain::Kinship(value) => signal_domain::Domain::Kinship(kinship(value)),
            v13::Domain::Selfhood(value) => signal_domain::Domain::Selfhood(selfhood(value)),
            v13::Domain::Spirituality(value) => {
                signal_domain::Domain::Spirituality(spirituality(value))
            }
            v13::Domain::Governance(value) => signal_domain::Domain::Governance(governance(value)),
            v13::Domain::Law(value) => signal_domain::Domain::Law(law(value)),
            v13::Domain::Community(value) => signal_domain::Domain::Community(community(value)),
            v13::Domain::Nature(value) => signal_domain::Domain::Nature(nature(value)),
            v13::Domain::Travel(value) => signal_domain::Domain::Travel(travel(value)),
            v13::Domain::Commerce(value) => signal_domain::Domain::Commerce(commerce(value)),
            v13::Domain::Leisure(value) => signal_domain::Domain::Leisure(leisure(value)),
            v13::Domain::Appearance(value) => signal_domain::Domain::Appearance(appearance(value)),
            v13::Domain::Safety(value) => signal_domain::Domain::Safety(safety(value)),
            v13::Domain::Information(value) => {
                signal_domain::Domain::Information(information(value))
            }
            v13::Domain::Technology(value) => signal_domain::Domain::Technology(technology(value)),
        }
    }

    pub fn technology(value: v13::Technology) -> signal_domain::TechnologyDomain {
        match value {
            v13::Technology::Hardware(value) => {
                signal_domain::TechnologyDomain::Hardware(hardware_leaf(value))
            }
            v13::Technology::Software(value) => {
                signal_domain::TechnologyDomain::Software(software(value))
            }
        }
    }

    pub fn software(value: v13::Software) -> signal_domain::SoftwareDomain {
        match value {
            v13::Software::Programming(value) => {
                signal_domain::SoftwareDomain::Programming(programming_leaf(value))
            }
            v13::Software::Theory => signal_domain::SoftwareDomain::Theory,
            v13::Software::Systems(value) => {
                signal_domain::SoftwareDomain::Systems(systems_leaf(value))
            }
            v13::Software::Distributed(value) => {
                signal_domain::SoftwareDomain::Distributed(distributed_leaf(value))
            }
            v13::Software::Data(value) => signal_domain::SoftwareDomain::Data(data_leaf(value)),
            v13::Software::Intelligence(value) => {
                signal_domain::SoftwareDomain::Intelligence(intelligence_leaf(value))
            }
            v13::Software::Security(value) => {
                signal_domain::SoftwareDomain::Security(security_leaf(value))
            }
            v13::Software::Quality(value) => {
                signal_domain::SoftwareDomain::Quality(quality_leaf(value))
            }
            v13::Software::Operations(value) => {
                signal_domain::SoftwareDomain::Operations(operations_leaf(value))
            }
            v13::Software::Observability(value) => {
                signal_domain::SoftwareDomain::Observability(observability_leaf(value))
            }
            v13::Software::Surfaces(value) => {
                signal_domain::SoftwareDomain::Surfaces(surfaces_leaf(value))
            }
            v13::Software::Engineering(value) => {
                signal_domain::SoftwareDomain::Engineering(engineering_leaf(value))
            }
        }
    }
}

/// Exhaustive projection of the exact b37fc963 v14 taxonomy. The historical
/// signal-domain revision is frozen in `v14`; each branch intentionally names
/// its current equivalent instead of decoding through a text codec.
mod frozen_v14_projection {
    use super::v14;

    pub fn health(value: v14::Health) -> signal_domain::HealthDomain {
        match value {
            v14::Health::Body => signal_domain::HealthDomain::Body,
            v14::Health::Mind => signal_domain::HealthDomain::Mind,
            v14::Health::Nutrition => signal_domain::HealthDomain::Nutrition,
            v14::Health::Exercise => signal_domain::HealthDomain::Exercise,
            v14::Health::Sleep => signal_domain::HealthDomain::Sleep,
            v14::Health::Medicine => signal_domain::HealthDomain::Medicine,
            v14::Health::Disease => signal_domain::HealthDomain::Disease,
            v14::Health::Medication => signal_domain::HealthDomain::Medication,
            v14::Health::Therapy => signal_domain::HealthDomain::Therapy,
            v14::Health::Reproduction => signal_domain::HealthDomain::Reproduction,
            v14::Health::Sexuality => signal_domain::HealthDomain::Sexuality,
            v14::Health::Aging => signal_domain::HealthDomain::Aging,
            v14::Health::Disability => signal_domain::HealthDomain::Disability,
            v14::Health::Addiction => signal_domain::HealthDomain::Addiction,
            v14::Health::Dentistry => signal_domain::HealthDomain::Dentistry,
            v14::Health::Senses => signal_domain::HealthDomain::Senses,
            v14::Health::Pain => signal_domain::HealthDomain::Pain,
            v14::Health::Prevention => signal_domain::HealthDomain::Prevention,
            v14::Health::FirstAid => signal_domain::HealthDomain::FirstAid,
            v14::Health::Rehabilitation => signal_domain::HealthDomain::Rehabilitation,
        }
    }

    pub fn food(value: v14::Food) -> signal_domain::FoodDomain {
        match value {
            v14::Food::Cooking => signal_domain::FoodDomain::Cooking,
            v14::Food::Diet => signal_domain::FoodDomain::Diet,
            v14::Food::Recipe => signal_domain::FoodDomain::Recipe,
            v14::Food::Baking => signal_domain::FoodDomain::Baking,
            v14::Food::Preservation => signal_domain::FoodDomain::Preservation,
            v14::Food::Fermentation => signal_domain::FoodDomain::Fermentation,
            v14::Food::Beverage => signal_domain::FoodDomain::Beverage,
            v14::Food::Entertaining => signal_domain::FoodDomain::Entertaining,
            v14::Food::Foraging => signal_domain::FoodDomain::Foraging,
            v14::Food::Fasting => signal_domain::FoodDomain::Fasting,
            v14::Food::Dining => signal_domain::FoodDomain::Dining,
        }
    }

    pub fn home(value: v14::Home) -> signal_domain::HomeDomain {
        match value {
            v14::Home::Housing => signal_domain::HomeDomain::Housing,
            v14::Home::Maintenance => signal_domain::HomeDomain::Maintenance,
            v14::Home::Renovation => signal_domain::HomeDomain::Renovation,
            v14::Home::Furnishing => signal_domain::HomeDomain::Furnishing,
            v14::Home::Cleaning => signal_domain::HomeDomain::Cleaning,
            v14::Home::Tidying => signal_domain::HomeDomain::Tidying,
            v14::Home::Relocation => signal_domain::HomeDomain::Relocation,
            v14::Home::Realty => signal_domain::HomeDomain::Realty,
            v14::Home::Property => signal_domain::HomeDomain::Property,
            v14::Home::Utilities => signal_domain::HomeDomain::Utilities,
            v14::Home::Locksmithing => signal_domain::HomeDomain::Locksmithing,
            v14::Home::Appliances => signal_domain::HomeDomain::Appliances,
        }
    }

    pub fn finance(value: v14::Finance) -> signal_domain::FinanceDomain {
        match value {
            v14::Finance::Budgeting => signal_domain::FinanceDomain::Budgeting,
            v14::Finance::Saving => signal_domain::FinanceDomain::Saving,
            v14::Finance::Spending => signal_domain::FinanceDomain::Spending,
            v14::Finance::Debt => signal_domain::FinanceDomain::Debt,
            v14::Finance::Credit => signal_domain::FinanceDomain::Credit,
            v14::Finance::Investing => signal_domain::FinanceDomain::Investing,
            v14::Finance::Retirement => signal_domain::FinanceDomain::Retirement,
            v14::Finance::Tax => signal_domain::FinanceDomain::Tax,
            v14::Finance::Insurance => signal_domain::FinanceDomain::Insurance,
            v14::Finance::Income => signal_domain::FinanceDomain::Income,
            v14::Finance::Banking => signal_domain::FinanceDomain::Banking,
            v14::Finance::Charity => signal_domain::FinanceDomain::Charity,
            v14::Finance::Planning => signal_domain::FinanceDomain::Planning,
            v14::Finance::Accounting => signal_domain::FinanceDomain::Accounting,
        }
    }

    pub fn work(value: v14::Work) -> signal_domain::WorkDomain {
        match value {
            v14::Work::Career => signal_domain::WorkDomain::Career,
            v14::Work::JobSearch => signal_domain::WorkDomain::JobSearch,
            v14::Work::Workplace => signal_domain::WorkDomain::Workplace,
            v14::Work::Vocation => signal_domain::WorkDomain::Vocation,
            v14::Work::Leadership => signal_domain::WorkDomain::Leadership,
            v14::Work::Entrepreneurship => signal_domain::WorkDomain::Entrepreneurship,
            v14::Work::Employment => signal_domain::WorkDomain::Employment,
            v14::Work::Compensation => signal_domain::WorkDomain::Compensation,
            v14::Work::Scheduling => signal_domain::WorkDomain::Scheduling,
            v14::Work::Unemployment => signal_domain::WorkDomain::Unemployment,
            v14::Work::Freelancing => signal_domain::WorkDomain::Freelancing,
            v14::Work::Teamwork => signal_domain::WorkDomain::Teamwork,
            v14::Work::Productivity => signal_domain::WorkDomain::Productivity,
            v14::Work::Project => signal_domain::WorkDomain::Project,
        }
    }

    pub fn craft(value: v14::Craft) -> signal_domain::CraftDomain {
        match value {
            v14::Craft::Electronics => signal_domain::CraftDomain::Electronics,
            v14::Craft::Construction => signal_domain::CraftDomain::Construction,
            v14::Craft::Carpentry => signal_domain::CraftDomain::Carpentry,
            v14::Craft::Metalworking => signal_domain::CraftDomain::Metalworking,
            v14::Craft::Sewing => signal_domain::CraftDomain::Sewing,
            v14::Craft::Manufacturing => signal_domain::CraftDomain::Manufacturing,
            v14::Craft::Repair => signal_domain::CraftDomain::Repair,
            v14::Craft::Engineering => signal_domain::CraftDomain::Engineering,
            v14::Craft::Handicraft => signal_domain::CraftDomain::Handicraft,
            v14::Craft::Invention => signal_domain::CraftDomain::Invention,
        }
    }

    pub fn knowledge(value: v14::Knowledge) -> signal_domain::KnowledgeDomain {
        match value {
            v14::Knowledge::Mathematics => signal_domain::KnowledgeDomain::Mathematics,
            v14::Knowledge::Logic => signal_domain::KnowledgeDomain::Logic,
            v14::Knowledge::Physics => signal_domain::KnowledgeDomain::Physics,
            v14::Knowledge::Chemistry => signal_domain::KnowledgeDomain::Chemistry,
            v14::Knowledge::Biology => signal_domain::KnowledgeDomain::Biology,
            v14::Knowledge::Astronomy => signal_domain::KnowledgeDomain::Astronomy,
            v14::Knowledge::Geology => signal_domain::KnowledgeDomain::Geology,
            v14::Knowledge::Computing => signal_domain::KnowledgeDomain::Computing,
            v14::Knowledge::Physiology => signal_domain::KnowledgeDomain::Physiology,
            v14::Knowledge::Statistics => signal_domain::KnowledgeDomain::Statistics,
            v14::Knowledge::Research => signal_domain::KnowledgeDomain::Research,
            v14::Knowledge::History => signal_domain::KnowledgeDomain::History,
            v14::Knowledge::Linguistics => signal_domain::KnowledgeDomain::Linguistics,
            v14::Knowledge::Philosophy => signal_domain::KnowledgeDomain::Philosophy,
            v14::Knowledge::Economics => signal_domain::KnowledgeDomain::Economics,
            v14::Knowledge::Cognition => signal_domain::KnowledgeDomain::Cognition,
            v14::Knowledge::Taxonomy => signal_domain::KnowledgeDomain::Taxonomy,
        }
    }

    pub fn education(value: v14::Education) -> signal_domain::EducationDomain {
        match value {
            v14::Education::Studying => signal_domain::EducationDomain::Studying,
            v14::Education::Teaching => signal_domain::EducationDomain::Teaching,
            v14::Education::Schooling => signal_domain::EducationDomain::Schooling,
            v14::Education::Skill => signal_domain::EducationDomain::Skill,
            v14::Education::Reading => signal_domain::EducationDomain::Reading,
            v14::Education::Memorization => signal_domain::EducationDomain::Memorization,
            v14::Education::Pedagogy => signal_domain::EducationDomain::Pedagogy,
            v14::Education::Mentoring => signal_domain::EducationDomain::Mentoring,
            v14::Education::Autodidacticism => signal_domain::EducationDomain::Autodidacticism,
            v14::Education::Credential => signal_domain::EducationDomain::Credential,
        }
    }

    pub fn language(value: v14::Language) -> signal_domain::LanguageDomain {
        match value {
            v14::Language::Writing => signal_domain::LanguageDomain::Writing,
            v14::Language::Rhetoric => signal_domain::LanguageDomain::Rhetoric,
            v14::Language::Translation => signal_domain::LanguageDomain::Translation,
            v14::Language::Grammar => signal_domain::LanguageDomain::Grammar,
            v14::Language::Conversation => signal_domain::LanguageDomain::Conversation,
            v14::Language::Correspondence => signal_domain::LanguageDomain::Correspondence,
            v14::Language::Listening => signal_domain::LanguageDomain::Listening,
            v14::Language::Oratory => signal_domain::LanguageDomain::Oratory,
            v14::Language::Editing => signal_domain::LanguageDomain::Editing,
            v14::Language::Terminology => signal_domain::LanguageDomain::Terminology,
            v14::Language::Notation => signal_domain::LanguageDomain::Notation,
        }
    }

    pub fn art(value: v14::Art) -> signal_domain::ArtDomain {
        match value {
            v14::Art::Fiction => signal_domain::ArtDomain::Fiction,
            v14::Art::Poetry => signal_domain::ArtDomain::Poetry,
            v14::Art::Music => signal_domain::ArtDomain::Music,
            v14::Art::Painting => signal_domain::ArtDomain::Painting,
            v14::Art::Photography => signal_domain::ArtDomain::Photography,
            v14::Art::Film => signal_domain::ArtDomain::Film,
            v14::Art::Theater => signal_domain::ArtDomain::Theater,
            v14::Art::Dance => signal_domain::ArtDomain::Dance,
            v14::Art::Design => signal_domain::ArtDomain::Design,
            v14::Art::Sculpture => signal_domain::ArtDomain::Sculpture,
            v14::Art::Creativity => signal_domain::ArtDomain::Creativity,
            v14::Art::Storytelling => signal_domain::ArtDomain::Storytelling,
            v14::Art::Publishing => signal_domain::ArtDomain::Publishing,
        }
    }

    pub fn kinship(value: v14::Kinship) -> signal_domain::KinshipDomain {
        match value {
            v14::Kinship::Friendship => signal_domain::KinshipDomain::Friendship,
            v14::Kinship::Romance => signal_domain::KinshipDomain::Romance,
            v14::Kinship::Marriage => signal_domain::KinshipDomain::Marriage,
            v14::Kinship::Family => signal_domain::KinshipDomain::Family,
            v14::Kinship::Parenting => signal_domain::KinshipDomain::Parenting,
            v14::Kinship::Relatives => signal_domain::KinshipDomain::Relatives,
            v14::Kinship::Reconciliation => signal_domain::KinshipDomain::Reconciliation,
            v14::Kinship::Boundaries => signal_domain::KinshipDomain::Boundaries,
            v14::Kinship::Intimacy => signal_domain::KinshipDomain::Intimacy,
            v14::Kinship::Rapport => signal_domain::KinshipDomain::Rapport,
            v14::Kinship::Caregiving => signal_domain::KinshipDomain::Caregiving,
            v14::Kinship::Grief => signal_domain::KinshipDomain::Grief,
            v14::Kinship::Belonging => signal_domain::KinshipDomain::Belonging,
        }
    }

    pub fn selfhood(value: v14::Selfhood) -> signal_domain::SelfhoodDomain {
        match value {
            v14::Selfhood::Growth => signal_domain::SelfhoodDomain::Growth,
            v14::Selfhood::Introspection => signal_domain::SelfhoodDomain::Introspection,
            v14::Selfhood::Discipline => signal_domain::SelfhoodDomain::Discipline,
            v14::Selfhood::Emotion => signal_domain::SelfhoodDomain::Emotion,
            v14::Selfhood::Virtue => signal_domain::SelfhoodDomain::Virtue,
            v14::Selfhood::Motivation => signal_domain::SelfhoodDomain::Motivation,
            v14::Selfhood::Confidence => signal_domain::SelfhoodDomain::Confidence,
            v14::Selfhood::Identity => signal_domain::SelfhoodDomain::Identity,
            v14::Selfhood::Purpose => signal_domain::SelfhoodDomain::Purpose,
            v14::Selfhood::Decision => signal_domain::SelfhoodDomain::Decision,
            v14::Selfhood::Temperament => signal_domain::SelfhoodDomain::Temperament,
            v14::Selfhood::Wellbeing => signal_domain::SelfhoodDomain::Wellbeing,
            v14::Selfhood::Composure => signal_domain::SelfhoodDomain::Composure,
        }
    }

    pub fn spirituality(value: v14::Spirituality) -> signal_domain::SpiritualityDomain {
        match value {
            v14::Spirituality::Worship => signal_domain::SpiritualityDomain::Worship,
            v14::Spirituality::Prayer => signal_domain::SpiritualityDomain::Prayer,
            v14::Spirituality::Meditation => signal_domain::SpiritualityDomain::Meditation,
            v14::Spirituality::Ritual => signal_domain::SpiritualityDomain::Ritual,
            v14::Spirituality::Faith => signal_domain::SpiritualityDomain::Faith,
            v14::Spirituality::Theology => signal_domain::SpiritualityDomain::Theology,
            v14::Spirituality::Contemplation => signal_domain::SpiritualityDomain::Contemplation,
            v14::Spirituality::Pilgrimage => signal_domain::SpiritualityDomain::Pilgrimage,
            v14::Spirituality::Scripture => signal_domain::SpiritualityDomain::Scripture,
            v14::Spirituality::Ethics => signal_domain::SpiritualityDomain::Ethics,
            v14::Spirituality::Mortality => signal_domain::SpiritualityDomain::Mortality,
            v14::Spirituality::Transcendence => signal_domain::SpiritualityDomain::Transcendence,
            v14::Spirituality::Asceticism => signal_domain::SpiritualityDomain::Asceticism,
            v14::Spirituality::Wisdom => signal_domain::SpiritualityDomain::Wisdom,
        }
    }

    pub fn governance(value: v14::Governance) -> signal_domain::GovernanceDomain {
        match value {
            v14::Governance::Politics => signal_domain::GovernanceDomain::Politics,
            v14::Governance::Government => signal_domain::GovernanceDomain::Government,
            v14::Governance::Administration => signal_domain::GovernanceDomain::Administration,
            v14::Governance::Citizenship => signal_domain::GovernanceDomain::Citizenship,
            v14::Governance::Elections => signal_domain::GovernanceDomain::Elections,
            v14::Governance::Activism => signal_domain::GovernanceDomain::Activism,
            v14::Governance::Policy => signal_domain::GovernanceDomain::Policy,
            v14::Governance::Diplomacy => signal_domain::GovernanceDomain::Diplomacy,
            v14::Governance::Movements => signal_domain::GovernanceDomain::Movements,
            v14::Governance::Organizing => signal_domain::GovernanceDomain::Organizing,
            v14::Governance::Services => signal_domain::GovernanceDomain::Services,
            v14::Governance::Naturalization => signal_domain::GovernanceDomain::Naturalization,
            v14::Governance::War => signal_domain::GovernanceDomain::War,
        }
    }

    pub fn law(value: v14::Law) -> signal_domain::LawDomain {
        match value {
            v14::Law::Rights => signal_domain::LawDomain::Rights,
            v14::Law::Contract => signal_domain::LawDomain::Contract,
            v14::Law::Title => signal_domain::LawDomain::Title,
            v14::Law::Crime => signal_domain::LawDomain::Crime,
            v14::Law::Litigation => signal_domain::LawDomain::Litigation,
            v14::Law::Compliance => signal_domain::LawDomain::Compliance,
            v14::Law::Custody => signal_domain::LawDomain::Custody,
            v14::Law::Liability => signal_domain::LawDomain::Liability,
            v14::Law::Procedure => signal_domain::LawDomain::Procedure,
            v14::Law::Justice => signal_domain::LawDomain::Justice,
            v14::Law::Policing => signal_domain::LawDomain::Policing,
            v14::Law::Arbitration => signal_domain::LawDomain::Arbitration,
        }
    }

    pub fn community(value: v14::Community) -> signal_domain::CommunityDomain {
        match value {
            v14::Community::Neighborliness => signal_domain::CommunityDomain::Neighborliness,
            v14::Community::Volunteering => signal_domain::CommunityDomain::Volunteering,
            v14::Community::Solidarity => signal_domain::CommunityDomain::Solidarity,
            v14::Community::Membership => signal_domain::CommunityDomain::Membership,
            v14::Community::Gatherings => signal_domain::CommunityDomain::Gatherings,
            v14::Community::Reputation => signal_domain::CommunityDomain::Reputation,
            v14::Community::Service => signal_domain::CommunityDomain::Service,
            v14::Community::Hospitality => signal_domain::CommunityDomain::Hospitality,
            v14::Community::Institutions => signal_domain::CommunityDomain::Institutions,
        }
    }

    pub fn nature(value: v14::Nature) -> signal_domain::NatureDomain {
        match value {
            v14::Nature::Agriculture => signal_domain::NatureDomain::Agriculture,
            v14::Nature::Gardening => signal_domain::NatureDomain::Gardening,
            v14::Nature::Horticulture => signal_domain::NatureDomain::Horticulture,
            v14::Nature::Husbandry => signal_domain::NatureDomain::Husbandry,
            v14::Nature::Pets => signal_domain::NatureDomain::Pets,
            v14::Nature::Forestry => signal_domain::NatureDomain::Forestry,
            v14::Nature::Fishing => signal_domain::NatureDomain::Fishing,
            v14::Nature::Hunting => signal_domain::NatureDomain::Hunting,
            v14::Nature::Conservation => signal_domain::NatureDomain::Conservation,
            v14::Nature::Weather => signal_domain::NatureDomain::Weather,
            v14::Nature::Wilderness => signal_domain::NatureDomain::Wilderness,
            v14::Nature::Sustainability => signal_domain::NatureDomain::Sustainability,
            v14::Nature::Resources => signal_domain::NatureDomain::Resources,
            v14::Nature::Stewardship => signal_domain::NatureDomain::Stewardship,
        }
    }

    pub fn travel(value: v14::Travel) -> signal_domain::TravelDomain {
        match value {
            v14::Travel::Itinerary => signal_domain::TravelDomain::Itinerary,
            v14::Travel::Destination => signal_domain::TravelDomain::Destination,
            v14::Travel::Transportation => signal_domain::TravelDomain::Transportation,
            v14::Travel::Driving => signal_domain::TravelDomain::Driving,
            v14::Travel::Navigation => signal_domain::TravelDomain::Navigation,
            v14::Travel::Commuting => signal_domain::TravelDomain::Commuting,
            v14::Travel::Logistics => signal_domain::TravelDomain::Logistics,
            v14::Travel::Migration => signal_domain::TravelDomain::Migration,
            v14::Travel::Tourism => signal_domain::TravelDomain::Tourism,
            v14::Travel::Transit => signal_domain::TravelDomain::Transit,
            v14::Travel::Cycling => signal_domain::TravelDomain::Cycling,
        }
    }

    pub fn commerce(value: v14::Commerce) -> signal_domain::CommerceDomain {
        match value {
            v14::Commerce::Selling => signal_domain::CommerceDomain::Selling,
            v14::Commerce::Buying => signal_domain::CommerceDomain::Buying,
            v14::Commerce::Marketing => signal_domain::CommerceDomain::Marketing,
            v14::Commerce::Retail => signal_domain::CommerceDomain::Retail,
            v14::Commerce::Sourcing => signal_domain::CommerceDomain::Sourcing,
            v14::Commerce::Trade => signal_domain::CommerceDomain::Trade,
            v14::Commerce::Support => signal_domain::CommerceDomain::Support,
            v14::Commerce::Pricing => signal_domain::CommerceDomain::Pricing,
            v14::Commerce::Negotiation => signal_domain::CommerceDomain::Negotiation,
            v14::Commerce::Assets => signal_domain::CommerceDomain::Assets,
            v14::Commerce::Market => signal_domain::CommerceDomain::Market,
        }
    }

    pub fn leisure(value: v14::Leisure) -> signal_domain::LeisureDomain {
        match value {
            v14::Leisure::Recreation => signal_domain::LeisureDomain::Recreation,
            v14::Leisure::Sport => signal_domain::LeisureDomain::Sport,
            v14::Leisure::Games => signal_domain::LeisureDomain::Games,
            v14::Leisure::Hobby => signal_domain::LeisureDomain::Hobby,
            v14::Leisure::Entertainment => signal_domain::LeisureDomain::Entertainment,
            v14::Leisure::Collecting => signal_domain::LeisureDomain::Collecting,
            v14::Leisure::Outdoors => signal_domain::LeisureDomain::Outdoors,
            v14::Leisure::Play => signal_domain::LeisureDomain::Play,
            v14::Leisure::Relaxation => signal_domain::LeisureDomain::Relaxation,
            v14::Leisure::Celebration => signal_domain::LeisureDomain::Celebration,
            v14::Leisure::Fandom => signal_domain::LeisureDomain::Fandom,
        }
    }

    pub fn appearance(value: v14::Appearance) -> signal_domain::AppearanceDomain {
        match value {
            v14::Appearance::Clothing => signal_domain::AppearanceDomain::Clothing,
            v14::Appearance::Grooming => signal_domain::AppearanceDomain::Grooming,
            v14::Appearance::Style => signal_domain::AppearanceDomain::Style,
            v14::Appearance::Cosmetics => signal_domain::AppearanceDomain::Cosmetics,
            v14::Appearance::Etiquette => signal_domain::AppearanceDomain::Etiquette,
            v14::Appearance::Comportment => signal_domain::AppearanceDomain::Comportment,
        }
    }

    pub fn safety(value: v14::Safety) -> signal_domain::SafetyDomain {
        match value {
            v14::Safety::Protection => signal_domain::SafetyDomain::Protection,
            v14::Safety::Preparedness => signal_domain::SafetyDomain::Preparedness,
            v14::Safety::Risk => signal_domain::SafetyDomain::Risk,
            v14::Safety::Cybersecurity => signal_domain::SafetyDomain::Cybersecurity,
            v14::Safety::Privacy => signal_domain::SafetyDomain::Privacy,
            v14::Safety::Disaster => signal_domain::SafetyDomain::Disaster,
            v14::Safety::Military => signal_domain::SafetyDomain::Military,
            v14::Safety::Deterrence => signal_domain::SafetyDomain::Deterrence,
        }
    }

    pub fn information(value: v14::Information) -> signal_domain::InformationDomain {
        match value {
            v14::Information::Curation => signal_domain::InformationDomain::Curation,
            v14::Information::RecordKeeping => signal_domain::InformationDomain::RecordKeeping,
            v14::Information::Documentation => signal_domain::InformationDomain::Documentation,
            v14::Information::News => signal_domain::InformationDomain::News,
            v14::Information::Broadcasting => signal_domain::InformationDomain::Broadcasting,
            v14::Information::Archives => signal_domain::InformationDomain::Archives,
            v14::Information::Database => signal_domain::InformationDomain::Database,
            v14::Information::Retrieval => signal_domain::InformationDomain::Retrieval,
            v14::Information::Classification => signal_domain::InformationDomain::Classification,
        }
    }

    pub fn hardware_leaf(value: v14::HardwareLeaf) -> signal_domain::HardwareLeaf {
        match value {
            v14::HardwareLeaf::All => signal_domain::HardwareLeaf::All,
            v14::HardwareLeaf::Networking => signal_domain::HardwareLeaf::Networking,
        }
    }

    pub fn programming_leaf(value: v14::ProgrammingLeaf) -> signal_domain::ProgrammingLeaf {
        match value {
            v14::ProgrammingLeaf::All => signal_domain::ProgrammingLeaf::All,
            v14::ProgrammingLeaf::TypeSystems => signal_domain::ProgrammingLeaf::TypeSystems,
            v14::ProgrammingLeaf::Compilation => signal_domain::ProgrammingLeaf::Compilation,
            v14::ProgrammingLeaf::Parsing => signal_domain::ProgrammingLeaf::Parsing,
            v14::ProgrammingLeaf::Grammars => signal_domain::ProgrammingLeaf::Grammars,
            v14::ProgrammingLeaf::CodeGeneration => signal_domain::ProgrammingLeaf::CodeGeneration,
            v14::ProgrammingLeaf::Metaprogramming => {
                signal_domain::ProgrammingLeaf::Metaprogramming
            }
            v14::ProgrammingLeaf::Macros => signal_domain::ProgrammingLeaf::Macros,
            v14::ProgrammingLeaf::DomainSpecificLanguages => {
                signal_domain::ProgrammingLeaf::DomainSpecificLanguages
            }
        }
    }

    pub fn systems_leaf(value: v14::SystemsLeaf) -> signal_domain::SystemsLeaf {
        match value {
            v14::SystemsLeaf::All => signal_domain::SystemsLeaf::All,
            v14::SystemsLeaf::SystemsProgramming => signal_domain::SystemsLeaf::SystemsProgramming,
            v14::SystemsLeaf::Concurrency => signal_domain::SystemsLeaf::Concurrency,
        }
    }

    pub fn distributed_leaf(value: v14::DistributedLeaf) -> signal_domain::DistributedLeaf {
        match value {
            v14::DistributedLeaf::All => signal_domain::DistributedLeaf::All,
            v14::DistributedLeaf::ProtocolDesign => signal_domain::DistributedLeaf::ProtocolDesign,
            v14::DistributedLeaf::EventDrivenArchitecture => {
                signal_domain::DistributedLeaf::EventDrivenArchitecture
            }
        }
    }

    pub fn data_leaf(value: v14::DataLeaf) -> signal_domain::DataLeaf {
        match value {
            v14::DataLeaf::All => signal_domain::DataLeaf::All,
            v14::DataLeaf::Persistence => signal_domain::DataLeaf::Persistence,
            v14::DataLeaf::Serialization => signal_domain::DataLeaf::Serialization,
            v14::DataLeaf::Formats => signal_domain::DataLeaf::Formats,
            v14::DataLeaf::Modeling => signal_domain::DataLeaf::Modeling,
            v14::DataLeaf::SchemaEvolution => signal_domain::DataLeaf::SchemaEvolution,
            v14::DataLeaf::Migration => signal_domain::DataLeaf::Migration,
        }
    }

    pub fn intelligence_leaf(value: v14::IntelligenceLeaf) -> signal_domain::IntelligenceLeaf {
        match value {
            v14::IntelligenceLeaf::All => signal_domain::IntelligenceLeaf::All,
            v14::IntelligenceLeaf::AgentSystems => signal_domain::IntelligenceLeaf::AgentSystems,
        }
    }

    pub fn security_leaf(value: v14::SecurityLeaf) -> signal_domain::SecurityLeaf {
        match value {
            v14::SecurityLeaf::All => signal_domain::SecurityLeaf::All,
            v14::SecurityLeaf::Cryptography => signal_domain::SecurityLeaf::Cryptography,
            v14::SecurityLeaf::Authentication => signal_domain::SecurityLeaf::Authentication,
            v14::SecurityLeaf::Authorization => signal_domain::SecurityLeaf::Authorization,
            v14::SecurityLeaf::SecretsManagement => signal_domain::SecurityLeaf::SecretsManagement,
            v14::SecurityLeaf::Privacy => signal_domain::SecurityLeaf::Privacy,
        }
    }

    pub fn quality_leaf(value: v14::QualityLeaf) -> signal_domain::QualityLeaf {
        match value {
            v14::QualityLeaf::All => signal_domain::QualityLeaf::All,
            v14::QualityLeaf::Testing => signal_domain::QualityLeaf::Testing,
        }
    }

    pub fn operations_leaf(value: v14::OperationsLeaf) -> signal_domain::OperationsLeaf {
        match value {
            v14::OperationsLeaf::All => signal_domain::OperationsLeaf::All,
            v14::OperationsLeaf::BuildSystem => signal_domain::OperationsLeaf::BuildSystem,
            v14::OperationsLeaf::ReleaseEngineering => {
                signal_domain::OperationsLeaf::ReleaseEngineering
            }
            v14::OperationsLeaf::DependencyManagement => {
                signal_domain::OperationsLeaf::DependencyManagement
            }
            v14::OperationsLeaf::Deployment => signal_domain::OperationsLeaf::Deployment,
            v14::OperationsLeaf::ConfigurationManagement => {
                signal_domain::OperationsLeaf::ConfigurationManagement
            }
        }
    }

    pub fn observability_leaf(value: v14::ObservabilityLeaf) -> signal_domain::ObservabilityLeaf {
        match value {
            v14::ObservabilityLeaf::All => signal_domain::ObservabilityLeaf::All,
            v14::ObservabilityLeaf::Tracing => signal_domain::ObservabilityLeaf::Tracing,
        }
    }

    pub fn surfaces_leaf(value: v14::SurfacesLeaf) -> signal_domain::SurfacesLeaf {
        match value {
            v14::SurfacesLeaf::All => signal_domain::SurfacesLeaf::All,
            v14::SurfacesLeaf::Visualization => signal_domain::SurfacesLeaf::Visualization,
            v14::SurfacesLeaf::CommandLineInterfaces => {
                signal_domain::SurfacesLeaf::CommandLineInterfaces
            }
        }
    }

    pub fn engineering_leaf(value: v14::EngineeringLeaf) -> signal_domain::EngineeringLeaf {
        match value {
            v14::EngineeringLeaf::All => signal_domain::EngineeringLeaf::All,
            v14::EngineeringLeaf::Architecture => signal_domain::EngineeringLeaf::Architecture,
            v14::EngineeringLeaf::Design => signal_domain::EngineeringLeaf::Design,
            v14::EngineeringLeaf::ApplicationProgrammingInterfaces => {
                signal_domain::EngineeringLeaf::ApplicationProgrammingInterfaces
            }
            v14::EngineeringLeaf::Documentation => signal_domain::EngineeringLeaf::Documentation,
            v14::EngineeringLeaf::VersionControl => signal_domain::EngineeringLeaf::VersionControl,
            v14::EngineeringLeaf::DevelopmentProcess => {
                signal_domain::EngineeringLeaf::DevelopmentProcess
            }
            v14::EngineeringLeaf::Management => signal_domain::EngineeringLeaf::Management,
            v14::EngineeringLeaf::Modularity => signal_domain::EngineeringLeaf::Modularity,
        }
    }

    pub fn kind(value: v14::Kind) -> signal_spirit::Kind {
        match value {
            v14::Kind::Decision => signal_spirit::Kind::Decision,
            v14::Kind::Principle => signal_spirit::Kind::Principle,
            v14::Kind::Correction => signal_spirit::Kind::Correction,
            v14::Kind::Clarification => signal_spirit::Kind::Clarification,
            v14::Kind::Constraint => signal_spirit::Kind::Constraint,
        }
    }

    pub fn magnitude(value: v14::Magnitude) -> signal_spirit::Magnitude {
        match value {
            v14::Magnitude::Zero => signal_spirit::Magnitude::Zero,
            v14::Magnitude::Minimum => signal_spirit::Magnitude::Minimum,
            v14::Magnitude::VeryLow => signal_spirit::Magnitude::VeryLow,
            v14::Magnitude::Low => signal_spirit::Magnitude::Low,
            v14::Magnitude::Medium => signal_spirit::Magnitude::Medium,
            v14::Magnitude::High => signal_spirit::Magnitude::High,
            v14::Magnitude::VeryHigh => signal_spirit::Magnitude::VeryHigh,
            v14::Magnitude::Maximum => signal_spirit::Magnitude::Maximum,
        }
    }

    pub fn domain(value: v14::Domain) -> signal_domain::Domain {
        match value {
            v14::Domain::All => signal_domain::Domain::All,
            v14::Domain::Health(value) => signal_domain::Domain::Health(health(value)),
            v14::Domain::Food(value) => signal_domain::Domain::Food(food(value)),
            v14::Domain::Home(value) => signal_domain::Domain::Home(home(value)),
            v14::Domain::Finance(value) => signal_domain::Domain::Finance(finance(value)),
            v14::Domain::Work(value) => signal_domain::Domain::Work(work(value)),
            v14::Domain::Craft(value) => signal_domain::Domain::Craft(craft(value)),
            v14::Domain::Knowledge(value) => signal_domain::Domain::Knowledge(knowledge(value)),
            v14::Domain::Education(value) => signal_domain::Domain::Education(education(value)),
            v14::Domain::Language(value) => signal_domain::Domain::Language(language(value)),
            v14::Domain::Art(value) => signal_domain::Domain::Art(art(value)),
            v14::Domain::Kinship(value) => signal_domain::Domain::Kinship(kinship(value)),
            v14::Domain::Selfhood(value) => signal_domain::Domain::Selfhood(selfhood(value)),
            v14::Domain::Spirituality(value) => {
                signal_domain::Domain::Spirituality(spirituality(value))
            }
            v14::Domain::Governance(value) => signal_domain::Domain::Governance(governance(value)),
            v14::Domain::Law(value) => signal_domain::Domain::Law(law(value)),
            v14::Domain::Community(value) => signal_domain::Domain::Community(community(value)),
            v14::Domain::Nature(value) => signal_domain::Domain::Nature(nature(value)),
            v14::Domain::Travel(value) => signal_domain::Domain::Travel(travel(value)),
            v14::Domain::Commerce(value) => signal_domain::Domain::Commerce(commerce(value)),
            v14::Domain::Leisure(value) => signal_domain::Domain::Leisure(leisure(value)),
            v14::Domain::Appearance(value) => signal_domain::Domain::Appearance(appearance(value)),
            v14::Domain::Safety(value) => signal_domain::Domain::Safety(safety(value)),
            v14::Domain::Information(value) => {
                signal_domain::Domain::Information(information(value))
            }
            v14::Domain::Technology(value) => signal_domain::Domain::Technology(technology(value)),
        }
    }

    pub fn technology(value: v14::Technology) -> signal_domain::TechnologyDomain {
        match value {
            v14::Technology::Hardware(value) => {
                signal_domain::TechnologyDomain::Hardware(hardware_leaf(value))
            }
            v14::Technology::Software(value) => {
                signal_domain::TechnologyDomain::Software(software(value))
            }
        }
    }

    pub fn software(value: v14::Software) -> signal_domain::SoftwareDomain {
        match value {
            v14::Software::Programming(value) => {
                signal_domain::SoftwareDomain::Programming(programming_leaf(value))
            }
            v14::Software::Theory => signal_domain::SoftwareDomain::Theory,
            v14::Software::Systems(value) => {
                signal_domain::SoftwareDomain::Systems(systems_leaf(value))
            }
            v14::Software::Distributed(value) => {
                signal_domain::SoftwareDomain::Distributed(distributed_leaf(value))
            }
            v14::Software::Data(value) => signal_domain::SoftwareDomain::Data(data_leaf(value)),
            v14::Software::Intelligence(value) => {
                signal_domain::SoftwareDomain::Intelligence(intelligence_leaf(value))
            }
            v14::Software::Security(value) => {
                signal_domain::SoftwareDomain::Security(security_leaf(value))
            }
            v14::Software::Quality(value) => {
                signal_domain::SoftwareDomain::Quality(quality_leaf(value))
            }
            v14::Software::Operations(value) => {
                signal_domain::SoftwareDomain::Operations(operations_leaf(value))
            }
            v14::Software::Observability(value) => {
                signal_domain::SoftwareDomain::Observability(observability_leaf(value))
            }
            v14::Software::Surfaces(value) => {
                signal_domain::SoftwareDomain::Surfaces(surfaces_leaf(value))
            }
            v14::Software::Engineering(value) => {
                signal_domain::SoftwareDomain::Engineering(engineering_leaf(value))
            }
        }
    }
}

use std::{
    fs,
    os::unix::{fs::DirBuilderExt, fs::PermissionsExt},
    path::{Path, PathBuf},
};

use datom_codec::{Compositional, Datomizable};
use thiserror::Error;

use crate::{
    Store, StoreError,
    schema::{
        sema::{MigratedRecordCount, Migration, SourceSchemaVersion, StoredRecord},
        signal::{Description, Domains, Entry, Importance},
    },
    store::ArchiveDatabase,
};

#[derive(Debug, Clone, PartialEq, Eq, Datomizable, Compositional)]
pub struct StoreMigrationRequest {
    database_path: String,
    optional_legacy_configuration_archive_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Datomizable, Compositional)]
pub struct StoreMigrationCompleted {
    record_count: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Datomizable, Compositional)]
pub enum StoreMigrationOutput {
    Current(StoreMigrationCompleted),
    Migrated(StoreMigrationCompleted),
}

#[derive(Debug, Error)]
pub enum StoreMigrationError {
    #[error("frozen v13 spirit store: {0}")]
    FrozenV13(#[from] v13::ReaderError),
    #[error("frozen v14 spirit store: {0}")]
    FrozenV14(#[from] v14::ReaderError),
    #[error("current spirit store: {0}")]
    Store(#[from] StoreError),
    #[error("store migration io: {0}")]
    Io(#[from] std::io::Error),
    #[error("rollback bundle {path} is incomplete: missing {missing}")]
    IncompleteRollbackBundle {
        path: PathBuf,
        missing: &'static str,
    },
    #[error("migrated store validation failed: {0}")]
    Validation(String),
}

pub struct StoreMigration {
    request: StoreMigrationRequest,
}

enum ArchiveSource {
    Absent,
    VersionThirteen(Vec<v13::StoredRecord>),
    Current(Vec<StoredRecord>),
}

impl StoreMigrationRequest {
    pub fn new(database_path: impl Into<String>) -> Self {
        Self {
            database_path: database_path.into(),
            optional_legacy_configuration_archive_path: None,
        }
    }

    pub fn database_path(&self) -> &str {
        &self.database_path
    }

    /// Offline cutover may carry the exact prior daemon-configuration archive.
    /// Its database location is intentionally not projected; only desired
    /// runtime settings are seeded into the migrated stable Sema.
    pub fn with_legacy_configuration_archive_path(mut self, path: impl Into<String>) -> Self {
        self.optional_legacy_configuration_archive_path = Some(path.into());
        self
    }
}

impl StoreMigrationCompleted {
    pub fn record_count(&self) -> u64 {
        self.record_count as u64
    }
}

impl StoreMigrationOutput {
    pub fn current(completed: StoreMigrationCompleted) -> Self {
        Self::Current(completed)
    }

    pub fn migrated(completed: StoreMigrationCompleted) -> Self {
        Self::Migrated(completed)
    }
}

impl StoreMigration {
    pub fn new(request: StoreMigrationRequest) -> Self {
        Self { request }
    }

    pub fn run(&self) -> Result<StoreMigrationOutput, StoreMigrationError> {
        let database_path = PathBuf::from(self.request.database_path());
        if !database_path.exists() {
            return Ok(StoreMigrationOutput::current(StoreMigrationCompleted {
                record_count: 0,
            }));
        }

        // Snapshot before any engine open: a read-oriented open may still
        // update storage bookkeeping, so copying later would not preserve the
        // exact quiesced v13 bytes.
        let created_rollback = self.stage_rollback_bundle(&database_path)?;
        if let Ok(store) = Store::open(&database_path) {
            if created_rollback {
                Self::remove_new_rollback_bundle(&database_path)?;
            }
            return Ok(StoreMigrationOutput::current(StoreMigrationCompleted {
                record_count: store.len() as i64,
            }));
        }

        match self.migrate_version_fourteen(database_path.clone()) {
            Ok(output) => Ok(output),
            Err(StoreMigrationError::FrozenV14(_)) => {
                match self.migrate_version_thirteen(database_path.clone()) {
                    Ok(output) => Ok(output),
                    Err(error @ StoreMigrationError::FrozenV13(_)) if created_rollback => {
                        Self::remove_new_rollback_bundle(&database_path)?;
                        Err(error)
                    }
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }

    fn legacy_or_default_configuration(
        &self,
    ) -> Result<signal_spirit::SpiritNexusConfiguration, StoreMigrationError> {
        let Some(path) = self
            .request
            .optional_legacy_configuration_archive_path
            .as_ref()
        else {
            return Ok(crate::Configuration::default_nexus_configuration());
        };
        let bytes = fs::read(path)?;
        let legacy =
            rkyv::from_bytes::<v14::SpiritDaemonConfiguration, rkyv::rancor::Error>(&bytes)
                .map_err(|_| {
                    StoreMigrationError::Validation(String::from(
                        "legacy configuration archive does not match published b37fc963 layout",
                    ))
                })?;
        let configuration = legacy
            .project_nexus_configuration()
            .map_err(StoreMigrationError::Validation)?;
        crate::Configuration::validate_nexus_configuration(&configuration)
            .map_err(|error| StoreMigrationError::Validation(error.to_string()))?;
        Ok(configuration)
    }

    /// One-shot v14 -> v15 cutover.  The v14 bytes are copied before any
    /// current engine opens them; current records and receipts are replayed
    /// into a fresh v15 store, whose configuration row is seeded exactly once.
    fn migrate_version_fourteen(
        &self,
        database_path: PathBuf,
    ) -> Result<StoreMigrationOutput, StoreMigrationError> {
        // `run` has already taken an exact pre-open snapshot in its private
        // rollback staging directory. Reuse that snapshot as the v14 source:
        // probing the new engine must never become the thing we migrate or
        // claim as the frozen layout.
        let staged = Self::rollback_bundle_path(&database_path).join("live.v13.sema");
        let frozen_source = if staged.exists() {
            staged
        } else {
            database_path.clone()
        };
        let rollback = database_path.with_file_name(format!(
            "{}.schema-14-rollback",
            Self::file_stem(&database_path)
        ));
        if !rollback.exists() {
            fs::DirBuilder::new().mode(0o700).create(&rollback)?;
        }
        fs::set_permissions(&rollback, fs::Permissions::from_mode(0o700))?;
        fs::copy(&frozen_source, rollback.join("live.v14.sema"))?;
        let source = v14::LiveReader::open(&frozen_source)?;
        let records = source.records()?;
        let migrations = source.migrations()?;
        drop(source);
        let temporary = database_path
            .with_extension(format!("schema-15-migrating-{}.sema", std::process::id()));
        if temporary.exists() {
            fs::remove_file(&temporary)?;
        }
        let configuration = self.legacy_or_default_configuration()?;
        let store = Store::open_with_configuration(&temporary, configuration)?;
        let migrated_record_count = records.len();
        for record in records {
            let projected = Self::project_v14_record(record);
            store.import_record(projected.record_identifier, projected.entry)?;
        }
        for migration in migrations {
            store.record_migration(Migration {
                source_schema_version: SourceSchemaVersion::new(
                    *migration.source_schema_version.content(),
                ),
                migrated_record_count: MigratedRecordCount::new(
                    *migration.migrated_record_count.content(),
                ),
            })?;
        }
        store.record_migration(Migration {
            source_schema_version: SourceSchemaVersion::new(14),
            migrated_record_count: MigratedRecordCount::new(migrated_record_count as u64),
        })?;
        if store.len() != migrated_record_count
            || store.nexus_configuration_state()?.meta_configure_occurred
        {
            return Err(StoreMigrationError::Validation(String::from(
                "v14 cutover changed retained records or seeded a false meta Configure marker",
            )));
        }
        drop(store);
        fs::rename(temporary, database_path)?;
        Ok(StoreMigrationOutput::migrated(StoreMigrationCompleted {
            record_count: migrated_record_count as i64,
        }))
    }

    fn migrate_version_thirteen(
        &self,
        database_path: PathBuf,
    ) -> Result<StoreMigrationOutput, StoreMigrationError> {
        let source = v13::LiveReader::open(&database_path)?;
        let inventory = source.enumerate()?;
        drop(source);

        // Enumeration validates every v13 family, including the two families
        // intentionally discarded by the projection.
        let projected_records = inventory
            .records
            .into_iter()
            .map(Self::project_record)
            .collect::<Result<Vec<_>, _>>()?;
        let archive_path = Self::archive_sibling_path(&database_path);
        let archive_source = Self::read_archive_source(&archive_path)?;
        let projected_archive = match &archive_source {
            ArchiveSource::Absent => None,
            ArchiveSource::VersionThirteen(records) => Some(
                records
                    .iter()
                    .cloned()
                    .map(Self::project_record)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            ArchiveSource::Current(records) => Some(records.clone()),
        };

        self.validate_rollback_bundle(&database_path, &archive_source)?;
        Self::sweep_stale_temporaries(&database_path)?;
        Self::sweep_stale_temporaries(&archive_path)?;

        let live_temporary = Self::temporary_path(&database_path);
        let archive_temporary = Self::temporary_path(&archive_path);
        Self::build_live_projection(&live_temporary, &projected_records)?;
        if let Some(records) = &projected_archive {
            Self::build_archive_projection(&archive_temporary, records)?;
        }

        // Reopen and compare retained substance before exposing either file.
        Self::validate_live_projection(&live_temporary, &projected_records)?;
        if let Some(records) = &projected_archive {
            Self::validate_archive_projection(&archive_temporary, records)?;
        }

        // Archive first makes a crash between the two renames recoverable: a
        // rerun can read the already-current archive while the live v13 source
        // and its private rollback links remain authoritative.
        if projected_archive.is_some() {
            fs::rename(&archive_temporary, &archive_path)?;
        }
        fs::rename(&live_temporary, &database_path)?;

        Ok(StoreMigrationOutput::migrated(StoreMigrationCompleted {
            record_count: projected_records.len() as i64,
        }))
    }

    fn project_record(record: v13::StoredRecord) -> Result<StoredRecord, StoreMigrationError> {
        // Frozen v13 values are decoded only by `v13`; this exhaustive
        // mapping projects retained fields without a text codec intermediate.
        let domains: Domains = record
            .entry
            .domains
            .into_payload()
            .into_iter()
            .map(frozen_projection::domain)
            .collect();
        let kind = frozen_projection::kind(record.entry.kind);
        let description: Description = record.entry.description.into_payload();
        let importance: Importance =
            frozen_projection::magnitude(record.entry.importance.into_payload());

        Ok(StoredRecord {
            record_identifier: record.record_identifier.into_payload(),
            entry: Entry {
                domains,
                kind,
                description,
                importance,
            },
        })
    }

    fn project_v14_record(record: v14::StoredRecord) -> StoredRecord {
        let entry = record.entry;
        StoredRecord {
            record_identifier: record.record_identifier.into_content(),
            entry: Entry {
                domains: entry
                    .domains
                    .into_content()
                    .into_iter()
                    .map(frozen_v14_projection::domain)
                    .collect(),
                kind: frozen_v14_projection::kind(entry.kind),
                description: entry.description.into_content(),
                importance: frozen_v14_projection::magnitude(entry.importance.into_content()),
            },
        }
    }

    fn build_live_projection(
        path: &Path,
        records: &[StoredRecord],
    ) -> Result<(), StoreMigrationError> {
        let store = Store::open(path)?;
        for record in records {
            store.import_record(record.record_identifier.clone(), record.entry.clone())?;
        }
        store.record_migration(Migration {
            source_schema_version: SourceSchemaVersion::new(13),
            migrated_record_count: MigratedRecordCount::new(records.len() as u64),
        })?;
        drop(store);
        Ok(())
    }

    fn build_archive_projection(
        path: &Path,
        records: &[StoredRecord],
    ) -> Result<(), StoreMigrationError> {
        let mut archive = ArchiveDatabase::open(path)?;
        for record in records {
            archive.import_archived_record(record.clone())?;
        }
        drop(archive);
        Ok(())
    }

    fn validate_live_projection(
        path: &Path,
        records: &[StoredRecord],
    ) -> Result<(), StoreMigrationError> {
        let store = Store::open(path)?;
        if store.len() != records.len() {
            return Err(StoreMigrationError::Validation(format!(
                "expected {} projected live records, found {}",
                records.len(),
                store.len()
            )));
        }
        for record in records {
            let found = store.entry_by_identifier(&record.record_identifier)?;
            if found.as_ref() != Some(&record.entry) {
                return Err(StoreMigrationError::Validation(format!(
                    "projected live record {} differs from retained v13 fields",
                    &record.record_identifier
                )));
            }
        }
        let migrations = store.migrations()?;
        if migrations.len() != 1
            || *migrations[0].source_schema_version.payload() != 13
            || *migrations[0].migrated_record_count.payload() != records.len() as u64
        {
            return Err(StoreMigrationError::Validation(String::from(
                "fresh v14 history does not contain exactly one v13 projection receipt",
            )));
        }
        Ok(())
    }

    fn validate_archive_projection(
        path: &Path,
        expected: &[StoredRecord],
    ) -> Result<(), StoreMigrationError> {
        let archive = ArchiveDatabase::open(path)?;
        let found = archive.migration_records()?;
        if found != Self::sorted_records(expected.to_vec()) {
            return Err(StoreMigrationError::Validation(String::from(
                "projected lifecycle archive differs from retained v13 fields",
            )));
        }
        Ok(())
    }

    fn read_archive_source(path: &Path) -> Result<ArchiveSource, StoreMigrationError> {
        if !path.exists() {
            return Ok(ArchiveSource::Absent);
        }
        if let Ok(reader) = v13::ArchiveReader::open(path) {
            return Ok(ArchiveSource::VersionThirteen(reader.records()?));
        }
        // This is the normal crash-recovery state after the archive rename and
        // before the live rename. No other current archive is accepted while a
        // v13 live source is present unless an earlier rollback bundle proves
        // where the original archive bytes survive.
        let archive = ArchiveDatabase::open(path)?;
        Ok(ArchiveSource::Current(archive.migration_records()?))
    }

    fn stage_rollback_bundle(&self, live_path: &Path) -> Result<bool, StoreMigrationError> {
        let bundle = Self::rollback_bundle_path(live_path);
        let created = !bundle.exists();
        if created {
            fs::DirBuilder::new().mode(0o700).create(&bundle)?;
        }
        fs::set_permissions(&bundle, fs::Permissions::from_mode(0o700))?;

        Self::ensure_snapshot_copy(live_path, &bundle.join("live.v13.sema"))?;
        let archive = Self::archive_sibling_path(live_path);
        if archive.exists() && !bundle.join("archive.v13.sema").exists() {
            fs::copy(&archive, bundle.join("archive.v13.sema"))?;
        }
        let journal = Self::guardian_v6_path(live_path);
        if journal.exists() && !bundle.join("guardian.v6.sema").exists() {
            fs::copy(&journal, bundle.join("guardian.v6.sema"))?;
        }
        Ok(created)
    }

    fn validate_rollback_bundle(
        &self,
        live_path: &Path,
        archive_source: &ArchiveSource,
    ) -> Result<(), StoreMigrationError> {
        let bundle = Self::rollback_bundle_path(live_path);
        if !bundle.join("live.v13.sema").exists() {
            return Err(StoreMigrationError::IncompleteRollbackBundle {
                path: bundle,
                missing: "live.v13.sema",
            });
        }
        match archive_source {
            ArchiveSource::Absent => {}
            ArchiveSource::VersionThirteen(_) | ArchiveSource::Current(_) => {
                let backup = bundle.join("archive.v13.sema");
                if !backup.exists() {
                    return Err(StoreMigrationError::IncompleteRollbackBundle {
                        path: bundle,
                        missing: "archive.v13.sema",
                    });
                }
            }
        }
        Ok(())
    }

    fn ensure_snapshot_copy(source: &Path, backup: &Path) -> Result<(), StoreMigrationError> {
        if backup.exists() {
            return Ok(());
        }
        fs::copy(source, backup)?;
        Ok(())
    }

    fn remove_new_rollback_bundle(live_path: &Path) -> Result<(), StoreMigrationError> {
        let bundle = Self::rollback_bundle_path(live_path);
        for name in ["live.v13.sema", "archive.v13.sema", "guardian.v6.sema"] {
            let path = bundle.join(name);
            if path.exists() {
                fs::remove_file(path)?;
            }
        }
        if bundle.exists() {
            fs::remove_dir(bundle)?;
        }
        Ok(())
    }

    fn sorted_records(mut records: Vec<StoredRecord>) -> Vec<StoredRecord> {
        records.sort_by(|left, right| left.record_identifier.cmp(&right.record_identifier));
        records
    }

    fn archive_sibling_path(database_path: &Path) -> PathBuf {
        let stem = Self::file_stem(database_path);
        database_path.with_file_name(format!("{stem}.archive.sema"))
    }

    fn guardian_v6_path(database_path: &Path) -> PathBuf {
        let stem = Self::file_stem(database_path);
        database_path.with_file_name(format!("{stem}.guardian.v6.sema"))
    }

    fn rollback_bundle_path(database_path: &Path) -> PathBuf {
        let stem = Self::file_stem(database_path);
        database_path.with_file_name(format!("{stem}.schema-13-rollback"))
    }

    fn temporary_path(database_path: &Path) -> PathBuf {
        database_path.with_extension(format!("schema-14-migrating-{}.sema", std::process::id()))
    }

    fn temporary_name_prefix(database_path: &Path) -> String {
        format!("{}.schema-14-migrating-", Self::file_stem(database_path))
    }

    fn sweep_stale_temporaries(database_path: &Path) -> Result<(), StoreMigrationError> {
        let directory = database_path.parent().unwrap_or_else(|| Path::new("."));
        let prefix = Self::temporary_name_prefix(database_path);
        let entries = match fs::read_dir(directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with(&prefix) && name.ends_with(".sema") {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }

    fn file_stem(path: &Path) -> String {
        path.file_stem()
            .map(|stem| stem.to_string_lossy().into_owned())
            .unwrap_or_else(|| String::from("spirit"))
    }
}
