#[allow(dead_code)]
pub mod process;

#[allow(dead_code)]
pub mod domain_fixtures {
    use signal_domain::{
        DataLeaf, Domain, DomainScope, DomainScopes, GovernanceDomain, InformationDomain,
        KnowledgeDomain, ObservabilityLeaf, SoftwareDomain, TechnologyDomain,
    };

    pub fn domains(labels: &[&str]) -> Vec<Domain> {
        labels
            .iter()
            .filter(|label| !label.is_empty())
            .map(|label| domain_for_label(label))
            .collect()
    }

    pub fn scopes(labels: &[&str]) -> DomainScopes {
        labels
            .iter()
            .map(|label| DomainScope {
                domain: domain_for_label(label),
            })
            .collect()
    }

    fn domain_for_label(label: &str) -> Domain {
        let normalized = label.to_ascii_lowercase();
        if normalized.contains("schema") {
            return Domain::Technology(TechnologyDomain::Software(SoftwareDomain::Data(
                DataLeaf::SchemaEvolution,
            )));
        }
        if normalized.contains("runtime") {
            return Domain::Technology(TechnologyDomain::Software(SoftwareDomain::Data(
                DataLeaf::Modeling,
            )));
        }
        if normalized.contains("migration") {
            return Domain::Technology(TechnologyDomain::Software(SoftwareDomain::Data(
                DataLeaf::Migration,
            )));
        }
        if normalized.contains("trace") || normalized.contains("stream") {
            return Domain::Technology(TechnologyDomain::Software(SoftwareDomain::Observability(
                ObservabilityLeaf::Tracing,
            )));
        }
        if normalized.contains("govern") {
            return Domain::Governance(GovernanceDomain::Government);
        }
        if normalized.contains("meaning") || normalized.contains("relating") {
            return Domain::Knowledge(KnowledgeDomain::Philosophy);
        }
        Domain::Information(InformationDomain::Documentation)
    }
}
