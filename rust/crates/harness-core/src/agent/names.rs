//! Immutable agent aliases over Streams. An index lookup grants no access.
//! Callers compose a binding with their own admission conditions and receipt.
use super::{AgentPath, Error, MAX_RECORD_BYTES};
use acyclic_stream::{
    CommitCondition, CommitMutation, CommitRequest, HeadProjection, HeadsRequest, StreamPath,
    StreamProvider,
};
use std::sync::Arc;

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    version: u32,
    name: String,
    agent: AgentPath,
}

/// One scoped name index. Bindings survive changes to display titles and
/// archival; another alias may name the same agent without moving its history.
pub struct NameIndex<P: ?Sized> {
    provider: Arc<P>,
    root: StreamPath,
}

impl<P: StreamProvider + ?Sized + 'static> NameIndex<P> {
    /// A caller-owned scope and its original provider.
    pub fn new(provider: Arc<P>, root: StreamPath) -> Self {
        Self { provider, root }
    }

    fn path(&self, name: &str) -> Result<StreamPath, Error> {
        if name.is_empty() {
            return Err(Error::Record("an agent name must be nonempty"));
        }
        let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.name.v1");
        hash.update(name.as_bytes());
        StreamPath::new(format!(
            "{}/{}",
            self.root.as_str(),
            hash.finalize().to_hex()
        ))
        .map_err(Error::Stream)
    }

    /// Select one exact immutable binding, independently of index cardinality.
    /// Original spelling is checked after hashing, so a collision refuses
    /// rather than resolving a different name.
    pub async fn resolve(&self, name: &str) -> Result<Option<AgentPath>, Error> {
        let request = HeadsRequest {
            paths: [self.path(name)?].into_iter().collect(),
            retained: Default::default(),
            durable_through: None,
            projection: HeadProjection::History,
            children: None,
            limit: 1,
            at_least: Default::default(),
        };
        let view = self
            .provider
            .heads(request.clone())
            .await
            .map_err(Error::Stream)?;
        view.validate_for(&request).map_err(Error::Stream)?;
        let Some(head) = view.heads.into_iter().next().flatten() else {
            return Ok(None);
        };
        let record = head
            .last
            .ok_or(Error::Record("agent name has no binding"))?;
        if head.tail != 1 || record.sequence != 0 {
            return Err(Error::Record("agent name binding is not immutable"));
        }
        let binding: Binding = serde_json::from_slice(&record.value)
            .map_err(|_| Error::Record("agent name binding is malformed"))?;
        if binding.version != 1 || binding.name != name {
            return Err(Error::Record("agent name binding differs from its lookup"));
        }
        Ok(Some(binding.agent))
    }

    /// Add one binding to the caller's atomic publication. An existing binding
    /// conflicts; the caller's unchanged receipt replays the original result.
    pub fn bind(
        &self,
        name: &str,
        agent: &AgentPath,
        request: &mut CommitRequest,
    ) -> Result<(), Error> {
        let path = self.path(name)?;
        let value = serde_json::to_vec(&Binding {
            version: 1,
            name: name.to_owned(),
            agent: agent.clone(),
        })
        .map_err(|_| Error::Record("agent name cannot encode"))?;
        if value.len() > MAX_RECORD_BYTES {
            return Err(Error::Record("agent name binding exceeds the record frame"));
        }
        request.effects.push(acyclic_stream::CommitEffect::hard(
            vec![CommitCondition::Absent { path: path.clone() }],
            vec![CommitMutation::Append {
                path,
                records: vec![value.into()],
            }],
        ));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_stream::{
        CommitOutcome, MemoryStream, StreamProviderExt as _,
        testing::{Counted, RequestClass},
    };
    use acyclic_test_support::scale::{Bound, assert_scales};

    #[tokio::test]
    async fn alias_lookup_requests_do_not_grow_with_unrelated_bindings()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut samples = Vec::new();
        for size in [16_u64, 128] {
            let provider = Arc::new(Counted::new(MemoryStream::default()));
            let root = StreamPath::new("aliases")?;
            let index = NameIndex::new(provider.clone(), root);
            let agent = AgentPath::root(&super::super::AgentId::from_bytes([1; 16]));
            for number in 0..size {
                let name = format!("alias-{number:03}");
                let mut request = CommitRequest {
                    deadline_unix_millis: None,
                    effects: Vec::new(),
                    idempotency_key: super::super::intent_key("alias-test", &[&name])?,
                };
                let before = provider.requests();
                index.bind(&name, &agent, &mut request)?;
                assert_eq!(
                    provider.requests(),
                    before,
                    "binding preparation dispatched IO"
                );
                assert!(matches!(
                    provider.commit(request).await?,
                    CommitOutcome::Committed(_)
                ));
            }
            assert_eq!(
                provider.requests().data(),
                size,
                "write counter is inactive"
            );
            let mut costs = Vec::new();
            for (name, expected) in [
                ("alias-000", Some(agent.clone())),
                ("alias-015", Some(agent.clone())),
                ("missing-alias", None),
            ] {
                let before = provider.requests();
                assert_eq!(index.resolve(name).await?, expected);
                let work = provider.requests().since(before);
                assert_eq!(work.class(RequestClass::Head), 1);
                assert_eq!(work.auxiliary(Some(RequestClass::Head)), Some(0));
                costs.push(work.class(RequestClass::Head));
            }
            samples.push((size, costs));
        }
        for (small, large) in samples[0].1.iter().zip(&samples[1].1) {
            assert_scales(
                Bound::Constant,
                (samples[0].0, *small),
                (samples[1].0, *large),
            );
        }
        Ok(())
    }
}
