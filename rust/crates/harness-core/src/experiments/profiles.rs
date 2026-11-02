use super::{ArmsFile, Error, Trial};
use crate::{
    agent::{AgentProfilePin, SpawnContext, SpawnProfiles},
    profile::Profiles,
};

/// Population admitted against the same immutable role snapshot as its host.
/// No child, history, transcript or replicate collection is retained.
pub struct Population {
    file: ArmsFile,
    digest: blake3::Hash,
}

impl Population {
    /// Checks all executable roles, so descendants cannot select an expensive
    /// model. `profiles` must be the host's immutable profile snapshot.
    pub fn admit(
        file: ArmsFile,
        profiles: &Profiles,
        parent: &AgentProfilePin,
    ) -> Result<Self, Error> {
        file.validate()?;
        let mut roles: Vec<_> = profiles.roles().collect();
        roles.sort_unstable_by(|(left, _), (right, _)| left.role.cmp(&right.role));
        let mut hash = blake3::Hasher::new_derive_key("acyclic.experiments.population.v1");
        hash.update(&serde_json::to_vec(&file)?);
        for (pin, role) in roles {
            if role.context() != SpawnContext::TopLevel
                && ![
                    super::CheapModel::Luna,
                    super::CheapModel::Haiku,
                    super::CheapModel::Flash,
                ]
                .iter()
                .any(|model| model.key() == role.model())
            {
                return Err(Error::Role(format!(
                    "{} selects an unapproved model",
                    role.name()
                )));
            }
            hash.update(&pin.digest);
        }
        for arm in &file.arms {
            let pin = profiles
                .child(parent, &arm.role)
                .map_err(|error| Error::Role(error.to_string()))?;
            let role = profiles
                .pinned(&pin)
                .map_err(|error| Error::Role(error.to_string()))?;
            if role.context() != SpawnContext::Fork || role.model() != arm.model.key() {
                return Err(Error::Role(format!(
                    "{} differs from its declared fork role or model",
                    arm.name
                )));
            }
        }
        Ok(Self {
            file,
            digest: hash.finalize(),
        })
    }

    /// Stable identity for joining result rows and paired observations.
    pub fn digest(&self) -> String {
        self.digest.to_hex().to_string()
    }

    /// One trial at a time, without multiplying counts or preallocating N rows.
    /// Arms rotate within each replicate to reduce temporal order confounding.
    pub fn trials(&self) -> impl Iterator<Item = Trial<'_>> {
        (0..self.file.replicates.get())
            .scan(0, move |offset, replicate| {
                let start = *offset;
                *offset = rotate(start, self.file.arms.len());
                Some((replicate, start))
            })
            .flat_map(move |(replicate, start)| {
                let (before, after) = self.file.arms.split_at(start);
                after
                    .iter()
                    .enumerate()
                    .map(move |(index, arm)| (index + start, arm))
                    .chain(before.iter().enumerate())
                    .map(move |(arm_index, arm)| Trial {
                        file: &self.file,
                        arm,
                        replicate,
                        arm_index,
                        digest: &self.digest,
                    })
            })
    }
}

fn rotate(start: usize, count: usize) -> usize {
    if start == count - 1 { 0 } else { start + 1 }
}

#[cfg(kani)]
mod proofs {
    #[kani::proof]
    fn rotation_is_bounded_and_always_advances() {
        let count: usize = kani::any();
        let start: usize = kani::any();
        kani::assume(count >= 2 && start < count);
        let next = super::rotate(start, count);
        assert!(next < count);
        assert!(next != start);
        assert!(start == count - 1 || next == start + 1);
    }
}
