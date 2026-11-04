//! Funded fixed-width radix keys keep lookup work independent of the journal length.
use super::*;
use crate::wire_codec::{OwnedIndexAllocation, OwnedIndexWork};
use acyclic_collections::FixedIndex;

pub(super) trait SummaryKey: Copy {
    fn digest(&self, io: Option<&PreparationIo>) -> Result<[u8; 32]>;
}

impl SummaryKey for [u8; 32] {
    fn digest(&self, _io: Option<&PreparationIo>) -> Result<[u8; 32]> {
        Ok(*self)
    }
}

impl SummaryKey for u32 {
    fn digest(&self, _io: Option<&PreparationIo>) -> Result<[u8; 32]> {
        let mut key = [0; 32];
        for (to, from) in key.iter_mut().zip(self.to_be_bytes()) {
            *to = from;
        }
        Ok(key)
    }
}

impl SummaryKey for (u32, crate::executor::ModelPurpose) {
    fn digest(&self, io: Option<&PreparationIo>) -> Result<[u8; 32]> {
        let mut key = self.0.digest(io)?;
        let purpose = key.get_mut(4).ok_or_else(|| {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "model summary key is invalid",
            )
        })?;
        *purpose = match self.1 {
            crate::executor::ModelPurpose::Response => 0,
            crate::executor::ModelPurpose::Summary => 1,
        };
        Ok(key)
    }
}

impl SummaryKey for (u32, [u8; 32]) {
    fn digest(&self, io: Option<&PreparationIo>) -> Result<[u8; 32]> {
        let mut bytes = [0; 36];
        for (to, from) in bytes
            .iter_mut()
            .zip(self.0.to_be_bytes().into_iter().chain(self.1))
        {
            *to = from;
        }
        crate::model::hash_bytes(&bytes, io)
    }
}

pub(super) struct SummaryMap<K, V> {
    keys: FixedIndex<u64, OwnedIndexAllocation, 32>,
    values: FixedIndex<([u8; 32], V), OwnedIndexAllocation, 8>,
    len: usize,
    _key: std::marker::PhantomData<K>,
}

impl<K, V> Default for SummaryMap<K, V> {
    fn default() -> Self {
        Self {
            keys: Default::default(),
            values: Default::default(),
            len: 0,
            _key: std::marker::PhantomData,
        }
    }
}

impl<K: SummaryKey, V> SummaryMap<K, V> {
    pub(super) fn entries<'a>(
        &'a self,
        io: Option<&'a PreparationIo>,
    ) -> impl Iterator<Item = Result<([u8; 32], &'a V)>> + 'a {
        let mut after = None;
        let mut finished = false;
        std::iter::from_fn(move || {
            if finished {
                return None;
            }
            let mut work = OwnedIndexWork(io);
            let next = match after {
                None => self.values.first(&mut work),
                Some(key) => self.values.seek_after(key, &mut work),
            };
            match next {
                Ok(Some((key, (digest, value)))) => {
                    after = Some(*key);
                    Some(Ok((*digest, value)))
                }
                Ok(None) => {
                    finished = true;
                    None
                }
                Err(error) => {
                    finished = true;
                    Some(Err(error))
                }
            }
        })
    }

    pub(super) fn get_digest(
        &self,
        digest: &[u8; 32],
        io: Option<&PreparationIo>,
    ) -> Result<Option<&V>> {
        let mut work = OwnedIndexWork(io);
        let Some(position) = self.keys.get(*digest, &mut work)? else {
            return Ok(None);
        };
        self.values
            .get(position.to_be_bytes(), &mut work)?
            .map(|(_, value)| value)
            .ok_or_else(|| missing(io))
            .map(Some)
    }

    fn position(&self, key: &K, io: Option<&PreparationIo>) -> Result<Option<u64>> {
        Ok(self
            .keys
            .get(key.digest(io)?, &mut OwnedIndexWork(io))?
            .copied())
    }

    pub(super) fn get(&self, key: &K, io: Option<&PreparationIo>) -> Result<Option<&V>> {
        self.get_digest(&key.digest(io)?, io)
    }

    pub(super) fn get_mut(
        &mut self,
        key: &K,
        io: Option<&PreparationIo>,
    ) -> Result<Option<&mut V>> {
        let Some(position) = self.position(key, io)? else {
            return Ok(None);
        };
        self.values
            .get_mut(position.to_be_bytes(), &mut OwnedIndexWork(io))?
            .map(|(_, value)| value)
            .ok_or_else(|| missing(io))
            .map(Some)
    }

    pub(super) fn contains_key(&self, key: &K, io: Option<&PreparationIo>) -> Result<bool> {
        Ok(self.position(key, io)?.is_some())
    }

    pub(super) fn insert(&mut self, key: K, value: V, io: Option<&PreparationIo>) -> Result<()> {
        let digest = key.digest(io)?;
        self.insert_digest(digest, value, io)
    }

    pub(super) fn insert_digest(
        &mut self,
        digest: [u8; 32],
        value: V,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let mut incoming = Some(value);
        let mut work = OwnedIndexWork(io);
        let position = *self
            .keys
            .get_or_try_insert_with(digest, &mut work, |work| {
                let next = self.len.checked_add(1).ok_or_else(|| {
                    canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "execution summary capacity overflows",
                    )
                })?;
                let ordinal = self.len as u64;
                self.values
                    .get_or_try_insert_with(ordinal.to_be_bytes(), work, |_| {
                        Ok((digest, incoming.take().ok_or_else(|| missing(io))?))
                    })?;
                // The outer index admitted all of its work before this constructor;
                // successful inner publication is followed only by outer publication.
                self.len = next;
                Ok(ordinal)
            })?;
        if let Some(value) = incoming {
            self.values
                .get_mut(position.to_be_bytes(), &mut work)?
                .ok_or_else(|| missing(io))?
                .1 = value;
        }
        Ok(())
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }
}

fn missing(io: Option<&PreparationIo>) -> Error {
    canonical_boundary(
        io,
        crate::wire::ErrorCode::Storage,
        "execution summary slot is missing",
    )
}
