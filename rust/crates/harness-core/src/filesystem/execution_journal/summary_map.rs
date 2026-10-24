//! Funded fixed-width radix keys keep lookup work independent of the journal length.
use super::*;

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
    keys: crate::model::DigestIndex<usize>,
    values: Vec<V>,
    _key: std::marker::PhantomData<K>,
}

impl<K, V> Default for SummaryMap<K, V> {
    fn default() -> Self {
        Self {
            keys: Default::default(),
            values: Vec::new(),
            _key: std::marker::PhantomData,
        }
    }
}

impl<K: SummaryKey, V> SummaryMap<K, V> {
    pub(super) fn entries(&self) -> impl Iterator<Item = Result<([u8; 32], &V)>> + '_ {
        self.keys.entries().map(|(key, position)| {
            self.values
                .get(position)
                .map(|value| (key, value))
                .ok_or_else(|| missing(None))
        })
    }

    pub(super) fn get_digest(
        &self,
        digest: &[u8; 32],
        io: Option<&PreparationIo>,
    ) -> Result<Option<&V>> {
        self.keys
            .get(0, digest, io)?
            .map(|position| self.values.get(position).ok_or_else(|| missing(io)))
            .transpose()
    }

    fn position(&self, key: &K, io: Option<&PreparationIo>) -> Result<Option<usize>> {
        self.keys.get(0, &key.digest(io)?, io)
    }

    pub(super) fn get(&self, key: &K, io: Option<&PreparationIo>) -> Result<Option<&V>> {
        self.position(key, io)?
            .map(|position| self.values.get(position).ok_or_else(|| missing(io)))
            .transpose()
    }

    pub(super) fn get_mut(
        &mut self,
        key: &K,
        io: Option<&PreparationIo>,
    ) -> Result<Option<&mut V>> {
        self.position(key, io)?
            .map(|position| self.values.get_mut(position).ok_or_else(|| missing(io)))
            .transpose()
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
        if let Some(position) = self.keys.get(0, &digest, io)? {
            *self.values.get_mut(position).ok_or_else(|| missing(io))? = value;
            return Ok(());
        }
        self.keys.reserve_key(io)?;
        if self.values.len() == self.values.capacity() {
            let capacity = self
                .values
                .capacity()
                .checked_mul(2)
                .map(|capacity| capacity.max(1))
                .ok_or_else(|| {
                    canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "execution summary capacity overflows",
                    )
                })?;
            crate::executor::stock_vec_reserve(&mut self.values, capacity, io)?;
        }
        let position = self.values.len();
        crate::executor::stock_vec_push(&mut self.values, value, io)?;
        self.keys.set(0, digest, Some(position), io)
    }

    pub(super) fn len(&self) -> usize {
        self.values.len()
    }
}

fn missing(io: Option<&PreparationIo>) -> Error {
    canonical_boundary(
        io,
        crate::wire::ErrorCode::Storage,
        "execution summary slot is missing",
    )
}
