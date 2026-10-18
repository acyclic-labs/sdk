//! Initialized read storage whose valid prefix does not shrink its backing buffer.
use std::io;

/// Initialized storage and the valid bytes returned by its latest read.
#[derive(Debug)]
pub struct InputBatch {
    storage: Vec<u8>,
    valid: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn prefixes_preserve_all_initialized_storage(
            input in prop::collection::vec(any::<u8>(), 0..128),
            lengths in prop::collection::vec(any::<u8>(), 0..32),
        ) {
            let mut batch = InputBatch::new(input.len());
            batch.storage_mut().copy_from_slice(&input);
            for length in lengths {
                let valid = usize::from(length) % (input.len() + 1);
                batch = batch.finish(valid).unwrap();
                prop_assert_eq!(batch.bytes(), &input[..valid]);
                prop_assert_eq!(batch.storage_mut(), input.as_slice());
            }
        }

        #[test]
        fn oversized_prefixes_are_rejected(size in 0usize..128, excess in 1usize..128) {
            prop_assert_eq!(InputBatch::new(size).finish(size + excess).unwrap_err().kind(), io::ErrorKind::InvalidData);
        }
    }
}

impl InputBatch {
    /// Initialize once; subsequent reads reuse all initialized bytes.
    pub fn new(size: usize) -> Self {
        Self {
            storage: vec![0; size],
            valid: 0,
        }
    }

    /// The complete initialized destination, independent of the previous read.
    pub fn storage_mut(&mut self) -> &mut [u8] {
        &mut self.storage
    }

    /// Publish the valid prefix without truncating or initializing storage.
    pub fn finish(mut self, valid: usize) -> io::Result<Self> {
        if valid > self.storage.len() {
            return Err(io::ErrorKind::InvalidData.into());
        }
        self.valid = valid;
        Ok(self)
    }

    /// The valid prefix of the last completed read.
    pub fn bytes(&self) -> &[u8] {
        &self.storage[..self.valid]
    }
}
