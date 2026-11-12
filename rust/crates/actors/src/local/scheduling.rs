//! Stable scheduling identity, independent of placement and process hash seeds.
use super::{ActorReference, Error, Store, StreamPath, StreamProvider};
use sha2::{Digest, Sha256};

/// The first SHA-256 byte of the domain followed by the canonical actor address.
/// These 256 partitions split scheduling ownership, not actor capacity. Moving
/// a partition between nodes never moves or renames its durable streams.
pub fn scheduling_shard(actor: &ActorReference) -> u8 {
    let mut hash = Sha256::new();
    hash.update(b"acyclic.actors.scheduling.v1\0");
    hash.update(actor.encode());
    hash.finalize()[0]
}

impl<P: StreamProvider + ?Sized> Store<P> {
    /// The wake log of one stable scheduling partition. Producers append here
    /// in the same publication as input; only its assigned host follows it.
    pub fn scheduling_path(&self, shard: u8) -> Result<StreamPath, Error> {
        Ok(StreamPath::new(format!(
            "{}/_schedule/{shard:02x}",
            self.root()
        ))?)
    }
}
