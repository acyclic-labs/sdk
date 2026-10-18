use crate::kernel::{BlobChunkRef, BlobNode, BlobPage, blob_page_id};
use crate::storage::{ObjectId, ObjectKind, object_digest};

// These fixtures stage one nonempty range, smaller than a blob chunk: one
// content object and one single-reference index page, in one provider batch.
// Derive its duplicate-index work from those objects, outside measurement.
// In particular, the process's hash keys can make the two objects collide.
pub(super) fn one_chunk_index(bytes: &[u8]) -> (u64, u64) {
    assert!(!bytes.is_empty());
    let chunk = ObjectId {
        kind: ObjectKind::BlobChunk,
        digest: object_digest(ObjectKind::BlobChunk, bytes),
    };
    let page = BlobPage {
        first_offset: 0,
        end_offset: bytes.len() as u64,
        node: BlobNode::Leaf(vec![BlobChunkRef {
            first_offset: 0,
            end_offset: bytes.len() as u64,
            chunk,
        }]),
    };
    let batches = [[chunk, blob_page_id(&page, 1).unwrap()]];
    let allocations = batches.len() as u64;
    let probes = batches
        .iter()
        .map(|batch| crate::logical_objects::expected_batch_probes(batch))
        .sum();
    (allocations, probes)
}
