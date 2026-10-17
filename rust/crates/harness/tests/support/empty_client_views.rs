//! Construct the same production fold for isolated renderer tests.
use super::harness::{
    client::{ClientViewLimits, ClientViews, QueryViewLimits},
    task_volume::{Query, TaskVolume},
};
use acyclic_stream::{MemoryStream, StreamPath};
use std::sync::Arc;
pub fn empty() -> ClientViews {
    let snapshot = futures::executor::block_on(async {
        TaskVolume::open(
            Arc::new(MemoryStream::default()),
            StreamPath::new("test-files").unwrap(),
        )
        .await
        .unwrap()
        .snapshot(&Query::Root, 1024)
        .await
        .unwrap()
    });
    let mut views = ClientViews::new(
        StreamPath::new("test").unwrap(),
        ClientViewLimits {
            agents: 1,
            selected: 1,
            overview_values: 1024,
            overview_bytes: 8_388_608,
            query: QueryViewLimits {
                files: 1024,
                segments: 1024,
                bytes: 8_388_608,
            },
        },
    )
    .unwrap();
    views
        .query_snapshot(Query::Root, snapshot, 1024, 8_388_608)
        .unwrap();
    views
}
