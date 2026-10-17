use crate::{
    client::{ClientViewLimits, ClientViews, QueryViewLimits},
    task_volume::{Cursor, Generation, Query, SCHEMA, Snapshot, query_id},
};
use acyclic_stream::StreamPath;

/// Empty fold for isolated rendering tests; performs no I/O or executor entry.
pub fn empty_client_views() -> ClientViews {
    let generation = Generation {
        volume: [1; 32],
        epoch: Default::default(),
        position: 1,
        commit: [1; 32],
    };
    let snapshot = Snapshot {
        files: Vec::new(),
        cursor: Cursor {
            volume: generation.volume,
            epoch: generation.epoch,
            schema: SCHEMA,
            query: query_id(&Query::Root).unwrap(),
            position: generation.position,
            accepted: generation.position,
            durable: generation.position,
            commit: generation.commit,
        },
        generation,
    };
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
