//! The same reducer, with no swarm selections for private file-query tests.
pub fn file_views() -> acyclic_harness::client::ClientViews {
    use acyclic_harness::client::{ClientViewLimits, ClientViews, QueryViewLimits};
    ClientViews::new(
        acyclic_stream::StreamPath::new("swarm").unwrap(),
        ClientViewLimits {
            agents: 0,
            selected: 0,
            overview_values: 0,
            overview_bytes: 0,
            query: QueryViewLimits {
                files: 0,
                segments: 0,
                bytes: 0,
            },
        },
    )
    .unwrap()
}
