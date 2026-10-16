//! Runs the production session pool with crate-local Loom synchronization.
#![cfg(all(acyclic_stream_loom, feature = "grpc"))]

#[test]
fn malformed_node_cannot_terminate_independent_routes_or_opens() {
    acyclic_stream::session::verify_link_isolation(false);
}

#[test]
#[should_panic(expected = "a peer fault must preserve the healthy route")]
fn negative_control_propagates_a_peer_fault_to_the_whole_pool() {
    acyclic_stream::session::verify_link_isolation(true);
}
