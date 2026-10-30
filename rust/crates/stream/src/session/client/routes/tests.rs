use super::super::*;

#[tokio::test]
async fn indexed_node_routes_follow_home_replacement_and_cancel()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let channel = tonic::transport::Endpoint::from_static("http://unused.invalid").connect_lazy();
    for unrelated in [1_u64, 1_000, 100_000] {
        let (mut pool, _healthy) = pool::fixture::fixture(&channel);
        pool.routes.clear();
        pool.reset_home();
        let (records, _receiver) = mpsc::channel(2);
        for id in 1..=unrelated {
            pool.routes.insert(
                id,
                pool::fixture::route("other", Kind::Follow(Default::default()), records.clone()),
            );
        }
        let id = unrelated + 1;
        pool.routes.insert(
            id,
            pool::fixture::route("", Kind::Follow(Default::default()), records.clone()),
        );
        let before = pool.routes.visits.get();
        let redundant = pool.links.remove("bad").ok_or("missing fixture link")?;
        pool.dial_ready(
            "",
            0,
            Ok(("good".into(), redundant, Box::pin(stream::empty()))),
        )?;
        assert_eq!(pool.routes.visits.get() - before, 2);
        assert_eq!(pool.pending.pop_front(), Some((id, 1)));
        assert!(pool.pending.is_empty());
        assert!(pool.routes.on_node("").next().is_none());

        let before = pool.routes.visits.get();
        assert!(pool.node_interested("good"));
        assert_eq!(pool.dial_owner("good"), Some(id));
        assert_eq!(pool.routes.visits.get() - before, 2);
        // The original lookup visits all unrelated routes before its owner.
        let mut global_visits = 0;
        let owner = pool.routes.iter().find_map(|(id, route)| {
            global_visits += 1;
            (route.node == "good" && route.live()).then_some(*id)
        });
        assert_eq!(owner, Some(id));
        assert_eq!(global_visits, unrelated + 1);

        // Movement and replacement must withdraw the previous physical owner.
        pool.routes.rehome(&id, "moved").ok_or("missing route")?;
        assert!(!pool.node_interested("good"));
        assert_eq!(pool.dial_owner("moved"), Some(id));
        let progress = Arc::clone(&pool.routes.get(&id).ok_or("missing route")?.progress);
        progress.cancel();
        assert!(!pool.node_interested("moved"));
        assert_eq!(pool.dial_owner("moved"), None);
        assert!(pool.node_interested("other"));
        pool.routes.insert(
            id,
            pool::fixture::route("", Kind::Follow(Default::default()), records),
        );
        assert!(pool.routes.on_node("moved").next().is_none());
        assert!(pool.node_interested(""));
        pool.routes.remove(&id).ok_or("missing replacement")?;
        assert!(!pool.node_interested(""));
        assert_eq!(pool.routes.len(), unrelated as usize);
        assert_eq!(pool.routes.nodes.len(), 1);
        pool.routes.clear();
        assert!(pool.routes.nodes.is_empty());
        eprintln!(
            "unrelated={unrelated}: home visits=2, owner/interest visits=1 each, old owner visits={global_visits}"
        );
    }
    Ok(())
}
