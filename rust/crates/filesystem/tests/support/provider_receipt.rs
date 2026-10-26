macro_rules! provider_receipt {
    ($store:expr, $method:ident($($arg:expr),* $(,)?), $budget:expr, $cancel:expr) => {{
        let arguments = ($($arg,)*);
        let funding = ($budget, $cancel);
        let mut work = acyclic_fs::AuthorityWork::new(funding.0, funding.1);
        let result = provider_receipt!(@call $store, $method, arguments, work);
        work.receipt(result)
    }};
    (@call $store:expr, create_authority, $args:ident, $work:ident) => {
        $store.create_authority($args.0, $args.1, &mut $work, None::<&mut ()>)
            .await.map_err(|error| error.into_error(std::convert::identity))
    };
    (@call $store:expr, compare_and_append, $args:ident, $work:ident) => {
        $store.compare_and_append(acyclic_fs::GuardedAppend {
            authority_id: $args.0,
            epoch: $args.1,
            expected: $args.2,
            commit: $args.3,
            permit: acyclic_fs::PublicationPermit::Unrestricted,
        }, &mut $work, None::<&mut ()>)
            .await.map_err(|error| error.into_error(std::convert::identity))
    };
    (@call $store:expr, replay, $args:ident, $work:ident) => {
        $store.replay($args.0, $args.1, $args.2, &mut $work)
            .await.map(|(value, _allocation)| value)
    };
    (@call $store:expr, read, $args:ident, $work:ident) => {
        $store.read($args.0, $args.1, &mut $work).await
    };
    (@call $store:expr, read_many, $args:ident, $work:ident) => {
        $store.read_many($args.0, &mut $work)
            .await.map(|(values, _allocation)| values)
    };
    (@call $store:expr, contains, $args:ident, $work:ident) => {
        $store.contains($args.0, &mut $work).await
    };
    (@call $store:expr, put, $args:ident, $work:ident) => {
        $store.put($args.0, $args.1, &mut $work).await
    };
    (@call $store:expr, put_many, $args:ident, $work:ident) => {
        $store.put_many($args.0, &mut $work).await
    };
    (@call $store:expr, fork_authority, $args:ident, $work:ident) => {
        $store.fork_authority($args.0, $args.1, $args.2, &mut $work, None::<&mut ()>)
            .await.map_err(|error| error.into_error(std::convert::identity))
    };
}
