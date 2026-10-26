macro_rules! unbounded {
    ($receiver:expr, $method:ident($($arg:expr),* $(,)?)) => {
        $receiver
            .$method(
                $($arg,)*
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
    };
}
