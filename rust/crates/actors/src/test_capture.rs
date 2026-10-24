// Runs before libtest creates threads, closing tracing-core's JustOne window.
ctor::declarative::ctor! {
    #[ctor(unsafe)]
    fn initialize() {
        acyclic_test_support::capture::initialize();
    }
}
