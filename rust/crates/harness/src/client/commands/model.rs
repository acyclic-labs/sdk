// Model sizes are bounded by each scenario; every schedule remains enabled.
pub(super) fn check(
    run: impl Fn() -> Result<(), Box<dyn std::error::Error + Send + Sync>> + Send + Sync + 'static,
) {
    loom::model(move || {
        let result = run();
        assert!(result.is_ok(), "{result:?}");
    });
}
