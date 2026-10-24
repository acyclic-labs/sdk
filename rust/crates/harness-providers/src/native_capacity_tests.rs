use super::*;
use proptest::prelude::*;

struct Boundary {
    transport: Fixture,
    capacity: u64,
}

impl HttpTransport for Boundary {
    fn send(&self, _: HttpRequest) -> BoxProviderFuture<'_, Result<HttpResponse>> {
        panic!("native task must use original-ledger transport")
    }
    fn now(&self) -> std::time::Duration {
        self.transport.now()
    }
    fn delay(&self, duration: std::time::Duration) -> BoxProviderFuture<'static, ()> {
        self.transport.delay(duration)
    }
    fn supports_native(&self, work: &NativeMaterializationWork) -> Result<bool> {
        self.transport.supports_native(work)
    }
    fn delay_with_native_work(
        &self,
        duration: std::time::Duration,
        work: &NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'static, ()>> {
        self.transport.delay_with_native_work(duration, work)
    }
    fn send_with_native_work<'a>(
        &'a self,
        request: HttpRequest,
        work: &'a NativeMaterializationWork,
    ) -> Result<NativeFutureBundle<'a, HttpResponse>> {
        if !(request.url.ends_with("count_tokens")
            || request.url.ends_with("input_tokens")
            || request.url.ends_with(":countTokens"))
        {
            let body = crate::json(&request.body)?;
            let output = [
                "/max_output_tokens",
                "/max_completion_tokens",
                "/max_tokens",
                "/generationConfig/maxOutputTokens",
            ]
            .into_iter()
            .find_map(|path| body.pointer(path).and_then(Value::as_u64))
            .expect("native dispatch has an explicit output bound");
            assert!(
                request.body.len() as u64 + output <= self.capacity,
                "native task dispatched encoded input plus output beyond catalog capacity"
            );
        }
        self.transport.send_with_native_work(request, work)
    }
}

async fn check(input: &str, capacity: u32) -> Result<usize> {
    let mut dispatched = 0;
    for (adapter, response) in stock_responses() {
        let transport = Arc::new(Boundary {
            transport: Fixture::new(response),
            capacity: u64::from(capacity),
        });
        let mut selected = provider_with(adapter, "http://127.0.0.1:1234/v1", transport.clone());
        selected.capacity.context_tokens = capacity;
        let policy = CompactionPolicy::Threshold(
            acyclic_harness_core::context::ThresholdCompaction::for_capacity(selected.capacity, 0)?,
        );
        let result = run_stock_input(
            selected,
            adapter,
            policy,
            acyclic_fs::Fs::memory(),
            input.to_owned(),
        )
        .await;
        let sent = transport.transport.sent.load(Ordering::SeqCst);
        if sent > 0 {
            assert!(
                matches!(result, Ok(acyclic_harness_core::Outcome::Succeeded(_))),
                "{adapter:?}: {result:?}"
            );
        } else {
            assert!(
                !matches!(result, Ok(acyclic_harness_core::Outcome::Succeeded(_))),
                "silent native success without dispatch"
            );
        }
        dispatched += sent;
    }
    Ok(dispatched)
}

#[tokio::test]
async fn native_small_input_dispatches_and_oversized_mandatory_input_does_not() -> Result<()> {
    assert_eq!(check("hello", 8192).await?, 4);
    assert_eq!(check(&"x".repeat(16_384), 8192).await?, 0);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]
    #[test]
    fn arbitrary_native_input_never_crosses_catalog_capacity(
        input in ".{0,64}", copies in 1usize..256, capacity in 4_096u32..16_384,
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
            .block_on(check(&input.repeat(copies), capacity)).unwrap();
    }
}
