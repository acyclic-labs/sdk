//! The real bb2 llama.cpp recording must pass every shared inference vector.

async fn captured_backend()
-> acyclic_harness_core::Result<impl acyclic_harness_core::model::conformance::Backend> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/inference/llamacpp/manifest.json");
    acyclic_conformance::inference::replay::captured(&path)
}

acyclic_harness_core::model::conformance::suite!(
    captured_llamacpp_passes_the_complete_inference_suite,
    captured_backend
);

#[tokio::test]
async fn captured_inference_benchmark_checks_each_response_and_dispatch_budget()
-> acyclic_harness_core::Result<()> {
    let measurement =
        acyclic_harness_core::model::conformance::bench(&captured_backend().await?).await?;
    assert_eq!(measurement.completions, 64);
    assert_eq!(measurement.requests.data(), 64);
    assert_eq!(measurement.requests.control(), 0);
    Ok(())
}
