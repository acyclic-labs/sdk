//! The real bb2 llama.cpp recording must pass every shared inference vector.

#[tokio::test]
async fn captured_llamacpp_passes_the_complete_inference_suite() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("fixtures/inference/llamacpp/manifest.json");
    let cases = acyclic_conformance::inference::replay::qualify(&path)
        .await
        .expect("real llama.cpp captures must satisfy every inference case");
    assert_eq!(cases, acyclic_conformance::inference::SUITE);
}
