include!("../../harness/tests/support/composer.rs");

pub(super) async fn collect<P, A, O>(
    mut recipe: Value,
    paths: Vec<String>,
    filesystem: acyclic_fs::Fs<A, O>,
    stream: std::sync::Arc<P>,
) -> TestResult<Capture>
where
    P: acyclic_stream::StreamProvider,
    A: acyclic_fs::AsyncAuthorityStore + 'static,
    O: acyclic_fs::AsyncObjectStore + 'static,
{
    let object = recipe
        .as_object_mut()
        .ok_or("composer fixture must be an object")?;
    if object.contains_key("expect") {
        return Err("host fixture must not contain expectations".into());
    }
    object.remove("driver");
    // Schema-only placeholders: collection never reads oracle values.
    let files: BTreeMap<_, Option<String>> = paths.into_iter().map(|path| (path, None)).collect();
    let mut expected = serde_json::json!({"result":{"fixture":true},"files":files});
    if object.get("complete").is_some_and(Value::is_string) {
        expected
            .as_object_mut()
            .ok_or("missing placeholder object")?
            .insert("completions".into(), serde_json::json!([]));
    }
    object.insert("expect".into(), expected);
    let case = Composer::parse(recipe)?;
    let storage = MemoryHarnessStorage::from_providers(
        acyclic_harness::AgentId::new(),
        1_048_576,
        filesystem,
        stream,
    )
    .await?;
    Box::pin(case.capture(&storage)).await
}
