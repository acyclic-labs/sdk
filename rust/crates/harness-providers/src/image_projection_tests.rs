//! Exercise the full production projection, before HTTP serialization.
use super::*;

#[tokio::test]
async fn image_projection_storage_is_independent_of_payload_length() -> Result<()> {
    for (adapter, _) in fixtures() {
        let (mut provider, _) = provider(adapter, 200);
        enable(&mut provider);
        let mut costs = Vec::new();
        for length in [128, 4096] {
            let mut bytes = base64::engine::general_purpose::STANDARD
                .decode(PNG)
                .unwrap();
            bytes.resize(length, 0);
            let (file, _, _) = file(&bytes, "image/png", false, None).await?;
            let (prepared, _) =
                prepare(&provider, vec![user(vec![text("describe"), image(&file)])])?;
            let request = prepared.request();
            let names = crate::tool_names::Names::new(request);
            let origin = provider.reasoning_origin(provider.key.as_deref(), None);
            let mut text_files = crate::files::FileText::default();
            let mut body = None;
            let allocation = acyclic_test_support::measure(|| {
                body = Some(
                    crate::projection::body_with_images(
                        &provider,
                        request,
                        &names,
                        &origin,
                        std::iter::once(bytes.as_slice()),
                        &mut text_files,
                    )
                    .unwrap(),
                );
            });
            let body = body.unwrap();
            let mut output = Vec::with_capacity(body.capacity()?);
            let encoding = acyclic_test_support::measure(|| {
                serde_json::to_writer(&mut output, &body).unwrap()
            });
            assert_eq!(
                encoding.count_total, 0,
                "{adapter:?}: projection must write directly into the final buffer"
            );
            costs.push((allocation.count_total, allocation.bytes_total));
            let wire = checked_body(adapter, &output)?;
            assert!(
                serde_json::to_string(&wire)
                    .unwrap()
                    .contains(&base64::engine::general_purpose::STANDARD.encode(&bytes))
            );
        }
        assert_eq!(
            costs[0], costs[1],
            "{adapter:?}: building the wire graph copied an image"
        );
    }
    Ok(())
}
