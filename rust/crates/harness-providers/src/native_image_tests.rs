use super::*;
use acyclic_harness_core::model::{ImageDetail, NativeMediaIntent, NativeMediaPolicy};
use base64::{Engine as _, engine::general_purpose::STANDARD};

fn policy(bytes: usize) -> FileProjectionPolicy {
    FileProjectionPolicy::Native(Box::new(NativeMediaPolicy {
        intent: NativeMediaIntent::Image {
            detail: ImageDetail::Auto,
        },
        maximum_bytes: bytes as u64,
        maximum_work: 4096,
        configuration: None,
    }))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn native_image_wire_roundtrips_every_format_and_adapter(bytes in prop::collection::vec(any::<u8>(), 0..256)) {
        crate::projection::image::assert_roundtrip(&bytes);
    }
}

#[tokio::test]
async fn maximum_work_one_rejects_images_before_reading_for_every_adapter() -> Result<()> {
    let bytes = b"\x89PNG\r\n\x1a\n\xff";
    for (adapter, _) in fixtures() {
        let (mut provider, _) = provider(adapter, 200);
        let reader = Arc::new(Reader { data: bytes.to_vec(), reads: AtomicUsize::new(0) });
        provider.files = crate::files::Files { reader: Some(reader.clone()), maximum_bytes: 1024 };
        let mut policy = policy(bytes.len());
        let FileProjectionPolicy::Native(native) = &mut policy else { unreachable!() };
        native.maximum_work = 1;
        let request = with_parts(&provider, vec![ModelContentPart::File {
            file: file(bytes, "image/png")?, policy,
        }])?;
        let error = crate::dispatch::encode(&provider, &request,
            &crate::tool_names::Names::new(request.request()), None,
            &provider.reasoning_origin(provider.key.as_deref(), None)).await.unwrap_err();
        assert!(error.to_string().contains("projection work limit"));
        assert_eq!(reader.reads.load(Ordering::SeqCst), 0);
    }
    Ok(())
}

#[tokio::test]
async fn native_images_use_verified_bytes_and_keep_mixed_content_order() -> Result<()> {
    let bytes = b"\x89PNG\r\n\x1a\n\xff\x00\x80";
    for (adapter, _) in fixtures() {
        let (mut provider, _) = provider(adapter, 200);
        let reader = Arc::new(Reader {
            data: bytes.to_vec(),
            reads: AtomicUsize::new(0),
        });
        provider.files = crate::files::Files {
            reader: Some(reader.clone()),
            maximum_bytes: 1024,
        };
        let image = file(bytes, "image/png")?;
        let request = with_parts(
            &provider,
            vec![
                ModelContentPart::Text {
                    text: "before".into(),
                },
                ModelContentPart::File {
                    file: image.clone(),
                    policy: policy(bytes.len()),
                },
                ModelContentPart::Text {
                    text: "after".into(),
                },
            ],
        )?;
        let encoded = crate::dispatch::encode(
            &provider,
            &request,
            &crate::tool_names::Names::new(request.request()),
            None,
            &provider.reasoning_origin(provider.key.as_deref(), None),
        )
        .await?;
        let body: Value = serde_json::from_slice(&encoded).unwrap();
        assert!(crate::validate_input(adapter, &body).is_empty(), "{body}");
        let parts = match adapter {
            Adapter::Responses => body["input"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|item| item["content"].as_array().unwrap())
                .collect::<Vec<_>>(),
            Adapter::Chat | Adapter::Anthropic => body["messages"][0]["content"]
                .as_array()
                .unwrap()
                .iter()
                .collect(),
            Adapter::Gemini => body["contents"][0]["parts"]
                .as_array()
                .unwrap()
                .iter()
                .collect(),
        };
        assert_eq!(parts.len(), 3);
        assert_eq!(parts[0]["text"], "before");
        assert_eq!(parts[2]["text"], "after");
        let wire = serde_json::to_string(parts[1]).unwrap();
        assert!(wire.contains(&STANDARD.encode(bytes)));
        assert!(!wire.contains("[file:"));
        assert_eq!(reader.reads.load(Ordering::SeqCst), 1);
        assert_eq!(image.descriptor().media_type(), "image/png");
    }
    Ok(())
}

#[tokio::test]
async fn image_digest_type_and_size_mismatches_cannot_reach_projection() -> Result<()> {
    let expected = b"\x89PNG\r\n\x1a\n\xff";
    for (data, media, maximum) in [
        (b"\x89PNG\r\n\x1a\n\x00".as_slice(), "image/png", 1024),
        (expected.as_slice(), "image/jpeg", 1024),
        (expected.as_slice(), "image/png", 1),
    ] {
        let (mut provider, script) = provider(Adapter::Responses, 200);
        let reader = Arc::new(Reader {
            data: data.to_vec(),
            reads: AtomicUsize::new(0),
        });
        provider.files = crate::files::Files {
            reader: Some(reader),
            maximum_bytes: maximum,
        };
        let request = with_parts(
            &provider,
            vec![ModelContentPart::File {
                file: file(expected, media)?,
                policy: policy(expected.len()),
            }],
        )?;
        assert!(
            crate::dispatch::encode(
                &provider,
                &request,
                &crate::tool_names::Names::new(request.request()),
                None,
                &provider.reasoning_origin(provider.key.as_deref(), None)
            )
            .await
            .is_err()
        );
        assert!(script.sent.lock().unwrap().is_empty());
    }
    Ok(())
}

#[test]
fn image_and_text_resolution_cannot_be_confused() {
    let mut text =
        crate::files::FileText(vec![crate::files::ResolvedFile::Text("body".into())].into_iter());
    assert!(text.image().is_err());
    let mut image =
        crate::files::FileText(vec![crate::files::ResolvedFile::Image("/w==".into())].into_iter());
    assert!(
        image
            .project(
                &file(b"body", "text/plain").unwrap(),
                &FileProjectionPolicy::BoundedFull
            )
            .is_err()
    );
}
