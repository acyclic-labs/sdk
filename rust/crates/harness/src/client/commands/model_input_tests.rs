use super::*;
use crate::{
    conversation::{FileDescriptor, FileRef},
    model::{FileProjectionPolicy, ImageInput, ModelContentPart},
};

fn selected_policy_survives(maximum_tokens: u32) {
    let base: FileRef =
        serde_json::from_str(include_str!("../../../fixtures/v2/file-ref.json")).unwrap();
    let file = FileRef::new(
        base.volume().clone(),
        "image.png".into(),
        "revision".into(),
        FileDescriptor::from_bytes(b"image", "image/png", &mut None).unwrap(),
        "image".into(),
        &mut None,
    )
    .unwrap();
    let image = ImageInput {
        maximum_tokens,
        ..Default::default()
    };
    let resolution = CommandResolution::Prompt {
        text: "inspect".into(),
        source: None,
        mentions: vec![FileMention {
            start: 0,
            end: 0,
            file: file.clone(),
            image_input: Some(image.clone()),
        }],
    };
    // The selected policy must survive the actual durable request codec before
    // conversion, independently of the provider's eventual projection.
    let decoded: CommandResolution =
        serde_json::from_slice(&serde_json::to_vec(&resolution).unwrap()).unwrap();
    assert_eq!(decoded, resolution);
    let input = decoded.into_model_input().unwrap();
    let ModelContentPart::File {
        file: actual,
        policy: FileProjectionPolicy::Native(policy),
    } = &input.parts()[1]
    else {
        panic!("image projection missing")
    };
    assert_eq!(actual, &file);
    assert_eq!(policy.maximum_bytes, image.maximum_bytes);
    assert_eq!(
        policy.maximum_work,
        u64::from(maximum_tokens),
        "selected image policy was replaced"
    );
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(32))]
    #[test]
    fn image_admission_policy_survives_request_codec_and_conversion(maximum_tokens in 1u32..u32::MAX) {
        selected_policy_survives(maximum_tokens);
    }
}

#[test]
fn hardcoded_image_allowance_is_detected_negative_control() {
    IGNORE_IMAGE_POLICY.with(|flag| flag.set(true));
    let caught = std::panic::catch_unwind(|| selected_policy_survives(20_000));
    IGNORE_IMAGE_POLICY.with(|flag| flag.set(false));
    let panic = caught.expect_err("hardcoding the production allowance must fail");
    let message = panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("selected image policy was replaced"),
        "{message}"
    );
}
