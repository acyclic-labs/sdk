//! Canonical Harness value admission, including unordered extension maps.
use acyclic_harness_core::{conversation as c, resources::ProviderRef, wire};
use prost::Message;
use std::fmt::Debug;

fn project<W, S>(bytes: &[u8], invalidate: impl FnOnce(&mut W))
where
    W: Message + Default + Clone + for<'a> From<&'a S>,
    S: TryFrom<W> + Clone + Debug + PartialEq,
    S::Error: Debug,
{
    super::project(bytes, S::try_from, |value| W::from(&value));
    if let Ok(mut wire) = W::decode(bytes)
        && S::try_from(wire.clone()).is_ok()
    {
        invalidate(&mut wire);
        assert!(
            S::try_from(wire).is_err(),
            "missing required value admitted"
        );
    }
}

pub(super) fn check(name: &str, bytes: &[u8]) {
    match name {
        "acyclic.harness.v2.ProviderRef" => {
            project::<wire::ProviderRef, ProviderRef>(bytes, |wire| wire.namespace.clear());
        }
        "acyclic.harness.v2.VolumeRef" => {
            project::<wire::VolumeRef, c::VolumeRef>(bytes, |wire| wire.owner = None);
        }
        "acyclic.harness.v2.FileDescriptor" => {
            project::<wire::FileDescriptor, c::FileDescriptor>(bytes, |wire| wire.sha256.clear());
        }
        "acyclic.harness.v2.FileRef" => {
            project::<wire::FileRef, c::FileRef>(bytes, |wire| wire.descriptor = None);
        }
        "acyclic.harness.v2.Attachment" => {
            project::<wire::Attachment, c::Attachment>(bytes, |wire| wire.file = None);
        }
        "acyclic.harness.v2.ReferencedAttachments" => {
            project::<wire::ReferencedAttachments, c::ReferencedAttachments>(bytes, |wire| {
                wire.source = None;
            });
        }
        "acyclic.harness.v2.ConversationMessage" => {
            project::<wire::ConversationMessage, c::ConversationMessage>(bytes, |wire| {
                wire.sequence = 0;
            });
        }
        _ => {}
    }
}

type Seeds = Vec<(&'static str, &'static str, Vec<u8>)>;

fn seed<W, S>(
    outputs: &mut Seeds,
    name: &'static str,
    variant: &'static str,
    value: &S,
) -> Result<(), Box<dyn std::error::Error>>
where
    W: Message + Default + for<'a> From<&'a S>,
    S: TryFrom<W> + Debug + PartialEq,
    S::Error: Debug,
{
    let encoded = W::from(value).encode_to_vec();
    let restored = S::try_from(W::decode(encoded.as_slice())?)
        .map_err(|error| format!("Harness writer {name}/{variant} refused: {error:?}"))?;
    if &restored != value {
        return Err(format!("Harness writer {name}/{variant} changed its semantic value").into());
    }
    outputs.push((name, variant, encoded));
    Ok(())
}

pub(super) fn seeds() -> Result<Seeds, Box<dyn std::error::Error>> {
    let mut outputs = Vec::new();
    macro_rules! capture {
        ($ty:ident, $variant:expr, $value:expr) => {
            seed::<wire::$ty, _>(
                &mut outputs,
                concat!("acyclic.harness.v2.", stringify!($ty)),
                $variant,
                $value,
            )?
        };
    }
    let provider = ProviderRef::new("example".into(), "filesystem".into(), "1".into(), &mut None)?;
    capture!(ProviderRef, "admitted", &provider);
    let mut volumes = Vec::new();
    for (variant, class, owner) in [
        (
            "project",
            c::VolumeClass::Project,
            c::VolumeOwner::Project("project".into()),
        ),
        (
            "agent",
            c::VolumeClass::AgentPrivate,
            c::VolumeOwner::Agent(acyclic_harness_core::AgentId::from_bytes([1; 16])),
        ),
        (
            "session",
            c::VolumeClass::SessionShared,
            c::VolumeOwner::Session("session".into()),
        ),
    ] {
        let volume = c::VolumeRef::new(provider.clone(), variant.into(), class, owner, &mut None)?;
        capture!(VolumeRef, variant, &volume);
        volumes.push(volume);
    }
    let descriptor = c::FileDescriptor::from_bytes(b"codec", "text/plain", &mut None)?;
    capture!(FileDescriptor, "content", &descriptor);
    let file = c::FileRef::new(
        volumes[1].clone(),
        "content.txt".into(),
        "one".into(),
        descriptor,
        "content.txt".into(),
        &mut None,
    )?;
    capture!(FileRef, "content", &file);
    let attachment = c::Attachment {
        file: file.clone(),
        label: Some("content".into()),
    };
    capture!(Attachment, "admitted", &attachment);
    let manifest_bytes = c::encode_attachment_manifest(std::slice::from_ref(&attachment))?;
    let descriptor = c::FileDescriptor::from_bytes(
        &manifest_bytes,
        c::ATTACHMENT_MANIFEST_MEDIA_TYPE,
        &mut None,
    )?;
    capture!(FileDescriptor, "manifest", &descriptor);
    let manifest = c::FileRef::new(
        volumes[1].clone(),
        "attachments.json".into(),
        "one".into(),
        descriptor,
        "attachments.json".into(),
        &mut None,
    )?;
    capture!(FileRef, "manifest", &manifest);
    let inline = c::ReferencedAttachments::Inline {
        items: vec![attachment],
    };
    capture!(ReferencedAttachments, "inline", &inline);
    let referenced = c::ReferencedAttachments::Manifest {
        manifest,
        item_count: 1,
    };
    capture!(ReferencedAttachments, "manifest", &referenced);
    for (variant, kind) in [
        ("user", c::MessageKind::User),
        ("assistant", c::MessageKind::Assistant),
        ("system", c::MessageKind::System),
        ("tool-call", c::MessageKind::ToolCall),
        ("tool-result", c::MessageKind::ToolResult),
        ("interaction", c::MessageKind::Interaction),
        ("permission", c::MessageKind::Permission),
        ("fork", c::MessageKind::Fork),
        ("merge", c::MessageKind::Merge),
    ] {
        let tool_call_id = matches!(kind, c::MessageKind::ToolCall | c::MessageKind::ToolResult)
            .then(|| "call".into());
        let message = c::ConversationMessage {
            id: "01010101-0101-0101-0101-010101010101".parse()?,
            sequence: 1,
            kind,
            content: file.clone(),
            attachments: inline.clone(),
            reply_to: Some("02020202-0202-0202-0202-020202020202".parse()?),
            tool_call_id,
            extensions: [
                ("example.first".into(), file.clone()),
                ("example.second".into(), file.clone()),
            ]
            .into(),
        };
        capture!(ConversationMessage, variant, &message);
    }
    Ok(outputs)
}
