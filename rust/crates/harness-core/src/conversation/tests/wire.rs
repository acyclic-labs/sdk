use super::*;

#[test]
fn v2_fixtures_round_trip_canonically_and_replay() -> Result<()> {
    fn round_trip<T: serde::de::DeserializeOwned + Serialize>(fixture: &str) -> Result<T> {
        let fixture = fixture.trim();
        let value: T =
            serde_json::from_str(fixture).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(
            serde_json::to_string(&value).map_err(|error| Error::Invalid(error.to_string()))?,
            fixture
        );
        Ok(value)
    }
    round_trip::<ConversationMessage>(include_str!(
        "../../../../harness/fixtures/v2/conversation-message.json"
    ))?
    .validate(&mut None)?;
    round_trip::<FileRef>(include_str!(
        "../../../../harness/fixtures/v2/file-ref.json"
    ))?;
    round_trip::<PrivateDirectoryPage>(include_str!(
        "../../../../harness/fixtures/v2/private-directory-page.json"
    ))?
    .validate()?;
    round_trip::<TaskOutcomeRecord>(include_str!(
        "../../../../harness/fixtures/v2/task-outcome.json"
    ))?
    .validate()?;
    let messages: Vec<ConversationMessage> = round_trip(include_str!(
        "../../../../harness/fixtures/v2/conversation-kinds.json"
    ))?;
    let mut state = ConversationState::default();
    state.bind(AgentId::from_bytes([1; 16]))?;
    for message in &messages {
        state.append(message.clone(), &mut None)?;
    }
    assert_eq!(state.messages.len(), 9);
    Ok(())
}

#[test]
fn v2_file_reference_security_cases_match_the_shared_fixture() -> Result<()> {
    let base: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../harness/fixtures/v2/file-ref.json"
    ))
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let corpus: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../harness/fixtures/v2/file-ref-security-cases.json"
    ))
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let cases = corpus["cases"]
        .as_array()
        .ok_or_else(|| Error::Invalid("security cases array missing".into()))?;
    assert_eq!(cases.len(), 13);
    for case in cases {
        let name = case["name"]
            .as_str()
            .ok_or_else(|| Error::Invalid("security case name missing".into()))?;
        let pointer = case["pointer"]
            .as_str()
            .ok_or_else(|| Error::Invalid("security case JSON pointer missing".into()))?;
        let (parent, key) = pointer
            .rsplit_once('/')
            .ok_or_else(|| Error::Invalid("security case pointer segment missing".into()))?;
        let mut modified = base.clone();
        let parent = modified
            .pointer_mut(parent)
            .ok_or_else(|| Error::Invalid("security fixture parent missing".into()))?
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("security fixture parent is not an object".into()))?;
        parent.insert(key.to_owned(), case["value"].clone());
        if case["check"] == "decode" {
            assert!(
                serde_json::from_value::<FileRef>(modified).is_err(),
                "{name}"
            );
        } else {
            assert_eq!(case["check"], "bytes", "{name}");
            let reference: FileRef = serde_json::from_value(modified)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            assert!(
                reference.descriptor().verify(b"hello", &mut None).is_err(),
                "{name}"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn composite_manifest_admission_routes_members_to_exact_providers() -> Result<()> {
    struct StaticContent {
        file: FileRef,
        bytes: Vec<u8>,
    }
    impl ContentResidencyVerifier for StaticContent {
        fn read<'a>(
            &'a self,
            reference: &'a FileRef,
            maximum_bytes: u64,
            io: Option<&'a ContentIo>,
        ) -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
            BoundedContentFuture::new(io, || async move {
                if reference != &self.file {
                    return Err(Error::NotFound("file".into()));
                }
                if self.bytes.len() as u64 > maximum_bytes {
                    return Err(Error::Invalid(
                        "content exceeds selected byte allowance".into(),
                    ));
                }
                Ok(self.bytes.clone().into())
            })
        }
    }
    let owner = VolumeOwner::Agent(AgentId::from_bytes([1; 16]));
    let manifest_provider =
        ProviderRef::new("test".into(), "filesystem".into(), "a".into(), &mut None)?;
    let member_provider =
        ProviderRef::new("test".into(), "filesystem".into(), "b".into(), &mut None)?;
    let member = FileRef::new(
        VolumeRef::new(
            member_provider.clone(),
            "private".into(),
            VolumeClass::AgentPrivate,
            owner.clone(),
            &mut None,
        )?,
        ("attachments/member.txt").into(),
        ("one").into(),
        FileDescriptor::from_bytes(&vec![b'x'; 1_024], "text/plain", &mut None)?,
        ("member.txt").into(),
        &mut None,
    )?;
    let items = vec![Attachment {
        file: member.clone(),
        label: None,
    }];
    let bytes = serde_json::to_vec(&items).map_err(|error| Error::Invalid(error.to_string()))?;
    let manifest = FileRef::new(
        VolumeRef::new(
            manifest_provider.clone(),
            "private".into(),
            VolumeClass::AgentPrivate,
            owner,
            &mut None,
        )?,
        ("attachments/list.json").into(),
        ("one").into(),
        FileDescriptor::from_bytes(
            &bytes,
            "application/vnd.acyclic.harness.attachments+json",
            &mut None,
        )?,
        ("list.json").into(),
        &mut None,
    )?;
    let first = Arc::new(StaticContent {
        file: manifest.clone(),
        bytes,
    });
    let second = Arc::new(StaticContent {
        file: member.clone(),
        bytes: vec![b'x'; 1_024],
    });
    let composite = CompositeContentVerifier::new(
        vec![
            (manifest.volume().clone(), first.clone()),
            (member.volume().clone(), second.clone()),
        ],
        None,
    )?;
    composite
        .verify_manifest(&manifest, 1, &Limits::default(), None)?
        .await?;
    let mut narrow = Limits::default();
    narrow.file_bytes = manifest.descriptor().byte_length().saturating_add(1);
    narrow.render_bytes = narrow.file_bytes;
    assert!(narrow.file_bytes < member.descriptor().byte_length());
    assert!(matches!(
        composite
            .verify_manifest(&manifest, 1, &narrow, None)?
            .await,
        Err(Error::Invalid(_))
    ));
    let incomplete =
        CompositeContentVerifier::new(vec![(manifest.volume().clone(), first.clone())], None)?;
    assert!(matches!(
        incomplete
            .verify_manifest(&manifest, 1, &Limits::default(), None)?
            .await,
        Err(Error::Unsupported(_))
    ));
    struct StaticMount {
        volume: VolumeRef,
        reader: Arc<dyn ContentResidencyVerifier>,
        grant: ContentGrant,
    }
    impl ContentMountResolver for StaticMount {
        fn mount<'a>(
            &'a self,
            reference: &'a FileRef,
        ) -> BoxFuture<'a, Result<(ContentGrant, Arc<dyn ContentResidencyVerifier>)>> {
            Box::pin(async move {
                if reference.volume() != &self.volume {
                    return Err(Error::Unauthorized("foreign volume was not granted".into()));
                }
                Ok((self.grant.clone(), self.reader.clone()))
            })
        }
    }
    let issuer = crate::core::AuthorityIssuer::new(
        "mount-owner",
        [18; 32],
        crate::core::Authority {
            kind: crate::core::AggregateKind::Agent,
            id: AgentId::from_bytes([1; 16]).to_string(),
        },
    );
    let scope = issuer
        .issue(
            "exact-member",
            &crate::Capabilities::new([member.read_capability(&mut None)?]),
            Some(AgentId::from_bytes([1; 16])),
            None,
            &mut None,
        )
        .expect("scope proof");
    let grant = ContentGrant::verify_file_read(
        &issuer.verifier(&mut None).expect("verifier copy"),
        &scope,
        &member,
    )?;
    let lazy = CompositeContentVerifier::new(
        vec![(manifest.volume().clone(), first)],
        Some(Arc::new(StaticMount {
            volume: member.volume().clone(),
            reader: second,
            grant,
        })),
    )?;
    lazy.verify_manifest(&manifest, 1, &Limits::default(), None)?
        .await?;
    let unauthorized = FileRef::new(
        member.volume().clone(),
        ("attachments/another.txt").into(),
        ("one").into(),
        FileDescriptor::from_bytes(b"another", "text/plain", &mut None)?,
        ("another.txt").into(),
        &mut None,
    )?;
    assert!(matches!(
        lazy.verify(&unauthorized, unauthorized.descriptor().byte_length(), None)?
            .await,
        Err(Error::Unauthorized(_))
    ));
    let same_provider_member = FileRef::new(
        VolumeRef::new(
            manifest_provider,
            "private".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([2; 16])),
            &mut None,
        )?,
        ("attachments/other-owner.txt").into(),
        ("one").into(),
        FileDescriptor::from_bytes(b"other-owner", "text/plain", &mut None)?,
        ("other-owner.txt").into(),
        &mut None,
    )?;
    let same_provider_items = vec![Attachment {
        file: same_provider_member.clone(),
        label: None,
    }];
    let same_provider_bytes = serde_json::to_vec(&same_provider_items)
        .map_err(|error| Error::Invalid(error.to_string()))?;
    let same_provider_manifest = FileRef::new(
        manifest.volume().clone(),
        ("attachments/other-owner-list.json").into(),
        ("one").into(),
        FileDescriptor::from_bytes(
            &same_provider_bytes,
            "application/vnd.acyclic.harness.attachments+json",
            &mut None,
        )?,
        ("other-owner-list.json").into(),
        &mut None,
    )?;
    let same_provider = CompositeContentVerifier::new(
        vec![
            (
                same_provider_manifest.volume().clone(),
                Arc::new(StaticContent {
                    file: same_provider_manifest.clone(),
                    bytes: same_provider_bytes,
                }),
            ),
            (
                same_provider_member.volume().clone(),
                Arc::new(StaticContent {
                    file: same_provider_member,
                    bytes: b"other-owner".to_vec(),
                }),
            ),
        ],
        None,
    )?;
    same_provider
        .verify_manifest(&same_provider_manifest, 1, &Limits::default(), None)?
        .await?;
    Ok(())
}
