use super::*;

#[tokio::test]
async fn later_model_references_fail_preflight_before_earlier_content_reads() -> Result<()> {
    let journal = Journal::default();
    let first = journal
        .stage(OperationId::new(), "first", &vec![1, 2], "image/png", None)?
        .await?;
    let second = journal
        .stage(OperationId::new(), "second", &vec![3; 8], "image/png", None)?
        .await?;
    let contents = [first.clone(), second.clone()].map(|file| {
        ModelContent::Part(ModelContentPart::File {
            file,
            policy: crate::model::FileProjectionPolicy::Reference,
        })
    });
    let first_only = RuntimeScope::new(
        Capabilities::new([first.read_capability()?]),
        Limits::default(),
    )?;
    assert!(matches!(
        verify_model_contents_scoped(
            &journal,
            contents.iter(),
            Limits::default(),
            Some(&first_only),
            None
        )
        .await,
        Err(Error::Unauthorized(_))
    ));
    assert_eq!(journal.4.load(Ordering::SeqCst), 0);
    let grants = Capabilities::new([first.read_capability()?, second.read_capability()?]);
    let narrow = RuntimeScope::new(
        grants.clone(),
        Limits {
            file_bytes: 4,
            ..Limits::default()
        },
    )?;
    assert!(matches!(
        verify_model_contents_scoped(
            &journal,
            contents.iter(),
            Limits::default(),
            Some(&narrow),
            None
        )
        .await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(journal.4.load(Ordering::SeqCst), 0);
    let allowed = RuntimeScope::new(grants, Limits::default())?;
    verify_model_contents_scoped(
        &journal,
        contents.iter(),
        Limits::default(),
        Some(&allowed),
        None,
    )
    .await?;
    assert_eq!(journal.4.load(Ordering::SeqCst), 2);
    Ok(())
}
