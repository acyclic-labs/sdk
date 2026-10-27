use super::*;

// Internal native-event mapping vectors, not captured provider responses.
#[tokio::test]
async fn empty_data_is_a_noop_before_binary_output_and_exit() -> Result<(), ProviderError> {
    let events = [
        Event::Data(Data::Stdout(String::new())),
        Event::Data(Data::Stderr(String::new())),
        Event::Data(Data::Stdout(STANDARD.encode([0, 255]))),
        Event::Data(Data::Stderr(STANDARD.encode(b"err\0"))),
        Event::End(End {
            code: 7,
            exited: true,
            error: None,
        }),
    ];
    let frames: Vec<_> = futures::stream::iter(events.into_iter().map(Ok))
        .try_filter_map(|event: Event| async move { event.output() })
        .try_collect()
        .await?;
    assert_eq!(frames.len(), 3);
    assert!(matches!(&frames[0], ExecOutput::Stdout(bytes) if bytes == &[0, 255]));
    assert!(matches!(&frames[1], ExecOutput::Stderr(bytes) if bytes == b"err\0"));
    assert!(matches!(&frames[2], ExecOutput::ExitCode(7)));
    assert!(matches!(
        Event::Data(Data::Stdout("!".into())).output(),
        Err(ProviderError::Unavailable)
    ));
    assert!(matches!(
        Event::End(End {
            code: 0,
            exited: false,
            error: None
        })
        .output(),
        Err(ProviderError::Unavailable)
    ));
    Ok(())
}
