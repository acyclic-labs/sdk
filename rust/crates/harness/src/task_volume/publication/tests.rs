use super::*;
use acyclic_stream::MemoryStream;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn only_an_unchanged_history_prefix_can_be_retried(mutation in 0u8..6, text in "[a-z]{0,32}") {
        futures::executor::block_on(async {
            let volume = TaskVolume::open(Arc::new(MemoryStream::default()), StreamPath::new("retry-property").unwrap()).await.unwrap();
            let mut expected = volume.head().await.unwrap();
            let goal = Write::new(FileKey::Goal, b"client".to_vec());
            volume.append(expected, &goal).await.unwrap();
            match mutation {
                1 => expected.volume[0] ^= 1,
                2 => { let mut epoch = *expected.epoch.as_bytes(); epoch[0] ^= 1; expected.epoch = Uuid::from_bytes(epoch); },
                3 => expected.commit[0] ^= 1,
                4 => expected.position = u64::MAX,
                5 => expected.position = 0,
                _ => {},
            }
            let write = Write::new(FileKey::Task { task: Component::new("task").unwrap(),
                file: TaskFile::Tool { turn: 0, call: Component::new("bash").unwrap(), file: ToolFile::Stdout(0) } }, text.into_bytes());
            let result = publish(&volume, expected, &write, || std::future::ready(Ok::<_, Error>((Vec::new(), None)))).await;
            prop_assert_eq!(result.is_ok(), mutation == 0);
            let snapshot = volume.snapshot(&Query::Root, 8).await.unwrap();
            prop_assert_eq!(snapshot.files.contains(&write), mutation == 0);
            prop_assert!(snapshot.files.contains(&goal));
            Ok(())
        })?;
    }
}
