use super::*;
use crate::task_volume::{Channel, TaskFile};
use acyclic_stream::MemoryStream;
use proptest::prelude::*;

fn input(name: &str, bytes: &[u8]) -> FileValue {
    FileValue::new(
        FileKey::parse(&format!("workspace/native/{name}")).unwrap(),
        bytes.to_vec(),
    )
}

fn observed(output: &[u8], expected: &FileValue) {
    let text = std::str::from_utf8(output).unwrap();
    let mut lines = text.lines();
    let (query, snapshot) = jsonl::parse_snapshot(lines.next().unwrap()).unwrap();
    assert_eq!(query, Query::File(expected.key.clone()));
    let mut value = snapshot.files.first().map(|file| {
        file.segments
            .iter()
            .flat_map(|bytes| bytes.iter().copied())
            .collect::<Vec<_>>()
    });
    let mut cursor = snapshot.cursor;
    for line in lines {
        let change = jsonl::parse_change(line).unwrap();
        assert!(change.cursor.position > cursor.position);
        assert_eq!(change.cursor.query, cursor.query);
        if let Some(file) = change.file {
            assert_eq!(file.key, expected.key);
            value = Some(file.bytes.to_vec());
        }
        cursor = change.cursor;
    }
    assert_eq!(value.as_deref(), Some(expected.bytes.as_ref()));
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    // The canonical file codec preserves exact input bytes and cannot substitute
    // a task, an execution-owned file, a claimed requester or assistant channel.
    #[test]
    fn input_codec_round_trips_and_rejects_owner_substitution(
        task in any::<[u8;16]>(), name in "[a-z]{1,16}", bytes in proptest::collection::vec(any::<u8>(), 0..64),
    ) {
        let task = TaskId::from_bytes(task);
        let file = input(&name, &bytes);
        prop_assert_eq!(parse(task, &jsonl::request(&file)?)?, file.clone());
        let own = Component::new(task.to_string()).unwrap();
        let mut request = FileValue::new(FileKey::Task { task:own.clone(),
            file:TaskFile::Request(Component::new("operation").unwrap()) }, bytes.clone());
        prop_assert_eq!(parse(task, &jsonl::request(&request)?)?, request.clone());
        let mut foreign = task.into_bytes();foreign[0] ^= 1;
        request.key = FileKey::Task { task:Component::new(TaskId::from_bytes(foreign).to_string()).unwrap(),
            file:TaskFile::Request(Component::new("operation").unwrap()) };
        prop_assert!(parse(task, &jsonl::request(&request)?).is_err());
        request.key = FileKey::Task {task:own,file:TaskFile::Status};
        prop_assert!(parse(task, &jsonl::request(&request)?).is_err());
        let mut forged = file.clone();forged.requester = Some(task);
        prop_assert!(parse(task, &jsonl::request(&forged)?).is_err());
        let mut forged = file;forged.channel = Some(Channel::Text);
        prop_assert!(parse(task, &jsonl::request(&forged)?).is_err());
    }

    // Each input is accepted independently, followed at its original cut, and
    // an exact replay emits the same snapshot without adding a durable record.
    #[test]
    fn immutable_upload_replay_preserves_bytes_and_publication_identity(
        bytes in proptest::collection::vec(any::<u8>(), 0..96),
    ) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let provider = Arc::new(MemoryStream::default());
            let files = TaskVolume::open(provider, StreamPath::new("native-inputs").unwrap()).await.unwrap();
            let source = input("cut/chunk", &bytes);
            let manifest = input("cut/manifest.json", b"{\"chunks\":1}");
            let lines = format!("{}\n{}\n",jsonl::request(&source).unwrap(),jsonl::request(&manifest).unwrap());
            let task = TaskId::from_bytes([0;16]);
            let mut output = Vec::new();
            serve(&files, task, manifest.key.clone(), &mut lines.as_bytes(), &mut output).await.unwrap();
            observed(&output, &manifest);
            let cut = files.head().await.unwrap();
            let mut replay = Vec::new();
            serve(&files, task, manifest.key.clone(), &mut lines.as_bytes(), &mut replay).await.unwrap();
            observed(&replay, &manifest);
            assert_eq!(files.head().await.unwrap(), cut);
            let mut changed = source;
            let mut replacement = bytes;replacement.push(0);
            changed.bytes = replacement.into();
            let line = jsonl::request(&changed).unwrap();
            assert!(serve(&files,task,manifest.key.clone(),&mut line.as_bytes(),&mut Vec::new()).await.is_err());
            assert_eq!(files.head().await.unwrap(), cut);
        });
    }
}

#[tokio::test]
async fn bounded_lines_and_file_client_future_do_not_grow_with_history() -> Result<()> {
    let mut output = Vec::new();
    let files = TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("future").map_err(storage)?,
    )
    .await
    .map_err(storage)?;
    let mut bytes = &b""[..];
    let future = serve(
        &files,
        TaskId::from_bytes([0; 16]),
        input("manifest", b"").key,
        &mut bytes,
        &mut output,
    );
    let size = std::mem::size_of_val(&future);
    eprintln!("native file client future: {size} bytes");
    assert!(
        size < 16 * 1024,
        "native file client future grew to {size} bytes"
    );
    drop(future);
    let expected = input("boundary", b"retained");
    let mut exact = jsonl::request(&expected)?.into_bytes();
    exact.resize(jsonl::MAXIMUM_REQUEST_BYTES, b' ');
    exact.push(b'\n');
    let mut buffer = Vec::new();
    assert!(line(&mut exact.as_slice(), &mut buffer).await?);
    assert_eq!(buffer.len(), jsonl::MAXIMUM_REQUEST_BYTES + 1);
    assert_eq!(
        parse(
            TaskId::from_bytes([0; 16]),
            std::str::from_utf8(&buffer).unwrap()
        )?,
        expected
    );
    buffer.clear();
    exact.pop();
    assert!(line(&mut exact.as_slice(), &mut buffer).await?);
    assert_eq!(buffer.len(), jsonl::MAXIMUM_REQUEST_BYTES);
    assert_eq!(
        parse(
            TaskId::from_bytes([0; 16]),
            std::str::from_utf8(&buffer).unwrap()
        )?,
        expected
    );
    let oversized = vec![b'x'; jsonl::MAXIMUM_REQUEST_BYTES + 1];
    buffer.clear();
    assert!(line(&mut oversized.as_slice(), &mut buffer).await.is_err());
    Ok(())
}
