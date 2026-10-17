use super::*;
use crate::task_volume::{FileKey, FileValue, InputPath, Query, TaskVolume};
use acyclic_stream::{MemoryStream, StreamPath};
use sha2::{Digest as _, Sha256};
use std::sync::Arc;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]
    // Uploaded files, exact native cuts and child seed rebasing preserve raw
    // ancestry; source, leaf, invocation and workspace substitution all fail.
    #[test]
    fn native_source_selection_round_trips_and_preserves_recursive_ancestry(text in "[^\\x00]{1,48}", history in 0usize..4) {
        tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
            let source=header();let session=source["id"].as_str().unwrap();
            let mut rows=Vec::new();let mut parent=None;
            for index in 0..history {
                let id=format!("user-{index}"); rows.push(user(&id,parent.as_deref(),&text));parent=Some(id);
            }
            rows.push(assistant("invoking",parent.as_deref(),&[("spawn","acyclic_spawn")]));
            let original=bytes(&source,&rows);
            let sha256:[u8;32]=Sha256::digest(&original).into();
            let prefix=format!("workspace/native/{session}/{}",hex::encode(sha256));
            let selected=super::super::source::Selection {manifest:InputPath::try_from(format!("{prefix}/manifest.json")).unwrap(),head:Some("invoking".into()),sha256};
            let encoded=serde_json::to_vec(&selected).unwrap();
            assert_eq!(serde_json::from_slice::<super::super::source::Selection>(&encoded).unwrap(),selected);
            let files=TaskVolume::open(Arc::new(MemoryStream::default()),StreamPath::new("native-context").unwrap()).await.unwrap();
            for (index,chunk) in original.chunks(8192).enumerate() {
                let file=FileValue::new(FileKey::parse(&format!("{prefix}/chunk-{index}")).unwrap(),chunk.to_vec());
                files.append(files.head().await.unwrap(),&file).await.unwrap();
            }
            let manifest=FileValue::new(FileKey::Input(selected.manifest.clone()),serde_json::to_vec(&json!({"chunks":original.len().div_ceil(8192),"length":original.len()})).unwrap());
            files.append(files.head().await.unwrap(),&manifest).await.unwrap();
            let (loaded,_)=selected.read(&files,session,original.len() as u64).await.unwrap();
            assert_eq!(loaded,original);
            let (_,cut)=selected.select(&loaded,session,std::path::Path::new("/project"),"spawn").unwrap();
            assert_eq!(cut.through,parent);
            assert!(selected.select(&loaded,session,std::path::Path::new("/foreign"),"spawn").is_err());
            assert!(selected.select(&loaded,"00000000-0000-0000-0000-000000000002",std::path::Path::new("/project"),"spawn").is_err());
            assert!(selected.select(&loaded,session,std::path::Path::new("/project"),"another-call").is_err());
            let mut child=source.clone();child["id"]=json!("00000000-0000-0000-0000-000000000003");
            let seed=SessionFile::decode(&loaded,loaded.len() as u64).unwrap().child_file(selected.head.as_deref(),&cut,&serde_json::to_vec(&child).unwrap(),std::path::Path::new("/project")).unwrap();
            let seed=super::super::header::rebase(&seed,child["id"].as_str().unwrap(),std::path::Path::new("/child")).unwrap();
            let seeded=SessionFile::decode(&seed,seed.len() as u64).unwrap();
            assert_eq!(seeded.header.cwd,"/child");
            assert_eq!(seeded.rows.iter().map(|row|row.bytes).collect::<Vec<_>>(),SessionFile::decode(&loaded,loaded.len() as u64).unwrap().require_cut(selected.head.as_deref(),&cut).unwrap());
            let chunk=FileValue::new(FileKey::parse(&format!("{prefix}/chunk-0")).unwrap(),b"changed source".to_vec());
            files.append(files.head().await.unwrap(),&chunk).await.unwrap();
            assert!(selected.read(&files,session,original.len() as u64).await.is_err());
            assert_eq!(files.snapshot(&Query::File(chunk.key.clone()),1).await.unwrap().files,vec![chunk]);
        });
    }
}

#[tokio::test]
async fn source_file_boundaries_keep_small_futures() {
    let files = TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("source-future").unwrap(),
    )
    .await
    .unwrap();
    let selected = super::super::source::Selection {
        manifest: InputPath::try_from("workspace/native/source/manifest.json".to_owned()).unwrap(),
        head: None,
        sha256: [0; 32],
    };
    let head = files.head().await.unwrap();
    let condition = acyclic_stream::CommitCondition::Head {
        path: files.path().clone(),
        expected: head.position,
        commit_id: acyclic_stream::CommitId::from_bytes(head.commit),
    };
    fn size(name: &str, future: impl Future) {
        let bytes = std::mem::size_of_val(&future);
        eprintln!("{name}: {bytes} bytes");
        assert!(bytes < 16 * 1024, "{name} future grew to {bytes} bytes");
    }
    size("native source read", selected.read(&files, "session", 1));
    size(
        "native source publication",
        selected.publish(
            &files,
            &files,
            ("session", 1, condition.clone()),
            &FileValue::new(FileKey::Goal, vec![]),
            || std::future::ready(Ok((condition.clone(), None))),
        ),
    );
}
