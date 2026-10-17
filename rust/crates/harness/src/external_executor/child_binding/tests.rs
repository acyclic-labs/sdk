use super::*;
use crate::external_executor::{ExternalHost, request};
use acyclic_stream::{MemoryStream, StreamPath};
use proptest::prelude::*;
use serde_json::json;
use std::sync::Arc;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    // A label selects exactly the child committed by its parent. Swapping a
    // label/session/parent cannot redirect stop, even when both children exist.
    #[test]
    fn swapped_labels_and_sessions_cannot_change_a_committed_child(
        parent in "[a-z]{1,16}", label in "[a-zA-Z0-9_/.-]{1,20}", session in "[a-z]{1,20}",
        host in 0usize..3, child_host in 0usize..3,
    ) {
        let hosts = [ExternalHost::Codex,ExternalHost::ClaudeCode,ExternalHost::Pi];
        let host = hosts[host];
        let child_host = hosts[child_host];
        let event = |start| match (host == ExternalHost::Pi,start) {
            (true,true)=>"subagent_start",(true,false)=>"subagent_stop",
            (false,true)=>"SubagentStart",(false,false)=>"SubagentStop",
        };
        let input = |label:&str,session:&str| json!({"sessionId":parent,"agentId":label,
            "parentAgentId":"parent-caller","childSession":{"host":child_host,"session_id":session,"caller":null}});
        let label_a = format!("a-{label}");
        let label_b = format!("b-{label}");
        let session_a = format!("a-{session}");
        let session_b = format!("b-{session}");
        let fork_a = request(host,event(true),input(&label_a,&session_a))?;
        let fork_b = request(host,event(true),input(&label_b,&session_b))?;
        let stop_a = request(host,event(false),input(&label_a,&session_a))?;
        let stop_b = request(host,event(false),input(&label_b,&session_b))?;
        let swapped_session = request(host,event(false),input(&label_a,&session_b))?;
        let swapped_label = request(host,event(false),input(&label_b,&session_a))?;
        let mut foreign = input(&label_a,&session_a);
        foreign["sessionId"] = json!(format!("other-{parent}"));
        let foreign = request(host,event(false),foreign)?;
        let binding = Binding::requested(fork_a.task_id(),&fork_a)?;
        prop_assert_eq!(Binding::decode(&volume::encode(&binding).unwrap())?,binding);
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        runtime.block_on(async {
            let results = TaskVolume::open(Arc::new(MemoryStream::default()),StreamPath::new("results").unwrap())
                .await.map_err(storage)?;
            let condition = CommitCondition::Absent {path:StreamPath::new("owner").unwrap()};
            bind(&results,fork_a.task_id(),&fork_a,condition.clone(),None).await?.unwrap();
            bind(&results,fork_a.task_id(),&fork_b,condition.clone(),None).await?.unwrap();
            require(&results,fork_a.task_id(),&stop_a).await?.unwrap();
            require(&results,fork_a.task_id(),&stop_b).await?.unwrap();
            for swapped in [&swapped_session,&swapped_label,&foreign] {
                assert!(require(&results,fork_a.task_id(),swapped).await?.is_err());
            }
            let before = results.snapshot(&Query::Root,16).await.map_err(storage)?;
            let changed_fork = request(host,event(true),input(&label_a,&session_b))?;
            assert!(matches!(bind(&results,fork_a.task_id(),&changed_fork,condition.clone(),None).await?,Err(RequestRejection::ChildBinding)));
            bind(&results,fork_a.task_id(),&fork_a,condition,None).await?.unwrap();
            let after = results.snapshot(&Query::Root,16).await.map_err(storage)?;
            assert_eq!(after.files,before.files);
            assert_eq!(after.generation,before.generation);
            require(&results,fork_a.task_id(),&stop_a).await?.unwrap();
            require(&results,fork_a.task_id(),&stop_b).await?.unwrap();
            Ok::<_,Error>(())
        })?;
    }
}

#[test]
fn checked_binding_seeds_reach_the_closed_record_decoder() {
    for bytes in [
        include_bytes!("../../../../harness-fuzz/corpus/external-hooks/child-binding.json")
            .as_slice(),
        include_bytes!(
            "../../../../harness-fuzz/corpus/external-hooks/duplicate-child-binding.json"
        )
        .as_slice(),
    ] {
        fuzz(bytes);
    }
    assert!(
        Binding::decode(include_bytes!(
            "../../../../harness-fuzz/corpus/external-hooks/child-binding.json"
        ))
        .is_ok()
    );
    assert!(
        Binding::decode(include_bytes!(
            "../../../../harness-fuzz/corpus/external-hooks/duplicate-child-binding.json"
        ))
        .is_err()
    );
}

#[tokio::test]
async fn binding_boundary_futures_remain_below_sixteen_kibibytes() -> Result<()> {
    let results = TaskVolume::open(
        Arc::new(MemoryStream::default()),
        StreamPath::new("results").unwrap(),
    )
    .await
    .map_err(storage)?;
    let fork = request(
        ExternalHost::Pi,
        "subagent_start",
        json!({"sessionId":"parent","agentId":"child"}),
    )?;
    let condition = CommitCondition::Absent {
        path: StreamPath::new("owner").unwrap(),
    };
    let bind = bind(&results, fork.task_id(), &fork, condition, None);
    let require = require(&results, fork.task_id(), &fork);
    let sizes = [
        std::mem::size_of_val(&bind),
        std::mem::size_of_val(&require),
    ];
    eprintln!("native child binding publication/read futures: {sizes:?}");
    assert!(sizes.iter().all(|size| *size < 16 * 1024));
    Ok(())
}
