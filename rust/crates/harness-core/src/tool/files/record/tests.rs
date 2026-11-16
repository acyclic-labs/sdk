use super::*;

fn report(number: usize) -> String {
    serde_json::json!({
        "version": 1, "kind": "slow", "what": format!("Delay {number}"),
        "evidence": format!("step {number}: é🦀\\\""),
    })
    .to_string()
}

#[test]
fn batches_replay_in_order_and_reject_the_whole_write_on_any_invalid_record() -> Result<()> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let io = crate::executor::PreparationIo::from_native_work(&work)?;
    let baseline = work.live_allocation_bytes();
    for count in [1, 2, 17] {
        let content = (0..count).map(report).collect::<Vec<_>>().join("\r\n");
        // The previous one-document decoder rejects precisely the multi-record
        // input that the ordinary write must publish atomically.
        assert_eq!(
            crate::contract::json_from_slice::<feedback::Report>(content.as_bytes()).is_ok(),
            count == 1
        );
        let native = decode_reports(&content, Some(&work), Some(&io))?;
        let portable = decode_reports(&content, None, None)?;
        assert_eq!(native.result.reports, portable.result.reports);
        assert_eq!(native.result.reports.len(), count);
        for (index, report) in native.result.reports.iter().enumerate() {
            assert_eq!(report.what, format!("Delay {index}"));
        }
        let bytes = serde_json::to_vec(&native.result).unwrap();
        let replay: RecordResult = crate::contract::json_from_slice(&bytes).unwrap();
        replay.validate()?;
        assert_eq!(replay.reports, native.result.reports);
        drop(native);
        assert_eq!(work.live_allocation_bytes(), baseline);
    }
    for content in [
        String::new(),
        " \r\n\t".into(),
        format!("{}\nundefined{{", report(0)),
        format!("{}\n{{\"version\":1}}", report(0)),
        format!(
            "{}\n{}",
            report(0),
            report(1).replacen("\"version\":1", "\"version\":2", 1)
        ),
        format!(
            "{}\n{}",
            report(0),
            report(1).replacen("{", "{\"agent\":\"forged\",", 1)
        ),
    ] {
        assert!(
            decode_reports(&content, Some(&work), Some(&io)).is_err(),
            "{content}"
        );
        assert!(decode_reports(&content, None, None).is_err(), "{content}");
        assert_eq!(
            work.live_allocation_bytes(),
            baseline,
            "failed batch retained a partial report"
        );
    }
    cancellation.cancel();
    assert!(decode_reports(&report(0), Some(&work), Some(&io)).is_err());
    Ok(())
}
