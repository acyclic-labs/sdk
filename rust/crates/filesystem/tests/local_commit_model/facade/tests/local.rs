/// One step of a commit against a volatile device cache, per device.
#[derive(Clone, Copy, PartialEq)]
enum Step {
    /// Write number `.0` to device `.1`.
    Write(usize, usize),
    /// Order the device's earlier writes ahead of its later ones.
    Barrier(usize),
    /// Make the device's earlier writes durable.
    Drain(usize),
    /// Tell the caller the commit is durable.
    Acknowledge,
}

/// Every crash state of `steps`: whether the acknowledgement was given, and
/// which writes survive. A crash keeps every write its device drained, and
/// otherwise any set of that device's writes that respects its barriers; a
/// barrier or drain on one device orders nothing on another.
fn crash_states(steps: &[Step]) -> Vec<(bool, Vec<bool>)> {
    let writes = steps
        .iter()
        .filter(|step| matches!(step, Step::Write(..)))
        .count();
    let mut states = Vec::new();
    for crash in 0..=steps.len() {
        let issued = &steps[..crash];
        let acknowledged = issued.contains(&Step::Acknowledge);
        // Per write: its number, device, ordering epoch and whether drained.
        let mut epoch = [0_usize; 2];
        let mut placed: Vec<(usize, usize, usize, bool)> = Vec::new();
        for step in issued {
            match *step {
                Step::Write(id, device) => placed.push((id, device, epoch[device], false)),
                Step::Barrier(device) => epoch[device] += 1,
                Step::Drain(device) => {
                    epoch[device] += 1;
                    for write in &mut placed {
                        write.3 |= write.1 == device;
                    }
                }
                Step::Acknowledge => {}
            }
        }
        for mask in 0_u32..(1 << placed.len()) {
            let kept = |index: usize| placed[index].3 || mask & (1 << index) != 0;
            let ordered = (0..placed.len()).all(|later| {
                !kept(later)
                    || (0..placed.len()).all(|earlier| {
                        placed[earlier].1 != placed[later].1
                            || placed[earlier].2 >= placed[later].2
                            || kept(earlier)
                    })
            });
            if ordered {
                let mut survived = vec![false; writes];
                for (index, write) in placed.iter().enumerate() {
                    survived[write.0] |= kept(index);
                }
                states.push((acknowledged, survived));
            }
        }
    }
    states
}

/// The local commit protocol: the objects (write 0) on `objects_device`
/// take `objects_flush`, then the frame (write 1) is written and drained on
/// device 0, then the commit is acknowledged. A surviving frame must find
/// its objects, and an acknowledged commit must survive whole.
fn commit_is_crash_consistent(objects_flush: Option<Step>, objects_device: usize) -> bool {
    let mut steps = vec![Step::Write(0, objects_device)];
    steps.extend(objects_flush);
    steps.extend([Step::Write(1, 0), Step::Drain(0), Step::Acknowledge]);
    crash_states(&steps)
        .into_iter()
        .all(|(acknowledged, survived)| {
            (!survived[1] || survived[0]) && (!acknowledged || (survived[0] && survived[1]))
        })
}

#[test]
fn an_ordering_barrier_suffices_for_objects_only_on_the_frame_device() {
    // The composition's choice: a barrier on the frame's device.
    assert!(commit_is_crash_consistent(Some(Step::Barrier(0)), 0));
    // On another device only that device's own drain keeps the contract.
    assert!(commit_is_crash_consistent(Some(Step::Drain(1)), 1));
    // Negative controls: without a flush a frame can outlive its objects, and
    // a barrier on another device neither orders the frame nor is drained by it.
    assert!(!commit_is_crash_consistent(None, 0));
    assert!(!commit_is_crash_consistent(Some(Step::Barrier(1)), 1));
}
