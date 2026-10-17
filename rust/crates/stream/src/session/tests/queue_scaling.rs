//! Paired reproduction: one credited head behind thousands of stalled follows.
use super::*;
use std::time::Instant;

fn record(sequence: u64) -> Event {
    Event::Record(crate::Record {
        sequence,
        value: wire::SessionLatestValue {
            key: Bytes::from_static(b"status"),
            value: Bytes::from_static(b"running"),
            attention: false,
        }
        .encode_to_vec()
        .into(),
        commit_id: crate::CommitId::from_bytes([1; 32]),
        committed_at_micros: 0,
    })
}

// The reviewed selector, retained only as the paired baseline. Local credit
// is zero for every channel except target, exactly as in the production case.
fn scanned_ready(
    queue: &mut VecDeque<wire::SessionFrame>,
    target: u64,
    credit: wire::SessionWindow,
) -> (wire::SessionFrame, usize) {
    let runnable = BTreeSet::from([target]);
    let mut visits = 0;
    for index in 0..queue.len() {
        visits += 1;
        let pending = &queue[index];
        if !runnable.contains(&pending.channel_id) {
            continue;
        }
        if queue
            .iter()
            .take(index)
            .any(|old| old.channel_id == pending.channel_id)
        {
            continue;
        }
        let mut global = credit;
        let mut local = credit;
        if wire_codec::session::consume_credit(&mut global, pending).is_err()
            || wire_codec::session::consume_credit(&mut local, pending).is_err()
        {
            continue;
        }
        return (queue.remove(index).unwrap(), visits);
    }
    panic!("credited baseline head was not selected");
}

#[test]
fn credited_tail_selection_visits_one_head_independent_of_stalled_follows() {
    for count in [1024, 4096, 8192] {
        let mut session = properties::empty_session(true);
        // The caller's explicit budget admits the fixture's many small frames.
        session.limits = Limits {
            channels: count + 1,
            pending_frames: count + 1,
            pending_bytes: crate::MAX_COMMAND_BYTES,
        };
        session.channels.clear();
        for index in 0..=count {
            let id = (index * 2 + 1) as u64;
            let channel = properties::empty_session(true).channels.remove(&1).unwrap();
            session.channels.insert(id, channel);
            session
                .record(
                    wire::SessionChannel {
                        channel_id: id,
                        generation: 1,
                    },
                    Ok(record(0)),
                )
                .unwrap();
        }
        let target = (count * 2 + 1) as u64;
        let mut before: VecDeque<_> = session
            .channels
            .values()
            .flat_map(|c| c.pending.iter().map(|p| p.frame.clone()))
            .collect();
        let mut before_time = std::time::Duration::ZERO;
        let mut after_time = std::time::Duration::ZERO;
        for sequence in 0..64 {
            session.credit = wire::SessionWindow {
                frames: 1,
                bytes: crate::MAX_COMMAND_BYTES as u64,
            };
            session.channels.get_mut(&target).unwrap().credit = session.credit;
            *before.back_mut().unwrap() = session
                .channels
                .get(&target)
                .unwrap()
                .pending
                .front()
                .unwrap()
                .frame
                .clone();
            let started = Instant::now();
            let (old, visits) = scanned_ready(&mut before, target, session.credit);
            before_time += started.elapsed();
            assert_eq!(visits, count + 1);
            let head_visits = session.head_visits;
            let started = Instant::now();
            session.refresh_source(target);
            let selected = session.ready().unwrap().unwrap();
            after_time += started.elapsed();
            assert_eq!(selected, old);
            before.push_back(old);
            assert_eq!(session.head_visits - head_visits, 1);
            assert_eq!(session.pending_frames, count);
            if sequence != 63 {
                session
                    .record(
                        wire::SessionChannel {
                            channel_id: target,
                            generation: 1,
                        },
                        Ok(record(sequence + 1)),
                    )
                    .unwrap();
            }
        }
        eprintln!(
            "stalled={count} trials=64 before={before_time:?} after={after_time:?} head_visits={}->1",
            count + 1
        );
    }
}

#[test]
fn global_byte_credit_selects_fitting_heads_without_overtaking_a_channel() {
    let mut session = properties::empty_session(false);
    let mut other = properties::empty_session(false)
        .channels
        .remove(&1)
        .unwrap();
    other.credit = wire::SessionWindow {
        bytes: 1_000_000,
        frames: 100,
    };
    session.channels.insert(3, other);
    for (id, size) in [(1, 1000), (1, 1), (3, 1)] {
        let sequence = session.channels.get(&id).unwrap().pending.len() as u64;
        let Event::Record(mut entry) = record(sequence) else {
            unreachable!()
        };
        entry.value = vec![0; size].into();
        session
            .record(
                wire::SessionChannel {
                    channel_id: id,
                    generation: 1,
                },
                Ok(Event::Record(entry)),
            )
            .unwrap();
        session.channels.get_mut(&id).unwrap().credit = wire::SessionWindow {
            bytes: 1_000_000,
            frames: 100,
        };
        session.refresh_source(id);
    }
    session.credit = wire::SessionWindow {
        bytes: 100,
        frames: 100,
    };
    assert_eq!(session.ready().unwrap().unwrap().channel_id, 3);
    assert!(session.ready().unwrap().is_none());
    session.credit.bytes = 10_000;
    assert_eq!(session.ready().unwrap().unwrap().channel_id, 1);
    assert_eq!(session.ready().unwrap().unwrap().channel_id, 1);
}
