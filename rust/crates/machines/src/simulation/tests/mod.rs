use super::*;

fn request(key: IdempotencyKey) -> CreateMachine {
    CreateMachine::new(
        key,
        Some(Image::custom([7; 32]).unwrap_or_else(|_| unreachable!())),
        [8; 32],
    )
}

fn key(suffix: u8) -> IdempotencyKey {
    IdempotencyKey::parse(&format!("00000000-0000-0000-0000-0000000001{suffix:02x}"))
        .unwrap_or_else(|_| unreachable!())
}

fn two() -> NonZeroU32 {
    NonZeroU32::new(2).unwrap_or(NonZeroU32::MIN)
}

mod forks;
mod operations;
mod replay;

#[tokio::test]
async fn large_page_hints_continue_through_all_machines_and_events()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = SimulatedMachines::default();
    let operation =
        |index: u32| IdempotencyKey::parse(&format!("00000000-0000-0000-0000-{index:012x}"));
    let mut expected = BTreeSet::new();
    for index in 1..=257 {
        let MutationOutcome::Created(machine) = provider.create(request(operation(index)?)).await?
        else {
            unreachable!()
        };
        expected.insert(machine.id);
    }
    let mut after = None;
    let mut observed = BTreeSet::new();
    loop {
        let page = provider.list_machines(after, u32::MAX).await?;
        assert!(page.machines.len() <= MAX_PAGE_SIZE as usize);
        for machine in page.machines {
            assert!(after.is_none_or(|cursor| machine.id > cursor));
            assert!(observed.insert(machine.id));
        }
        match page.next {
            Some(next) => {
                assert_eq!(observed.last().copied(), Some(next));
                after = Some(next);
            }
            None => break,
        }
    }
    assert_eq!(observed, expected);
    let machine = *observed.first().ok_or("missing created machine")?;
    for index in 0..513 {
        provider
            .suspend(machine, operation(10_000 + index * 2)?)
            .await?;
        provider
            .wake(machine, operation(10_001 + index * 2)?)
            .await?;
    }
    let mut after = None;
    let mut sequence = 0;
    loop {
        let page = provider.events(machine, after, u32::MAX).await?;
        assert!(page.events.len() <= MAX_EVENT_PAGE_SIZE as usize);
        for event in page.events {
            sequence += 1;
            assert_eq!(event.sequence, sequence);
        }
        match page.next_sequence {
            Some(next) => {
                assert_eq!(next, sequence);
                after = Some(next);
            }
            None => break,
        }
    }
    assert_eq!(sequence, 1_027);
    Ok(())
}
