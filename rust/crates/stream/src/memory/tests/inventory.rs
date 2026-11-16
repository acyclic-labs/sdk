use super::*;

type TestResult<T> = Result<T, Box<dyn std::error::Error>>;

/// Counts entries whose address differs from their first sighting, then
/// records the addresses of entries seen for the first time.
fn relocations(seen: &mut Vec<usize>, current: &[usize]) -> usize {
    let moved = seen
        .iter()
        .zip(current)
        .filter(|(first, now)| first != now)
        .count();
    seen.extend(current.iter().skip(seen.len()));
    moved
}

#[tokio::test]
async fn retained_entries_never_move_across_power_of_two_boundaries() -> TestResult<()> {
    let provider = MemoryStream::default();
    let path = path("inventory/growth")?;
    let mut seen = Vec::new();
    // 2^10 - 1 entries fill segments 0..=9 exactly, crossing nine boundaries.
    for _ in 0..1023 {
        provider
            .append(AppendRequest {
                path: path.clone(),
                records: vec![Bytes::from_static(b"value").into()],
                if_tail: None,
                idempotency_key: crate::IdempotencyKey::unique(),
            })
            .await?;
        let state = provider.state.read().await;
        let current: Vec<usize> = state
            .retained
            .iter()
            .map(|entry| std::ptr::from_ref(entry).addr())
            .collect();
        assert_eq!(current.len(), state.retained.len());
        for (ordinal, address) in current.iter().enumerate() {
            let entry = state.retained.get(ordinal).ok_or("ordinal lookup")?;
            assert_eq!(std::ptr::from_ref(entry).addr(), *address);
        }
        assert!(state.retained.get(current.len()).is_none());
        assert_eq!(
            relocations(&mut seen, &current),
            0,
            "a retained entry moved"
        );
        // Only the segments the next ordinal needs are funded.
        assert!(state.retained.capacity() < 2 * state.retained.len());
    }
    assert_eq!(provider.state.read().await.retained.capacity(), 1023);

    // Negative control: the replaced growth path. At a full backing it funds a
    // doubled buffer while the old one is live and moves every entry into it,
    // so the same check must observe each prior entry at a new address.
    let mut control: Vec<u64> = Vec::new();
    let mut seen = Vec::new();
    let mut moved = 0;
    for value in 0..1023_u64 {
        if control.len() == control.capacity() {
            let mut grown = Vec::with_capacity((2 * control.capacity()).max(1));
            grown.append(&mut control);
            control = grown;
        }
        control.push(value);
        let current: Vec<usize> = control
            .iter()
            .map(|entry| std::ptr::from_ref(entry).addr())
            .collect();
        moved += relocations(&mut seen, &current);
    }
    assert!(moved > 0, "the relocation check missed a copied backing");
    Ok(())
}
