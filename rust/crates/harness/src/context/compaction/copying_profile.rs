use super::*;
use std::{hint::black_box, time::Instant};

#[test]
fn without_subscriber() -> Result<()> {
    profile(false)
}

#[test]
fn with_tracing() -> Result<()> {
    profile(true)
}

fn profile(tracing: bool) -> Result<()> {
    let dispatch = if tracing {
        tracing::Dispatch::new(tracing_subscriber::registry())
    } else {
        tracing::Dispatch::none()
    };
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let selected = std::env::var("HARNESS_CONTEXT_COPY").ok();
    let probe = selected.is_some();
    for (count, bytes) in [(0, 0), (1, 0), (32, 0), (32, 1024), (512, 0)] {
        if probe && (count, bytes) != (32, 0) {
            continue;
        }
        let mut first = empty_messages(count);
        let mut second = empty_messages(count);
        for message in first.iter_mut().chain(second.iter_mut()) {
            message.content = ModelContent::Text("x".repeat(bytes));
        }
        let user = ModelContent::Text("u".repeat(bytes));
        let operation = |legacy: bool| -> Result<()> {
            let first = black_box(&first);
            let second = black_box(&second);
            if legacy {
                black_box(original(first, Some(&user), second));
            } else {
                black_box(clone_messages(first, Some(&user), second, &mut None)?);
            }
            Ok(())
        };
        let batch = if probe || cfg!(debug_assertions) {
            1
        } else {
            256
        };
        let samples = if probe { 1 } else { 16 };
        let mut times = [Vec::with_capacity(samples), Vec::with_capacity(samples)];
        for pair in 0..samples {
            for legacy in if pair % 2 == 0 {
                [true, false]
            } else {
                [false, true]
            } {
                if selected
                    .as_ref()
                    .is_some_and(|name| (name == "before") != legacy)
                {
                    continue;
                }
                if probe {
                    eprintln!("COPY_BEGIN");
                }
                let start = Instant::now();
                for _ in 0..batch {
                    operation(legacy)?;
                }
                times[usize::from(!legacy)].push(start.elapsed().as_nanos() / batch);
                if probe {
                    eprintln!("COPY_END");
                }
            }
        }
        for (index, values) in times.iter_mut().enumerate() {
            if values.is_empty() {
                continue;
            }
            values.sort_unstable();
            let allocation = allocation_counter::measure(|| {
                operation(index == 0).expect("copy");
            });
            eprintln!(
                "copy tracing={tracing} legacy={} first={count} second={count} text_bytes={bytes} median_ns={} allocations={} bytes={} peak={}",
                index == 0,
                values[values.len() / 2],
                allocation.count_total,
                allocation.bytes_total,
                allocation.bytes_max
            );
        }
    }
    Ok(())
}

#[cfg(feature = "filesystem")]
#[test]
fn accounting() -> Result<()> {
    use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkBudget};
    let messages = empty_messages(32);
    let user = ModelContent::Text(String::new());
    let cancellation = CancellationToken::new();
    let mut times = Vec::with_capacity(16);
    let mut allocation = allocation_counter::AllocationInfo::default();
    for sample in 0..16 {
        let work = NativeMaterializationWork::new(
            WorkBudget {
                allocation_operations: 128,
                peak_allocation_bytes: NativeMaterializationWork::accounting_state_bytes()
                    + 1024 * 1024,
                bytes_copied: 1024 * 1024,
                items_examined: 1024,
                ..WorkBudget::default()
            },
            &cancellation,
        )?;
        let mut observer = crate::store::HistoryWork::new(
            crate::filesystem::PreparationIo::from_native_work(&work)?,
        );
        let start = Instant::now();
        let mut result = None;
        let mut copy = || {
            result = Some(clone_messages(
                &messages,
                Some(&user),
                &messages,
                &mut Some(&mut observer),
            ));
        };
        if sample == 0 {
            allocation = allocation_counter::measure(copy);
        } else {
            copy();
        }
        times.push(start.elapsed().as_nanos());
        let (copy, claim) = result.expect("copy was invoked")?;
        assert_eq!(copy.len(), 65);
        let custody = observer.into_custody()?;
        assert_eq!(custody.bytes(), claim);
        drop(copy);
        drop(custody);
    }
    times.sort_unstable();
    eprintln!(
        "copy accounting=true first=32 second=32 median_ns={} allocations={} bytes={} peak={}",
        times[8], allocation.count_total, allocation.bytes_total, allocation.bytes_max
    );
    Ok(())
}
