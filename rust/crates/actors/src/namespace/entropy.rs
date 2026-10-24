//! One entropy seam for local IDs and stub sessions; persisted IDs never enter it.
use super::Error;

pub(super) fn random_id() -> Result<String, Error> {
    draw(host_random_id)
}

fn draw(fallback: impl FnOnce() -> Result<String, Error>) -> Result<String, Error> {
    #[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
    {
        let mut bytes = [0; 16];
        if acyclic_native_runtime::testing::fill_seeded(&mut bytes) {
            return Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect());
        }
    }
    fallback()
}

fn host_random_id() -> Result<String, Error> {
    // UUID-v4 masks six bits. Select eight unmasked random bytes from each
    // of two independent draws, retaining a full 128 random bits.
    let left = uuid::Uuid::new_v4();
    let right = uuid::Uuid::new_v4();
    let mut bytes = Vec::with_capacity(16);
    for value in [left.as_bytes(), right.as_bytes()] {
        for index in [0, 1, 2, 3, 4, 5, 7, 9] {
            bytes.push(*value.get(index).ok_or(Error::Scope)?);
        }
    }
    let text: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    Ok(text)
}

#[cfg(all(test, feature = "test-support", not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::namespace::{Namespace, tests::Mock};
    use acyclic_native_runtime::testing::{Schedule, fill_seeded};
    use std::sync::Arc;

    #[tokio::test]
    async fn ids_and_stub_sessions_draw_exact_seeded_bytes_in_order() -> Result<(), Error> {
        async fn actual(seed: u64) -> Result<(String, String), Error> {
            let (scope, _) = Schedule::seeded(seed);
            scope
                .scope(async {
                    let namespace =
                        Namespace::new(b"account", b"ClassOne", Arc::new(Mock::default()))?;
                    let id = namespace.new_unique_id()?;
                    let stub = namespace.get(&id, None)?;
                    Ok((id.as_str().to_owned(), stub.session))
                })
                .await
        }
        for seed in 0..16 {
            let first = actual(seed).await?;
            assert_eq!(first, actual(seed).await?);
            let (scope, _) = Schedule::seeded(seed);
            let expected = scope
                .scope(async {
                    let mut bytes = [0; 32];
                    assert!(fill_seeded(&mut bytes));
                    let (id, session) = bytes.split_at(16);
                    let hex = |bytes: &[u8]| {
                        bytes
                            .iter()
                            .map(|byte| format!("{byte:02x}"))
                            .collect::<String>()
                    };
                    (hex(id), hex(session))
                })
                .await;
            assert_eq!(first, expected);
        }
        Ok(())
    }

    #[tokio::test]
    async fn seeded_draws_cannot_reach_the_host_fallback() -> Result<(), Error> {
        let (scope, _) = Schedule::seeded(71);
        let value = scope
            .scope(async {
                // Negative control: bypassing the seeded seam reaches this error.
                draw(|| Err(Error::Scope))
            })
            .await?;
        assert_eq!(value.len(), 32);
        let (scope, _) = Schedule::without_entropy(71);
        let value = scope
            .scope(async { draw(|| Ok("host-fallback".into())) })
            .await?;
        assert_eq!(value, "host-fallback");
        Ok(())
    }
}
