//! Fresh actor identities share one entropy source; persisted IDs never enter it.

pub(crate) fn random_id() -> String {
    draw(uuid::Uuid::from_bytes, host_random_id)
        .simple()
        .to_string()
}

pub(crate) fn fresh_uuid() -> uuid::Uuid {
    draw(
        |bytes| uuid::Builder::from_random_bytes(bytes).into_uuid(),
        uuid::Uuid::new_v4,
    )
}

fn draw<T>(seeded: impl FnOnce([u8; 16]) -> T, fallback: impl FnOnce() -> T) -> T {
    #[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
    {
        let mut bytes = [0; 16];
        if acyclic_native_runtime::testing::fill_seeded(&mut bytes) {
            return seeded(bytes);
        }
    }
    // The seeded conversion is used only by a native simulation.
    let _ = seeded;
    fallback()
}

fn host_random_id() -> uuid::Uuid {
    // UUID-v4 masks six bits. Select eight unmasked random bytes from each
    // of two independent draws, retaining a full 128 random bits.
    let [a, b, c, d, e, f, _, g, _, h, ..] = uuid::Uuid::new_v4().into_bytes();
    let [i, j, k, l, m, n, _, o, _, p, ..] = uuid::Uuid::new_v4().into_bytes();
    uuid::Uuid::from_bytes([a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p])
}

#[cfg(all(test, feature = "test-support", not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::namespace::Error;
    use acyclic_native_runtime::testing::Schedule;

    #[tokio::test]
    async fn seeded_draws_cannot_reach_the_host_fallback() -> Result<(), Error> {
        let (scope, _) = Schedule::seeded(71);
        let value = scope
            .scope(async {
                // Negative control: bypassing the seeded seam reaches this error.
                draw(
                    |bytes| Ok(uuid::Uuid::from_bytes(bytes).simple().to_string()),
                    || Err(Error::Scope),
                )
            })
            .await?;
        assert_eq!(value.len(), 32);
        let (scope, _) = Schedule::without_entropy(71);
        let value = scope
            .scope(async { draw(|_| Err(Error::Scope), || Ok("host-fallback".to_owned())) })
            .await?;
        assert_eq!(value, "host-fallback");
        Ok(())
    }
}
