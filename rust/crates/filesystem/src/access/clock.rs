/// An unavailable or pre-epoch clock cannot authorize expiring access.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClockUnavailable;

impl std::fmt::Display for ClockUnavailable {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        output.write_str("access clock is unavailable")
    }
}

impl std::error::Error for ClockUnavailable {}

/// Convert a host clock observation into authorization time. Zero is the
/// `UnixMillisClock` unavailable sentinel; it must never make expired grants live.
pub fn unix_seconds(millis: u64) -> Result<u64, ClockUnavailable> {
    if millis == 0 {
        return Err(ClockUnavailable);
    }
    Ok(millis / 1000)
}

#[cfg(test)]
mod tests {

    #[test]
    fn unavailable_clock_cannot_authorize_an_expired_grant()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::access::*;
        let policy: Policy = serde_json::from_slice(INITIAL_POLICY)?;
        let principal = Principal {
            kind: PrincipalKind::Agent,
            id: "clock-fixture".into(),
            lineage: vec![],
            groups: Default::default(),
            roles: Default::default(),
            tags: Default::default(),
            attributes: Default::default(),
        };
        let mut capability = Capability::whole_volume(None, &Action::ALL);
        capability.expires_at = Some(1);
        let resource = Resource {
            config: crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral),
            lineage: &[],
            volume: "fixture",
            generation: 1,
            head: 1,
            path: "file",
        };
        let authorize = |now| {
            decide(
                &policy,
                &principal,
                Action::Read,
                resource,
                Context { now, approvals: 0 },
                &capability,
            )
            .allowed
        };
        assert!(!unix_seconds(0).is_ok_and(authorize));
        assert!(!unix_seconds(2000).is_ok_and(authorize));
        // Negative control: the previous zero fallback resurrected the expired grant.
        let unchecked_clock = |millis: u64| millis / 1000;
        assert!(authorize(unchecked_clock(0)));
        Ok(())
    }
}
