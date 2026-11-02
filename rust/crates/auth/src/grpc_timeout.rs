//! The original gRPC call budget, shared by transport and product admission.
use std::{future::Future, io, time::Duration};
use tonic::codegen::http::{Extensions, HeaderMap, Request};

/// Decodes the caller's optional timeout. Duplicate or malformed values fail
/// admission; absence imposes no deadline.
pub fn read(headers: &HeaderMap) -> io::Result<Option<Duration>> {
    let mut values = headers.get_all("grpc-timeout").iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(invalid());
    }
    let (&unit, digits) = value.as_bytes().split_last().ok_or_else(invalid)?;
    if digits.is_empty() || digits.len() > 8 || !digits.iter().all(u8::is_ascii_digit) {
        return Err(invalid());
    }
    let count = digits
        .iter()
        .fold(0u64, |n, byte| n * 10 + u64::from(byte - b'0'));
    Ok(Some(match unit {
        b'H' => Duration::from_secs(count * 3600),
        b'M' => Duration::from_secs(count * 60),
        b'S' => Duration::from_secs(count),
        b'm' => Duration::from_millis(count),
        b'u' => Duration::from_micros(count),
        b'n' => Duration::from_nanos(count),
        _ => return Err(invalid()),
    }))
}

/// The original caller budget elapsed. This says nothing about whether an
/// accepted physical operation committed or retired.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("request deadline elapsed")]
pub struct Elapsed;

/// One absolute transport deadline, retained through decoding, admission and
/// forwarding. Ingress should insert this value before decoding, or retain its
/// original arrival as a `std::time::Instant` extension.
#[derive(Clone, Copy, Debug)]
pub struct Deadline {
    end: Option<tokio::time::Instant>,
}

impl Deadline {
    /// Retains an admitted deadline extension, or admits the original header
    /// relative to the transport's arrival extension.
    pub fn of<B>(request: &Request<B>) -> io::Result<Self> {
        Self::from_parts(request.headers(), request.extensions())
    }

    /// The same admission for adapters whose request separates metadata and
    /// extensions. A retained deadline takes precedence over forwarded headers.
    pub fn from_parts(headers: &HeaderMap, extensions: &Extensions) -> io::Result<Self> {
        if let Some(deadline) = extensions.get::<Self>() {
            return Ok(*deadline);
        }
        let start = extensions
            .get::<std::time::Instant>()
            .copied()
            .map(tokio::time::Instant::from_std)
            .unwrap_or_else(tokio::time::Instant::now);
        Ok(Self {
            end: read(headers)?
                .map(|duration| start.checked_add(duration).ok_or_else(invalid))
                .transpose()?,
        })
    }

    /// Original absolute boundary for a downstream admission policy.
    #[must_use]
    pub const fn end(self) -> Option<tokio::time::Instant> {
        self.end
    }

    /// Remaining original budget, without renewing it.
    #[must_use]
    pub fn remaining(self) -> Option<Duration> {
        self.end
            .map(|end| end.saturating_duration_since(tokio::time::Instant::now()))
    }

    /// Refuses work or disclosure after the original budget elapsed.
    pub fn check(self) -> Result<(), Elapsed> {
        if self.remaining().is_some_and(|left| left.is_zero()) {
            Err(Elapsed)
        } else {
            Ok(())
        }
    }

    /// Bounds interest in the original future, including an immediately ready
    /// result at expiry. The future's own result remains intact; callers retain
    /// their original accepted-operation custody and uncertainty rules.
    pub async fn within<F: Future>(self, work: F) -> Result<F::Output, Elapsed> {
        self.check()?;
        let result = match self.end {
            Some(end) => tokio::time::timeout_at(end, work)
                .await
                .map_err(|_| Elapsed)?,
            None => work.await,
        };
        self.check()?;
        Ok(result)
    }

    /// Forwards the remaining budget, rounding down when precision decreases.
    /// A request with no deadline removes a prior relative timeout.
    pub fn forward(self, headers: &mut HeaderMap) -> io::Result<()> {
        if let Some(left) = self.remaining() {
            let (value, unit) = [
                (left.as_nanos(), 'n'),
                (left.as_micros(), 'u'),
                (left.as_millis(), 'm'),
                (u128::from(left.as_secs()), 'S'),
                (u128::from(left.as_secs() / 60), 'M'),
                (u128::from(left.as_secs() / 3600), 'H'),
            ]
            .into_iter()
            .find(|(value, _)| *value <= 99_999_999)
            .ok_or_else(invalid)?;
            headers.insert(
                "grpc-timeout",
                format!("{value}{unit}").parse().map_err(io::Error::other)?,
            );
        } else {
            headers.remove("grpc-timeout");
        }
        Ok(())
    }
}

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "invalid grpc-timeout")
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt as _;

    #[test]
    fn deadline_admission_rejects_ambiguous_headers_and_preserves_wire_units() {
        let mut headers = HeaderMap::new();
        assert_eq!(read(&headers).unwrap(), None);
        for (value, expected) in [
            ("99999999H", Duration::from_secs(99_999_999 * 3600)),
            ("1M", Duration::from_secs(60)),
            ("2S", Duration::from_secs(2)),
            ("3m", Duration::from_millis(3)),
            ("4u", Duration::from_micros(4)),
            ("5n", Duration::from_nanos(5)),
            ("0n", Duration::ZERO),
        ] {
            headers.insert("grpc-timeout", value.parse().unwrap());
            assert_eq!(read(&headers).unwrap(), Some(expected));
        }
        for value in ["", "S", "+1S", "-1S", "1s", "100000000n", "1S,2S"] {
            headers.insert("grpc-timeout", value.parse().unwrap());
            assert_eq!(
                read(&headers).unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }
        headers.insert("grpc-timeout", "1S".parse().unwrap());
        headers.append("grpc-timeout", "2S".parse().unwrap());
        assert!(read(&headers).is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_survives_hops_and_has_no_implicit_limit() {
        for timeout in [Some("1H"), None] {
            let mut request = Request::new(());
            if let Some(timeout) = timeout {
                request
                    .headers_mut()
                    .insert("grpc-timeout", timeout.parse().unwrap());
            }
            let deadline = Deadline::of(&request).unwrap();
            let (produce, produced) = tokio::sync::oneshot::channel();
            let mut work = Box::pin(deadline.within(produced));
            assert!(work.as_mut().now_or_never().is_none());
            tokio::time::advance(Duration::from_secs(306)).await;
            assert!(work.as_mut().now_or_never().is_none());
            deadline.forward(request.headers_mut()).unwrap();
            assert_eq!(
                read(request.headers()).unwrap(),
                timeout.map(|_| Duration::from_secs(3294))
            );
            request.extensions_mut().insert(deadline);
            assert_eq!(Deadline::of(&request).unwrap().end(), deadline.end());
            produce.send(7).unwrap();
            assert_eq!(work.await.unwrap().unwrap(), 7);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_stops_pending_interest_and_refuses_ready_results_at_expiry() {
        let request = Request::builder()
            .header("grpc-timeout", "1S")
            .body(())
            .unwrap();
        let deadline = Deadline::of(&request).unwrap();
        let (produce, produced) = tokio::sync::oneshot::channel::<()>();
        let mut work = Box::pin(deadline.within(produced));
        assert!(work.as_mut().now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(1)).await;
        produce.send(()).unwrap();
        assert!(matches!(work.await, Err(Elapsed)));
        let result: Result<(), Elapsed> = deadline
            .within(async { panic!("expired work must not be polled") })
            .await;
        assert_eq!(result, Err(Elapsed));

        let mut request = Request::builder()
            .header("grpc-timeout", "1S")
            .body(())
            .unwrap();
        request
            .extensions_mut()
            .insert(std::time::Instant::now() - Duration::from_secs(2));
        assert_eq!(Deadline::of(&request).unwrap().check(), Err(Elapsed));
    }
}
