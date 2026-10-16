use crate::{StreamError, StreamWatermarks, wire};

/// Decodes an explicit accepted observation, rejecting legacy peers and
/// impossible persistence claims instead of inferring durability from acceptance.
pub fn watermarks_from_wire(response: wire::TailResponse) -> Result<StreamWatermarks, StreamError> {
    let durable = response.durable.ok_or(StreamError::Unsupported)?;
    if durable > response.tail {
        return Err(StreamError::Unavailable);
    }
    Ok(StreamWatermarks {
        accepted: response.tail,
        durable,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use prost::Message;
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]
        #[test]
        fn accepted_observation_round_trips_and_preserves_the_exact_boundary(
            accepted in any::<u64>(), durable in proptest::option::of(any::<u64>()),
        ) {
            let request = wire::TailRequest { path: "agents/new".into(), accepted: accepted % 2 == 0 };
            prop_assert_eq!(wire::TailRequest::decode(request.encode_to_vec().as_slice()).unwrap(), request);
            let wire = wire::TailResponse { tail: accepted, durable };
            let decoded = wire::TailResponse::decode(wire.encode_to_vec().as_slice()).unwrap();
            prop_assert_eq!(&decoded, &wire);
            let expected = match durable {
                None => Err(StreamError::Unsupported),
                Some(durable) if durable > accepted => Err(StreamError::Unavailable),
                Some(durable) => Ok(StreamWatermarks { accepted, durable }),
            };
            prop_assert_eq!(watermarks_from_wire(decoded), expected);
        }
    }
}
