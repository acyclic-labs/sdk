use crate::{StreamError, StreamPath};

/// Encodes a stream name as an injective canonical product resource.
/// Callers obtain the path from the owning Stream parser before admission.
pub fn stream_resource(path: &str) -> String {
    let mut resource = String::from("streams:/");
    for (index, component) in path.split('/').enumerate() {
        if index != 0 {
            resource.push('/');
        }
        acyclic_auth::append_component(component.as_bytes(), &mut resource);
    }
    resource
}

/// Match a canonical resource against its owning path without allocating.
/// The shared component decoder rejects aliases and encoded separators.
pub fn stream_resource_matches(resource: &str, path: &StreamPath) -> bool {
    let Some(encoded) = resource.strip_prefix("streams:/") else {
        return false;
    };
    let mut components = encoded.split('/');
    path.as_str().split('/').all(|component| {
        components
            .next()
            .is_some_and(|encoded| acyclic_auth::component_matches(encoded, component.as_bytes()))
    }) && components.next().is_none()
}

/// Validates the canonical envelope before handing its name to its product owner.
pub fn validate_resource(resource: &str) -> Result<(), StreamError> {
    envelope(resource)?;
    if let Some(encoded) = resource.strip_prefix("streams:/") {
        if encoded.contains('%') {
            decode_stream_path(encoded)?;
        } else {
            StreamPath::validate(encoded)?;
        }
    }
    Ok(())
}

fn envelope(resource: &str) -> Result<(), StreamError> {
    if resource.len() > 3 * crate::MAX_PATH_BYTES + "streams:/".len() {
        return Err(StreamError::LimitExceeded);
    }
    if !acyclic_auth::valid_resource(resource) {
        return Err(StreamError::InvalidArgument);
    }
    Ok(())
}

/// Decodes a Streams resource with the owning path grammar, rejecting aliases.
pub fn stream_path(resource: &str) -> Result<StreamPath, StreamError> {
    envelope(resource)?;
    let encoded = resource
        .strip_prefix("streams:/")
        .ok_or(StreamError::Unsupported)?;
    decode_stream_path(encoded)
}

fn decode_stream_path(encoded: &str) -> Result<StreamPath, StreamError> {
    let mut path = String::new();
    for (index, component) in encoded.split('/').enumerate() {
        let decoded =
            acyclic_auth::decode_component(component).map_err(|_| StreamError::InvalidArgument)?;
        // A slash inside a component cannot become a namespace separator.
        if decoded.contains(&b'/') {
            return Err(StreamError::InvalidPath);
        }
        if index != 0 {
            path.push('/');
        }
        path.push_str(std::str::from_utf8(&decoded).map_err(|_| StreamError::InvalidPath)?);
    }
    StreamPath::new(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resource_round_trip_preserves_reserved_names_and_literal_percent_bytes()
    -> Result<(), StreamError> {
        for path in [
            "events",
            ".acyclic/fs/volume",
            "caf%C3%A9/release.v1",
            "literal%2F/key",
        ] {
            let resource = stream_resource(path);
            // The original temporary-component composition is the independent
            // ownership control; both must produce the identical resource.
            #[cfg(not(target_arch = "wasm32"))]
            {
                let (_, [temporary, composed]) = acyclic_test_support::differential(
                    || {
                        let mut resource = String::from("streams:/");
                        for (index, component) in path.split('/').enumerate() {
                            if index != 0 {
                                resource.push('/');
                            }
                            resource
                                .push_str(&acyclic_auth::encode_component(component.as_bytes()));
                        }
                        resource
                    },
                    || stream_resource(std::hint::black_box(path)),
                );
                assert!(composed.count_total < temporary.count_total);
                eprintln!(
                    "resource={path} allocations: temporary={} composed={}",
                    temporary.count_total, composed.count_total
                );
            }
            let decoded = stream_path(&resource)?;
            assert_eq!(decoded.as_str(), path);
            assert!(stream_resource_matches(&resource, &decoded));
            assert!(!stream_resource_matches(
                &format!("{resource}/other"),
                &decoded
            ));
            assert!(!stream_resource_matches(
                &resource,
                &StreamPath::new(format!("{path}/other"))?
            ));
        }
        assert_eq!(stream_resource(".acyclic/fs/v"), "streams:/%2Eacyclic/fs/v");
        assert_eq!(
            stream_resource("literal%2F/key"),
            "streams:/literal%252F/key"
        );
        Ok(())
    }

    #[test]
    fn malformed_and_alternate_resource_spellings_never_reach_a_stream() {
        for resource in [
            "events",
            "streams:/a%2Fb",
            "streams:/a%2fb",
            "streams:/%65vents",
            "streams:/a.b",
            "streams:/a//b",
            "streams:/a/",
            "streams:/%2E%2E/b",
            "streams:/caf%C3%A9",
            "streams:/",
            "fs:/volume",
        ] {
            assert!(stream_path(resource).is_err(), "accepted alias {resource}");
            for path in ["events", "a/b", "a.b", "caf%C3%A9", "fs/volume"] {
                assert!(!stream_resource_matches(
                    resource,
                    &StreamPath::new(path).expect("valid original path")
                ));
            }
        }
    }
}
