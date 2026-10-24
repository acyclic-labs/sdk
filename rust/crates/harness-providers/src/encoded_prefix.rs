//! A single retained static prefix per resolved model, compared after file verification.
use acyclic_harness_core::{NativeCustody, NativeWork, Result};
use serde::Serialize;
use serde_json::Value;

const NAMES: [&str; 4] = ["tools", "system", "instructions", "systemInstruction"];

#[derive(Default)]
pub(crate) struct PrefixCache(std::sync::Mutex<Option<Prefix>>);

struct Prefix {
    fields: [Option<Value>; 4],
    encoded: Vec<u8>,
    system_keys: Vec<crate::projection::wire::SystemKey>,
    encoded_system: Vec<Box<serde_json::value::RawValue>>,
    _custody: NativeCustody,
}

#[cfg(test)]
std::thread_local! {
    pub(crate) static PREFIX_ENCODINGS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

struct Fields<'a>(&'a [Option<Value>; 4]);
impl Serialize for Fields<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap as _;
        let mut map = serializer.serialize_map(None)?;
        for (name, value) in NAMES.iter().zip(self.0) {
            if let Some(value) = value {
                map.serialize_entry(name, value)?;
            }
        }
        map.end()
    }
}

fn append(prefix: &[u8], tail: &[u8]) -> Result<Vec<u8>> {
    let comma = usize::from(prefix.len() > 2 && tail.len() > 2);
    let length = prefix
        .len()
        .checked_add(tail.len())
        .and_then(|n| n.checked_sub(2))
        .and_then(|n| n.checked_add(comma))
        .ok_or_else(|| crate::invalid("encoded request length overflow"))?;
    let mut bytes = Vec::with_capacity(length);
    let head = prefix
        .get(..prefix.len().saturating_sub(1))
        .ok_or_else(|| crate::invalid("invalid encoded prefix"))?;
    bytes.extend_from_slice(head);
    if comma != 0 {
        bytes.push(b',');
    }
    bytes.extend_from_slice(
        tail.get(1..)
            .ok_or_else(|| crate::invalid("invalid encoded suffix"))?,
    );
    Ok(bytes)
}

impl Prefix {
    fn matches(
        &self,
        fields: &[Option<Value>; 4],
        body: &crate::projection::wire::Body<'_>,
    ) -> bool {
        // File projection and authority checks have already completed on the
        // current ledger. Only exact verified wire bytes are shared here;
        // retained allocation custody stays on its original ledger.
        &self.fields == fields && body.matches_system_prefix(&self.system_keys)
    }
}

impl PrefixCache {
    pub(crate) fn encode(
        &self,
        mut body: crate::projection::wire::Body<'_>,
        work: NativeWork<'_>,
        custody: &mut NativeCustody,
    ) -> Result<Vec<u8>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = work {
            work.remaining()?;
        }
        #[cfg(target_arch = "wasm32")]
        let _ = work;
        let fields = NAMES.map(|name| body.fields.remove(name));
        {
            let current = self.0.lock().map_err(|_| prefix_lock_error())?;
            if let Some(prefix) = current
                .as_ref()
                .filter(|prefix| prefix.matches(&fields, &body))
            {
                let tail = serde_json::to_vec(&crate::projection::wire::WithChatPrefix(
                    &body,
                    Some(&prefix.encoded_system),
                ))
                .map_err(|_| crate::invalid("model suffix encoding failed"))?;
                return append(&prefix.encoded, &tail);
            }
        }
        #[cfg(test)]
        PREFIX_ENCODINGS.with(|count| count.set(count.get() + 1));
        let encoded = serde_json::to_vec(&Fields(&fields))
            .map_err(|_| crate::invalid("model prefix encoding failed"))?;
        let (system_keys, encoded_system) = body
            .encode_system_prefix()
            .map_err(|_| crate::invalid("model system message encoding failed"))?;
        let tail = serde_json::to_vec(&crate::projection::wire::WithChatPrefix(
            &body,
            Some(&encoded_system),
        ))
        .map_err(|_| crate::invalid("model suffix encoding failed"))?;
        #[cfg(not(target_arch = "wasm32"))]
        let prefix_custody = match custody {
            Some(hold) => {
                let mut retained = (std::mem::size_of::<Prefix>() + encoded.capacity()) as u64;
                retained = retained
                    .checked_add(
                        (system_keys.capacity()
                            * std::mem::size_of::<crate::projection::wire::SystemKey>())
                            as u64,
                    )
                    .and_then(|n| {
                        n.checked_add(
                            (encoded_system.capacity()
                                * std::mem::size_of::<Box<serde_json::value::RawValue>>())
                                as u64,
                        )
                    })
                    .ok_or_else(|| crate::invalid("model prefix custody overflow"))?;
                for key in &system_keys {
                    retained = retained
                        .checked_add(key.owned_bytes()?)
                        .ok_or_else(|| crate::invalid("model prefix custody overflow"))?;
                }
                for value in &encoded_system {
                    retained = retained
                        .checked_add(value.get().len() as u64)
                        .ok_or_else(|| crate::invalid("model prefix custody overflow"))?;
                }
                for value in fields.iter().flatten() {
                    retained = retained
                        .checked_add(
                            acyclic_harness_core::executor::json::json_value_logical_owned_bytes(
                                value,
                            )?,
                        )
                        .ok_or_else(|| crate::invalid("model prefix custody overflow"))?;
                }
                Some(hold.split_off(retained)?)
            }
            None => None,
        };
        #[cfg(target_arch = "wasm32")]
        let prefix_custody = {
            let _ = custody;
            None
        };
        let prefix = Prefix {
            fields,
            encoded,
            system_keys,
            encoded_system,
            _custody: prefix_custody,
        };
        let bytes = append(&prefix.encoded, &tail)?;
        let retired = self
            .0
            .lock()
            .map_err(|_| prefix_lock_error())?
            .replace(prefix);
        // Releasing original custody may run callbacks. Never do it under the cache lock.
        drop(retired);
        Ok(bytes)
    }
}

fn prefix_lock_error() -> acyclic_harness_core::Error {
    #[cfg(not(target_arch = "wasm32"))]
    {
        acyclic_harness_core::Error::NativeBoundary {
            code: acyclic_harness_core::wire::ErrorCode::Storage,
            reason: "model prefix cache poisoned",
        }
    }
    #[cfg(target_arch = "wasm32")]
    {
        acyclic_harness_core::Error::Storage("model prefix cache poisoned".into())
    }
}
