use super::{Result, Value, invalid};
use std::mem::size_of;

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
#[path = "budget_properties.rs"]
mod properties;

pub(super) struct Budget<'work> {
    used: usize,
    limit: usize,
    #[cfg(not(target_arch = "wasm32"))]
    work: Option<&'work acyclic_fs::NativeMaterializationWork>,
    #[cfg(target_arch = "wasm32")]
    _work: std::marker::PhantomData<&'work ()>,
}

impl<'work> Budget<'work> {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn bytes(&self) -> Result<usize> {
        if let Some(work) = self.work {
            work.charge(acyclic_fs::WorkCounters {
                items_examined: 1,
                ..acyclic_fs::WorkCounters::UNCHARGED
            })?;
        }
        Ok(self.used)
    }
    #[cfg(test)]
    #[cfg(not(target_arch = "wasm32"))]
    pub fn used(&self) -> usize {
        self.used
    }
    pub fn new(maximum: usize) -> Self {
        Self {
            used: 0,
            limit: maximum,
            #[cfg(not(target_arch = "wasm32"))]
            work: None,
            #[cfg(target_arch = "wasm32")]
            _work: std::marker::PhantomData,
        }
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub fn native(maximum: usize, work: &'work acyclic_fs::NativeMaterializationWork) -> Self {
        Self {
            work: Some(work),
            ..Self::new(maximum)
        }
    }
    pub fn replace(&mut self, old: usize, new: usize) -> Result<()> {
        let bytes = self.used.checked_sub(old).and_then(|n| n.checked_add(new));
        match bytes {
            Some(bytes) if bytes <= self.limit => {
                self.used = bytes;
                Ok(())
            }
            _ => Err(invalid("retained model state exceeds aggregate byte limit")),
        }
    }
    pub fn add(&mut self, bytes: usize) -> Result<()> {
        self.replace(0, bytes)
    }
    pub fn string(&mut self, target: &mut String, text: &str) -> Result<()> {
        if target == text {
            return Ok(());
        }
        self.replace(target.capacity(), text.len())?;
        *target = text.into();
        Ok(())
    }
    pub fn append(&mut self, target: &mut String, value: Option<&Value>) -> Result<()> {
        let Some(value) = crate::protocol::present(value) else {
            return Ok(());
        };
        let delta = value
            .as_str()
            .ok_or_else(|| invalid("tool delta must be a string"))?;
        self.append_str(target, delta)
    }
    pub fn append_str(&mut self, target: &mut String, delta: &str) -> Result<()> {
        let required = target
            .len()
            .checked_add(delta.len())
            .ok_or_else(|| invalid("tool size overflow"))?;
        if required > target.capacity() {
            let old = target.capacity();
            let available = (self.limit - self.used)
                .checked_add(old)
                .ok_or_else(|| invalid("tool capacity overflow"))?;
            if required > available {
                return Err(invalid("retained model state exceeds aggregate byte limit"));
            }
            let capacity = required.max(old.saturating_mul(2)).max(8).min(available);
            // Only a growth can relocate old text. Native frame admission covers
            // decoding/copying the incoming fragment; it never recharges the prefix.
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = self.work {
                work.charge(acyclic_fs::WorkCounters {
                    bytes_copied: target.len() as u64,
                    ..acyclic_fs::WorkCounters::UNCHARGED
                })?;
            }
            self.add(capacity - old)?;
            if target.try_reserve_exact(capacity - target.len()).is_err() {
                self.used -= capacity - old;
                return Err(invalid("tool buffer allocation failed"));
            }
        }
        target.push_str(delta);
        Ok(())
    }
    pub fn value(&mut self, target: &mut Value, value: &Value) -> Result<()> {
        if target == value {
            return Ok(());
        }
        self.replace(size(target, false)?, size(value, true)?)?;
        *target = value.clone();
        Ok(())
    }
    pub fn member(
        &mut self,
        map: &mut serde_json::Map<String, Value>,
        key: &str,
        value: &Value,
    ) -> Result<()> {
        match map.get_mut(key) {
            Some(target) => self.value(target, value),
            None => {
                // Admit the map slot and payload before either is allocated.
                let payload = size(value, true)?;
                self.add(size_of::<(String, Value)>() + key.len() + payload)?;
                map.insert(key.into(), value.clone());
                Ok(())
            }
        }
    }
    pub fn member_str(
        &mut self,
        map: &mut serde_json::Map<String, Value>,
        key: &str,
        text: &str,
    ) -> Result<()> {
        if let Some(target) = map.get_mut(key) {
            self.replace(size(target, false)?, text.len())?;
            *target = Value::String(text.into());
        } else {
            self.add(size_of::<(String, Value)>() + key.len() + text.len())?;
            map.insert(key.into(), Value::String(text.into()));
        }
        Ok(())
    }
    pub fn encoded<T: serde::Serialize>(&mut self, target: &mut String, value: &T) -> Result<()> {
        let length = crate::projection::encoded_len(value)?;
        self.replace(target.capacity(), length)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| invalid("tool buffer allocation failed"))?;
        serde_json::to_writer(&mut bytes, value).map_err(|_| invalid("tool encoding failed"))?;
        *target = String::from_utf8(bytes).map_err(|_| invalid("tool encoding is not UTF-8"))?;
        Ok(())
    }
}

fn size(value: &Value, cloned: bool) -> Result<usize> {
    let mut result = 0;
    match value {
        // Also bounds Number's owned digits when arbitrary_precision is enabled
        // elsewhere in a workspace build; that representation is private to serde.
        Value::Number(_) => result = crate::projection::encoded_len(value)?,
        Value::String(text) => result = if cloned { text.len() } else { text.capacity() },
        Value::Array(values) => {
            result = if cloned {
                values.len()
            } else {
                values.capacity()
            } * size_of::<Value>();
            for value in values {
                result = result
                    .checked_add(size(value, cloned)?)
                    .ok_or_else(|| invalid("retained size overflow"))?;
            }
        }
        Value::Object(values) => {
            result = values.len() * size_of::<(String, Value)>();
            for (key, value) in values {
                let payload = size(value, cloned)?;
                result = result
                    .checked_add(if cloned { key.len() } else { key.capacity() })
                    .and_then(|bytes| bytes.checked_add(payload))
                    .ok_or_else(|| invalid("retained size overflow"))?;
            }
        }
        _ => {}
    }
    Ok(result)
}
