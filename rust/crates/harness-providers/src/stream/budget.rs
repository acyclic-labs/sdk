use super::{Result, Value, invalid};
use std::mem::size_of;

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "budget_properties.rs"]
mod properties;

#[derive(Clone, Copy, Default)]
pub(super) struct Size {
    pub bytes: usize,
    pub items: usize,
}

pub(super) struct Budget<'work> {
    used: Size,
    limit: Size,
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
        Ok(self.used.bytes)
    }
    #[cfg(all(test, not(target_arch = "wasm32")))]
    pub fn used(&self) -> (usize, usize) {
        (self.used.bytes, self.used.items)
    }
    pub fn new(maximum: usize) -> Self {
        Self {
            used: Size::default(),
            limit: Size {
                bytes: maximum,
                items: maximum.min(1024),
            },
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
    pub fn replace(&mut self, old: Size, new: Size) -> Result<()> {
        let bytes = self
            .used
            .bytes
            .checked_sub(old.bytes)
            .and_then(|n| n.checked_add(new.bytes));
        let items = self
            .used
            .items
            .checked_sub(old.items)
            .and_then(|n| n.checked_add(new.items));
        match (bytes, items) {
            (Some(bytes), Some(items))
                if bytes <= self.limit.bytes && items <= self.limit.items =>
            {
                self.used = Size { bytes, items };
                Ok(())
            }
            _ => Err(invalid(
                "retained model state exceeds aggregate byte or item limit",
            )),
        }
    }
    pub fn add(&mut self, bytes: usize, items: usize) -> Result<()> {
        self.replace(Size::default(), Size { bytes, items })
    }
    pub fn string(&mut self, target: &mut String, text: &str) -> Result<()> {
        if target == text {
            return Ok(());
        }
        self.replace(
            Size {
                bytes: target.capacity(),
                items: 0,
            },
            Size {
                bytes: text.len(),
                items: 0,
            },
        )?;
        *target = text.into();
        Ok(())
    }
    pub fn append(&mut self, target: &mut String, value: Option<&Value>) -> Result<()> {
        let Some(value) = value else { return Ok(()) };
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
            let available = (self.limit.bytes - self.used.bytes)
                .checked_add(old)
                .ok_or_else(|| invalid("tool capacity overflow"))?;
            if required > available {
                return Err(invalid(
                    "retained model state exceeds aggregate byte or item limit",
                ));
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
            self.add(capacity - old, 0)?;
            if target.try_reserve_exact(capacity - target.len()).is_err() {
                self.used.bytes -= capacity - old;
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
                self.add(
                    size_of::<(String, Value)>() + key.len() + payload.bytes,
                    1 + payload.items,
                )?;
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
            self.replace(
                size(target, false)?,
                Size {
                    bytes: text.len(),
                    items: 0,
                },
            )?;
            *target = Value::String(text.into());
        } else {
            self.add(size_of::<(String, Value)>() + key.len() + text.len(), 1)?;
            map.insert(key.into(), Value::String(text.into()));
        }
        Ok(())
    }
    pub fn encoded<T: serde::Serialize>(&mut self, target: &mut String, value: &T) -> Result<()> {
        let length = crate::projection::encoded_len(value)?;
        self.replace(
            Size {
                bytes: target.capacity(),
                items: 0,
            },
            Size {
                bytes: length,
                items: 0,
            },
        )?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| invalid("tool buffer allocation failed"))?;
        serde_json::to_writer(&mut bytes, value).map_err(|_| invalid("tool encoding failed"))?;
        *target = String::from_utf8(bytes).map_err(|_| invalid("tool encoding is not UTF-8"))?;
        Ok(())
    }
}

fn size(value: &Value, cloned: bool) -> Result<Size> {
    let mut result = Size::default();
    match value {
        // Also bounds Number's owned digits when arbitrary_precision is enabled
        // elsewhere in a workspace build; that representation is private to serde.
        Value::Number(_) => result.bytes = crate::projection::encoded_len(value)?,
        Value::String(text) => result.bytes = if cloned { text.len() } else { text.capacity() },
        Value::Array(values) => {
            result.bytes = if cloned {
                values.len()
            } else {
                values.capacity()
            } * size_of::<Value>();
            result.items = values.len();
            for value in values {
                merge(&mut result, size(value, cloned)?)?;
            }
        }
        Value::Object(values) => {
            result.bytes = values.len() * size_of::<(String, Value)>();
            result.items = values.len();
            for (key, value) in values {
                merge(
                    &mut result,
                    Size {
                        bytes: if cloned { key.len() } else { key.capacity() },
                        items: 0,
                    },
                )?;
                merge(&mut result, size(value, cloned)?)?;
            }
        }
        _ => {}
    }
    Ok(result)
}

fn merge(target: &mut Size, other: Size) -> Result<()> {
    target.bytes = target
        .bytes
        .checked_add(other.bytes)
        .ok_or_else(|| invalid("retained size overflow"))?;
    target.items = target
        .items
        .checked_add(other.items)
        .ok_or_else(|| invalid("retained size overflow"))?;
    Ok(())
}
