//! Lifetime of native reducer component allocations.
use super::*;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(in crate::core) type Component = (NativeComponentKey<'static>, acyclic_fs::NativeAllocation, acyclic_fs::NativeAllocation);
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
pub(in crate::core) type Component = ();
#[derive(Default)]
pub(in crate::core) struct NativeComponentCustody {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(in crate::core) components: Vec<Component>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(in crate::core) buffer: Option<acyclic_fs::NativeAllocation>,
}
impl std::fmt::Debug for NativeComponentCustody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.debug_struct("NativeComponentCustody").finish_non_exhaustive() }
}
impl PartialEq for NativeComponentCustody { fn eq(&self, _: &Self) -> bool { true } }
impl NativeComponentCustody {
    pub(in crate::core) fn produce<T>(&mut self, key: &NativeComponentKey<'_>, work: &mut Option<&mut dyn HistoryIo>,
        copy: impl FnOnce(&mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<T>) -> Result<(T, Option<Component>)> {
        let prepared = Self::prepare(key, 0, work)?;
        let retired = self.install(prepared, work)?;
        let native = work.as_deref().is_some_and(is_native);
        let mut ordinary = None;
        let mut quota = crate::wire_codec::CodecQuota::new(if native { work } else { &mut ordinary }, 0)?;
        let result = copy(&mut quota).and_then(|value| {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if native {
                let bytes = quota.retained();
                quota.transfer();
                self.absorb_produced(key, bytes, quota.work().as_deref_mut().expect("native copy observer"))?;
            }
            Ok(value)
        });
        match result {
            Ok(value) => Ok((value, retired)),
            Err(error) => { drop(self.rollback(key, retired)); Err(error) }
        }
    }
    pub(in crate::core) fn install(&mut self, component: Option<Component>, work: &mut Option<&mut dyn HistoryIo>) -> Result<Option<Component>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(component) = component {
            if let Some(index) = self.components.iter().position(|(key, _, _)| key == &component.0) {
                return Ok(Some(std::mem::replace(&mut self.components[index], component)));
            }
            if self.components.len() == self.components.capacity() {
                let count = self.components.capacity().checked_mul(2).unwrap_or(usize::MAX).max(2);
                if let Err(error) = self.reserve(count, work) {
                    let (key, payload, metadata) = component;
                    drop((key, metadata));
                    return Err(match error { Error::NativeWork { cause, custody: None } => Error::NativeWork { cause, custody: Some(payload) }, error => error });
                }
            }
            self.components.push(component);
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = (component, work);
        Ok(None)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(in crate::core) fn reserve(&mut self, count: usize, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
        if count <= self.components.capacity() { return Ok(()); }
        let copied = Self::metadata_bytes(self.components.len())?;
        work.as_deref().expect("native table observer").admit_copy(copied)?;
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut replacement = reserve_copy_vector(count, &mut quota)?;
        let bytes = quota.retained();
        let allocation = quota.work().as_deref_mut().expect("native table observer").take_native_custody(bytes)?;
        quota.transfer();
        quota.work().as_deref_mut().expect("native table observer").copy_bytes(copied)?;
        replacement.append(&mut self.components);
        let old = self.buffer.replace(allocation);
        drop(std::mem::replace(&mut self.components, replacement));
        drop(old);
        Ok(())
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(in crate::core) fn absorb_produced(&mut self, key: &NativeComponentKey<'_>, bytes: u64, io: &mut dyn HistoryIo) -> Result<()> {
        let mut incoming = io.take_native_custody(bytes)?;
        let index = self.components.iter().position(|(current, _, _)| current == key).ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage, reason: "native copy destination is absent",
        })?;
        self.components[index].1.absorb(&mut incoming).map_err(|cause| Error::NativeWork { cause, custody: Some(incoming) })
    }
    pub(in crate::core) fn metadata_bytes(count: usize) -> Result<u64> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let size = std::mem::size_of::<Component>();
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let size = std::mem::size_of::<NativeComponentKey>();
        owned_buffer::<u8>(count.checked_mul(size).ok_or_else(|| Error::Invalid("native metadata overflows".into()))?)
    }
    pub(in crate::core) fn prepare(key: &NativeComponentKey<'_>, bytes: u64, work: &mut Option<&mut dyn HistoryIo>) -> Result<Option<Component>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.as_deref().is_some_and(is_native) {
            let mut quota = crate::wire_codec::CodecQuota::new(work, bytes)?;
            let key = key.copy(&mut quota)?;
            let keys = quota.retained() - bytes;
            let io = quota.work().as_deref_mut().expect("native component observer");
            let metadata = io.take_native_custody(keys)?;
            let payload = io.take_native_custody(bytes)?;
            quota.transfer();
            return Ok(Some((key, payload, metadata)));
        }
        let _ = (key, bytes, work);
        Ok(None)
    }
    pub(in crate::core) fn rollback(&mut self, key: &NativeComponentKey<'_>, retired: Option<Component>) -> Option<Component> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(index) = self.components.iter().position(|(current, _, _)| current == key) {
            return Some(match retired { Some(retired) => std::mem::replace(&mut self.components[index], retired), None => self.components.swap_remove(index) });
        }
        let _ = (key, retired);
        None
    }
    pub(in crate::core) fn remove(&mut self, key: &NativeComponentKey<'_>) { drop(self.rollback(key, None)); }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(in crate::core) fn transfer_effect(&mut self, id: EffectId, io: &mut dyn HistoryIo) -> Result<u64> {
        let mut bytes = 0;
        for key in [NativeComponentKey::EffectAction(id), NativeComponentKey::EffectStatus(id), NativeComponentKey::EffectAttempts(id)] {
            if let Some((key, allocation, metadata)) = self.rollback(&key, None) {
                drop((key, metadata));
                bytes = owned_sum([bytes, allocation.bytes()])?;
                io.absorb_native_custody(allocation)?;
            }
        }
        Ok(bytes)
    }
    pub(in crate::core) fn require_work(&self, work: &Option<&mut dyn HistoryIo>, reason: &'static str) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if !self.components.is_empty() && !work.as_deref().is_some_and(is_native) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason });
        }
        let _ = (self, work, reason);
        Ok(())
    }
}
