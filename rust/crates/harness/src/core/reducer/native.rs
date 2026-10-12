//! Native allocations retained by the reducer context.
use super::*;
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(in crate::core) type Component = (
    NativeComponentKey<'static>,
    acyclic_fs::NativeAllocation,
    acyclic_fs::NativeAllocation,
);
#[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
pub(in crate::core) type Component = ();

impl Reducer {
    pub(in crate::core) fn produce<T>(
        &mut self,
        key: &NativeComponentKey<'_>,
        work: &mut Option<&mut dyn HistoryIo>,
        copy: impl FnOnce(&Self, &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<T>,
    ) -> Result<(T, Option<Component>)> {
        let component = Self::claim_component(key, work)?;
        let retired = self.install_component(component, work)?;
        let native = work.as_deref().is_some_and(is_native);
        let mut unmeasured = None;
        let mut quota =
            crate::wire_codec::CodecQuota::new(if native { work } else { &mut unmeasured }, 0)?;
        let copied = copy(self, &mut quota).and_then(|value| {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if native {
                let bytes = quota.retained();
                quota.transfer();
                self.absorb_produced(
                    key,
                    bytes,
                    quota.work().as_deref_mut().expect("native copy"),
                )?;
            }
            Ok(value)
        });
        match copied {
            Ok(value) => Ok((value, retired)),
            Err(error) => {
                drop(self.rollback_component(key, retired));
                Err(error)
            }
        }
    }

    pub(in crate::core) fn install_component(
        &mut self,
        component: Option<Component>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Option<Component>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(component) = component {
            if let Some(index) = self
                .native_components
                .iter()
                .position(|(key, _, _)| key == &component.0)
            {
                return Ok(Some(std::mem::replace(
                    &mut self.native_components[index],
                    component,
                )));
            }
            if self.native_components.len() == self.native_components.capacity() {
                let count = self.native_components.capacity().saturating_mul(2).max(2);
                if let Err(error) = self.reserve_native_components(count, work) {
                    let (key, payload, metadata) = component;
                    drop((key, metadata));
                    return Err(match error {
                        Error::NativeWork {
                            cause,
                            custody: None,
                        } => Error::NativeWork {
                            cause,
                            custody: Some(payload),
                        },
                        error => error,
                    });
                }
            }
            self.native_components.push(component);
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = (component, work);
        Ok(None)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(in crate::core) fn reserve_native_components(
        &mut self,
        count: usize,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        if count <= self.native_components.capacity() {
            return Ok(());
        }
        let copied = owned_buffer::<Component>(self.native_components.len())?;
        work.as_deref().expect("native table").admit_copy(copied)?;
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut replacement = reserve_copy_vector(count, &mut quota)?;
        let bytes = quota.retained();
        let allocation = quota
            .work()
            .as_deref_mut()
            .expect("native table")
            .take_native_custody(bytes)?;
        quota.transfer();
        quota
            .work()
            .as_deref_mut()
            .expect("native table")
            .copy_bytes(copied)?;
        replacement.append(&mut self.native_components);
        self.native_components = replacement;
        self.native_component_buffer = Some(allocation);
        Ok(())
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(in crate::core) fn absorb_produced(
        &mut self,
        key: &NativeComponentKey<'_>,
        bytes: u64,
        io: &mut dyn HistoryIo,
    ) -> Result<()> {
        let mut incoming = io.take_native_custody(bytes)?;
        let index = self
            .native_components
            .iter()
            .position(|(current, _, _)| current == key)
            .ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Storage,
                reason: "native copy destination is absent",
            })?;
        self.native_components[index]
            .1
            .absorb(&mut incoming)
            .map_err(|cause| Error::NativeWork {
                cause,
                custody: Some(incoming),
            })
    }

    pub(in crate::core) fn claim_component(
        key: &NativeComponentKey<'_>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Option<Component>> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if work.as_deref().is_some_and(is_native) {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let key = key.copy(&mut quota)?;
            let bytes = quota.retained();
            let io = quota.work().as_deref_mut().expect("native component");
            let metadata = io.take_native_custody(bytes)?;
            let payload = io.take_native_custody(0)?;
            quota.transfer();
            return Ok(Some((key, payload, metadata)));
        }
        let _ = (key, work);
        Ok(None)
    }

    pub(in crate::core) fn rollback_component(
        &mut self,
        key: &NativeComponentKey<'_>,
        retired: Option<Component>,
    ) -> Option<Component> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(index) = self
            .native_components
            .iter()
            .position(|(current, _, _)| current == key)
        {
            return Some(match retired {
                Some(retired) => std::mem::replace(&mut self.native_components[index], retired),
                None => self.native_components.swap_remove(index),
            });
        }
        let _ = (key, retired);
        None
    }

    pub(in crate::core) fn remove_component(&mut self, key: &NativeComponentKey<'_>) {
        drop(self.rollback_component(key, None));
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(in crate::core) fn transfer_effect(
        &mut self,
        id: EffectId,
        io: &mut dyn HistoryIo,
    ) -> Result<u64> {
        let mut bytes = 0;
        for key in [
            NativeComponentKey::EffectAction(id),
            NativeComponentKey::EffectStatus(id),
            NativeComponentKey::EffectAttempts(id),
        ] {
            if let Some((key, allocation, metadata)) = self.rollback_component(&key, None) {
                drop((key, metadata));
                bytes = owned_sum([bytes, allocation.bytes()])?;
                io.absorb_native_custody(allocation)?;
            }
        }
        Ok(bytes)
    }

    pub(in crate::core) fn require_native_work(
        &self,
        work: &Option<&mut dyn HistoryIo>,
        reason: &'static str,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if !self.native_components.is_empty() && !work.as_deref().is_some_and(is_native) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason,
            });
        }
        let _ = (self, work, reason);
        Ok(())
    }
}
