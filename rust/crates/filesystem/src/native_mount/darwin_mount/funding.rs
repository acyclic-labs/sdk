use super::*;

pub(super) struct DarwinOwned<T> {
    pub(super) value: T,
    // Physical backing drops before its original allocation owner.
    pub(super) allocation: Option<crate::NativeAllocation>,
}

impl<T> std::ops::Deref for DarwinOwned<T> {
    type Target = T;
    fn deref(&self) -> &T { &self.value }
}

impl<T> std::ops::DerefMut for DarwinOwned<T> {
    fn deref_mut(&mut self) -> &mut T { &mut self.value }
}

impl<T> DarwinOwned<T> {
    pub(super) fn empty(value: T) -> Self { Self { value, allocation: None } }

    pub(super) fn claim_more(&mut self, bytes: u64, work: &crate::NativeMaterializationWork)
        -> Result<(), NativeMountError>
    {
        if bytes == 0 { return Ok(()); }
        let mut next = work.claim(bytes, 1, crate::WorkCounters::UNCHARGED)?;
        if let Some(allocation) = &mut self.allocation { allocation.absorb(&mut next)?; }
        else { self.allocation = Some(next); }
        Ok(())
    }

    pub(super) fn retire_part(&mut self, bytes: u64) {
        if bytes != 0 {
            self.allocation.as_mut().expect("funded backing").release(bytes)
                .expect("retire only physically dropped backing");
        }
    }

    pub(super) fn map<U>(self, transform: impl FnOnce(T) -> U) -> DarwinOwned<U> {
        let Self { value, allocation } = self;
        DarwinOwned { value: transform(value), allocation }
    }

    pub(super) fn into_parts(self) -> (T, Option<crate::NativeAllocation>) {
        (self.value, self.allocation)
    }
}

pub(super) fn owned<T>(work: &crate::NativeMaterializationWork, bytes: u64,
    build: impl FnOnce() -> Result<T, NativeMountError>) -> Result<DarwinOwned<T>, NativeMountError>
{
    let allocation = if bytes == 0 { None }
        else { Some(work.claim(bytes, 1, crate::WorkCounters::UNCHARGED)?) };
    Ok(DarwinOwned { value: build()?, allocation })
}

pub(super) fn copying<T>(work: &crate::NativeMaterializationWork, delta: crate::WorkCounters,
    copy: impl FnOnce() -> T) -> Result<T, NativeMountError>
{
    work.admit(delta)?;
    let value = copy();
    work.record_admitted(delta)?;
    Ok(value)
}

pub(super) fn shared<T>(work: &crate::NativeMaterializationWork, value: T)
    -> Result<Arc<DarwinOwned<T>>, NativeMountError>
{
    let bytes = std::mem::size_of::<DarwinOwned<T>>().checked_add(2 * std::mem::size_of::<usize>())
        .ok_or(crate::WorkError::Overflow)? as u64;
    Ok(Arc::new(owned(work, bytes, || Ok(value))?))
}
