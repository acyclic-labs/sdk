use super::NativeDemandReadMeter;
use std::cell::Cell;

std::thread_local! {
    static TARGET: Cell<(*const NativeDemandReadMeter, usize)> = const { Cell::new((std::ptr::null(), 0)) };
}

pub(super) fn visit(meter: &NativeDemandReadMeter) {
    TARGET.with(|target| {
        let (pointer, count) = target.get();
        if std::ptr::eq(pointer, meter) {
            target.set((pointer, count + 1));
        }
    });
}

pub(super) fn count<T>(meter: &NativeDemandReadMeter, operation: impl FnOnce() -> T) -> (T, usize) {
    struct Restore((*const NativeDemandReadMeter, usize));
    impl Drop for Restore {
        fn drop(&mut self) {
            TARGET.with(|target| target.set(self.0));
        }
    }
    let _restore = Restore(TARGET.with(|target| target.replace((meter, 0))));
    let result = operation();
    (result, TARGET.with(|target| target.get().1))
}
