//! Thin ownership adapter over QuickJS. Every engine allocation uses its one
//! configured C allocator; Rust owns only the admitted runtime/callback frames.
#![allow(unsafe_code, reason = "QuickJS C ownership and callback ABI")]

use super::*;
use rquickjs_sys as qjs;
use std::{
    cell::UnsafeCell,
    ffi::CStr,
    marker::PhantomData,
    panic::{AssertUnwindSafe, catch_unwind},
    ptr::{self, NonNull},
};

pub(super) fn at_stage<T>(result: Result<T>, stage: &'static str) -> Result<T> {
    result.map_err(|error| match error {
        Error::ToolRejected {
            code,
            reason: "code interpreter rejected the program",
        } => Error::ToolRejected {
            code,
            reason: stage,
        },
        error => error,
    })
}

#[derive(Clone)]
pub(super) struct Runtime(Arc<RuntimeInner>);

struct RuntimeInner {
    raw: NonNull<qjs::JSRuntime>,
    gate: Mutex<()>,
    #[cfg(feature = "filesystem")]
    allocator: Option<Arc<native_allocator::AllocatorState>>,
    _credit: crate::NativeCustody,
}

// SAFETY: all execution, reference destruction and runtime state access is
// serialized by gate. No raw pointer escapes this adapter.
unsafe impl Send for RuntimeInner {}
unsafe impl Sync for RuntimeInner {}

impl Drop for RuntimeInner {
    fn drop(&mut self) {
        // SAFETY: contexts retain this exact Arc, so no context remains. The
        // allocator field is live until after JS_FreeRuntime returns.
        unsafe { qjs::JS_FreeRuntime(self.raw.as_ptr()) };
    }
}

impl Runtime {
    fn enter(&self) -> std::sync::MutexGuard<'_, ()> {
        let guard = lock(&self.0.gate);
        // SAFETY: the runtime can move between executor threads while dormant.
        // QuickJS must observe the current thread's stack before engine work.
        unsafe { qjs::JS_UpdateStackTop(self.0.raw.as_ptr()) };
        guard
    }
    pub(super) fn new() -> Result<Self> {
        // SAFETY: creates the sole engine with QuickJS's ordinary allocator.
        let raw = unsafe { qjs::JS_NewRuntime() };
        let raw =
            NonNull::new(raw).ok_or_else(|| rejected("code interpreter allocation failed"))?;
        Ok(Self(Arc::new(RuntimeInner {
            raw,
            gate: Mutex::new(()),
            #[cfg(feature = "filesystem")]
            allocator: None,
            _credit: None,
        })))
    }

    #[cfg(feature = "filesystem")]
    pub(super) fn with_allocator(allocator: Arc<native_allocator::AllocatorState>) -> Result<Self> {
        let credit = allocator.claim(
            acyclic_native_runtime::arc_allocation_bytes::<RuntimeInner>() as u64,
            1,
        )?;
        // SAFETY: callbacks borrow the exact stable Arc retained by RuntimeInner
        // and remain valid throughout runtime destruction, including refusal.
        let raw = unsafe {
            qjs::JS_NewRuntime2(
                &native_allocator::FUNCTIONS,
                Arc::as_ptr(&allocator).cast_mut().cast(),
            )
        };
        if raw.is_null() {
            return Err(allocator
                .take_failure()
                .unwrap_or_else(|| rejected("code interpreter allocation failed")));
        }
        let runtime = Self(Arc::new(RuntimeInner {
            raw: NonNull::new(raw).ok_or_else(|| rejected("code interpreter allocation failed"))?,
            gate: Mutex::new(()),
            allocator: Some(allocator),
            _credit: Some(credit),
        }));
        if let Some(error) = runtime
            .0
            .allocator
            .as_ref()
            .and_then(|state| state.take_failure())
        {
            return Err(error);
        }
        Ok(runtime)
    }

    pub(super) fn set_memory_limit(&self, bytes: usize) {
        let _gate = self.enter();
        // SAFETY: this live runtime is exclusively locked.
        unsafe { qjs::JS_SetMemoryLimit(self.0.raw.as_ptr(), bytes as _) };
    }

    pub(super) fn execute_pending_job(&self) -> Result<bool> {
        let _gate = self.enter();
        self.execute_pending_job_locked()
    }

    fn execute_pending_job_locked(&self) -> Result<bool> {
        let mut context = ptr::null_mut();
        // SAFETY: runtime lock is held and output points to a live stack slot.
        let result = unsafe { qjs::JS_ExecutePendingJob(self.0.raw.as_ptr(), &mut context) };
        if result < 0 {
            if !context.is_null() {
                // SAFETY: QuickJS supplies the failing live context.
                unsafe { qjs::JS_FreeValue(context, qjs::JS_GetException(context)) };
            }
            Err(rejected("code microtask failed"))
        } else {
            Ok(result != 0)
        }
    }

    fn claim(&self, bytes: u64, operations: u64) -> Result<crate::NativeCustody> {
        #[cfg(feature = "filesystem")]
        if let Some(allocator) = &self.0.allocator {
            return allocator.claim(bytes, operations).map(Some);
        }
        let _ = (bytes, operations);
        Ok(None)
    }

    pub(super) fn interrupt<'a>(&'a self, state: &'a Interrupt<'a>) -> InterruptLease<'a> {
        let _gate = self.enter();
        // SAFETY: state is borrowed through the guard and every return/unwind
        // removes this callback before the borrowed stack state can disappear.
        unsafe {
            qjs::JS_SetInterruptHandler(
                self.0.raw.as_ptr(),
                Some(interrupt),
                (state as *const Interrupt<'_>).cast_mut().cast(),
            )
        };
        InterruptLease {
            runtime: self,
            _state: state,
        }
    }
}

pub(super) struct Interrupt<'a> {
    pub(super) control: &'a Arc<dyn CodeControl>,
    pub(super) failure: &'a Mutex<Option<Error>>,
}

pub(super) struct InterruptLease<'a> {
    runtime: &'a Runtime,
    _state: &'a Interrupt<'a>,
}
impl Drop for InterruptLease<'_> {
    fn drop(&mut self) {
        let _gate = self.runtime.enter();
        // SAFETY: cleanup cannot enter user host effects; its interrupt always
        // rejects. Drain original jobs before retiring the borrowed producer.
        unsafe {
            qjs::JS_SetInterruptHandler(self.runtime.0.raw.as_ptr(), Some(stop), ptr::null_mut())
        };
        while self.runtime.execute_pending_job_locked().unwrap_or(true) {}
        // SAFETY: no callback may retain the expired stack producer.
        unsafe { qjs::JS_SetInterruptHandler(self.runtime.0.raw.as_ptr(), None, ptr::null_mut()) };
    }
}

unsafe extern "C" fn stop(_: *mut qjs::JSRuntime, _: *mut qjs::c_void) -> qjs::c_int {
    1
}
unsafe extern "C" fn interrupt(_: *mut qjs::JSRuntime, opaque: *mut qjs::c_void) -> qjs::c_int {
    // SAFETY: InterruptLease retains this stack object for the entire callback.
    let state = unsafe { &*opaque.cast::<Interrupt<'_>>() };
    match catch_unwind(AssertUnwindSafe(|| state.control.check())) {
        Ok(Ok(())) => 0,
        result => {
            let error = match result {
                Ok(Err(error)) => error,
                _ => rejected("code execution control panicked"),
            };
            let mut failure = lock(state.failure);
            if failure.is_none() {
                *failure = Some(error);
            }
            1
        }
    }
}

pub(super) struct Callbacks {
    pub(super) queue: Arc<Retained<Mutex<PendingQueue>>>,
    pub(super) protocol: Arc<Retained<Protocol>>,
    pub(super) buffers: Buffers,
    pub(super) recursive: Owned<String>,
    pub(super) ordinal: u64,
}

pub(super) struct Context {
    raw: NonNull<qjs::JSContext>,
    runtime: Runtime,
    _callbacks: Option<Box<CallbackOwner>>,
}
struct CallbackOwner {
    value: UnsafeCell<Callbacks>,
    _credit: crate::NativeCustody,
}

// SAFETY: the same runtime gate protects every access and context destruction.
unsafe impl Send for Context {}
unsafe impl Sync for Context {}

impl Context {
    pub(super) fn new(runtime: &Runtime, callbacks: Option<Callbacks>) -> Result<Self> {
        let callbacks = callbacks
            .map(|value| {
                let credit = runtime.claim(std::mem::size_of::<CallbackOwner>() as u64, 1)?;
                Ok::<_, Error>(Box::new(CallbackOwner {
                    value: UnsafeCell::new(value),
                    _credit: credit,
                }))
            })
            .transpose()?;
        let _gate = runtime.enter();
        // SAFETY: exclusive engine access; callback frame is a stable Box and
        // stays live through JS_FreeContext, without a Ctx reference cycle.
        let raw = unsafe { qjs::JS_NewContext(runtime.0.raw.as_ptr()) };
        let raw = NonNull::new(raw).ok_or_else(|| rejected("code context allocation failed"))?;
        if let Some(callbacks) = &callbacks {
            unsafe { qjs::JS_SetContextOpaque(raw.as_ptr(), callbacks.value.get().cast()) };
        }
        Ok(Self {
            raw,
            runtime: runtime.clone(),
            _callbacks: callbacks,
        })
    }

    pub(super) fn with<T>(&self, action: impl for<'a> FnOnce(Ctx<'a>) -> Result<T>) -> Result<T> {
        let _gate = self.runtime.enter();
        action(Ctx {
            raw: self.raw.as_ptr(),
            _lifetime: PhantomData,
        })
    }

    pub(super) fn with_pair<T>(
        &self,
        other: &Self,
        action: impl for<'a> FnOnce(Ctx<'a>, Ctx<'a>) -> Result<T>,
    ) -> Result<T> {
        if !Arc::ptr_eq(&self.runtime.0, &other.runtime.0) {
            return Err(rejected("code contexts belong to different interpreters"));
        }
        let _gate = self.runtime.enter();
        action(
            Ctx {
                raw: self.raw.as_ptr(),
                _lifetime: PhantomData,
            },
            Ctx {
                raw: other.raw.as_ptr(),
                _lifetime: PhantomData,
            },
        )
    }
}

impl Drop for Context {
    fn drop(&mut self) {
        let _gate = self.runtime.enter();
        // SAFETY: only this owner destroys the context and its opaque frame
        // remains retained through release. C function realm cycles may await
        // QuickJS GC; clear opaque so none can reference retired Rust state.
        unsafe {
            qjs::JS_SetContextOpaque(self.raw.as_ptr(), ptr::null_mut());
            qjs::JS_FreeContext(self.raw.as_ptr());
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct Ctx<'a> {
    raw: *mut qjs::JSContext,
    _lifetime: PhantomData<&'a Context>,
}
pub(super) struct Value<'a> {
    ctx: Ctx<'a>,
    raw: qjs::JSValue,
}
pub(super) struct Text<'a> {
    ctx: Ctx<'a>,
    raw: NonNull<qjs::c_char>,
    length: usize,
}

impl<'a> Ctx<'a> {
    fn checked(self, raw: qjs::JSValue) -> Result<Value<'a>> {
        // SAFETY: live locked context and an owned return value from its engine.
        if unsafe { qjs::JS_IsException(raw) } {
            unsafe { qjs::JS_FreeValue(self.raw, qjs::JS_GetException(self.raw)) };
            Err(rejected("code interpreter rejected the program"))
        } else {
            Ok(Value { ctx: self, raw })
        }
    }
    pub(super) fn eval(self, source: &CStr) -> Result<Value<'a>> {
        // SAFETY: CStr guarantees the exact trailing zero required by JS_Eval.
        self.checked(unsafe {
            qjs::JS_Eval(
                self.raw,
                source.as_ptr(),
                source.to_bytes().len() as _,
                c"code".as_ptr(),
                qjs::JS_EVAL_TYPE_GLOBAL as i32 | qjs::JS_EVAL_FLAG_STRICT as i32,
            )
        })
    }
    pub(super) fn string(self, text: &str) -> Result<Value<'a>> {
        // SAFETY: JS_NewStringLen copies this borrowed UTF-8 before returning.
        self.checked(unsafe {
            qjs::JS_NewStringLen(self.raw, text.as_ptr().cast(), text.len() as _)
        })
    }
    pub(super) fn number(self, number: u32) -> Value<'a> {
        Value {
            ctx: self,
            raw: qjs::JS_NewFloat64(f64::from(number)),
        }
    }
    pub(super) fn array(self) -> Result<Value<'a>> {
        self.checked(unsafe { qjs::JS_NewArray(self.raw) })
    }
    pub(super) fn global(self) -> Result<Value<'a>> {
        self.checked(unsafe { qjs::JS_GetGlobalObject(self.raw) })
    }
    pub(super) fn callback(self, name: &CStr, enqueue: bool) -> Result<Value<'a>> {
        self.checked(unsafe {
            qjs::JS_NewCFunction2(
                self.raw,
                Some(if enqueue {
                    enqueue_callback
                } else {
                    admit_callback
                }),
                name.as_ptr(),
                if enqueue { 2 } else { 0 },
                qjs::JSCFunctionEnum_JS_CFUNC_generic,
                0,
            )
        })
    }
}

impl<'a> Value<'a> {
    pub(super) fn get(&self, name: &CStr) -> Result<Self> {
        self.ctx
            .checked(unsafe { qjs::JS_GetPropertyStr(self.ctx.raw, self.raw, name.as_ptr()) })
    }
    pub(super) fn set(&self, name: &CStr, value: Self) -> Result<()> {
        // JS_SetProperty consumes the value on success and refusal alike.
        let raw = value.raw;
        std::mem::forget(value);
        if unsafe { qjs::JS_SetPropertyStr(self.ctx.raw, self.raw, name.as_ptr(), raw) } < 0 {
            self.ctx.checked(qjs::JS_EXCEPTION).map(drop)
        } else {
            Ok(())
        }
    }
    pub(super) fn set_index(&self, index: u32, value: Self) -> Result<()> {
        let raw = value.raw;
        std::mem::forget(value);
        if unsafe { qjs::JS_SetPropertyUint32(self.ctx.raw, self.raw, index, raw) } < 0 {
            self.ctx.checked(qjs::JS_EXCEPTION).map(drop)
        } else {
            Ok(())
        }
    }
    pub(super) fn call<const N: usize>(&self, arguments: [Self; N]) -> Result<Self> {
        let mut raw: [qjs::JSValue; N] = std::array::from_fn(|index| arguments[index].raw);
        self.ctx.checked(unsafe {
            qjs::JS_Call(
                self.ctx.raw,
                self.raw,
                qjs::JS_UNDEFINED,
                N as i32,
                raw.as_mut_ptr(),
            )
        })
    }
    pub(super) fn text(&self) -> Result<Text<'a>> {
        let mut length = 0;
        let raw = unsafe { qjs::JS_ToCStringLen(self.ctx.raw, &mut length, self.raw) };
        let raw = NonNull::new(raw.cast_mut())
            .ok_or_else(|| rejected("code string allocation failed"))?;
        Ok(Text {
            ctx: self.ctx,
            raw,
            length,
        })
    }
}
impl Drop for Value<'_> {
    fn drop(&mut self) {
        unsafe { qjs::JS_FreeValue(self.ctx.raw, self.raw) };
    }
}
impl Text<'_> {
    pub(super) fn as_str(&self) -> Result<&str> {
        let bytes =
            unsafe { std::slice::from_raw_parts(self.raw.as_ptr().cast::<u8>(), self.length) };
        std::str::from_utf8(bytes).map_err(|_| rejected("code string is not UTF-8"))
    }
}
impl Drop for Text<'_> {
    fn drop(&mut self) {
        unsafe { qjs::JS_FreeCString(self.ctx.raw, self.raw.as_ptr()) };
    }
}

unsafe fn callback<'a>(ctx: *mut qjs::JSContext) -> Result<&'a mut Callbacks> {
    let opaque = unsafe { qjs::JS_GetContextOpaque(ctx) };
    if opaque.is_null() {
        return Err(rejected("code callback has no original owner"));
    }
    // SAFETY: installed once by Context::new, exclusively accessed on this
    // engine's lock and kept alive until the context is destroyed.
    Ok(unsafe { &mut *opaque.cast::<Callbacks>() })
}
unsafe fn throw(ctx: *mut qjs::JSContext) -> qjs::JSValue {
    unsafe { qjs::JS_ThrowTypeError(ctx, c"code host call refused".as_ptr()) }
}
unsafe extern "C" fn enqueue_callback(
    ctx: *mut qjs::JSContext,
    _: qjs::JSValue,
    argc: i32,
    argv: *mut qjs::JSValue,
) -> qjs::JSValue {
    let result = catch_unwind(AssertUnwindSafe(|| {
        let callback = unsafe { callback(ctx) }?;
        let context = Ctx {
            raw: ctx,
            _lifetime: PhantomData,
        };
        let result = (|| {
            if argc != 2 {
                return Err(rejected("code callback arguments are invalid"));
            }
            let arguments = unsafe { std::slice::from_raw_parts(argv, 2) };
            // Coercing an arbitrary object could execute JS while this
            // callback has exclusive access to its opaque frame.
            if !unsafe { qjs::JS_IsString(arguments[0]) && qjs::JS_IsString(arguments[1]) } {
                return Err(rejected("code callback arguments are not strings"));
            }
            // Borrowed argv values remain owned by QuickJS. Text has its own
            // exact C reference and is released before returning to the VM.
            let name = Value {
                ctx: context,
                raw: unsafe { qjs::JS_DupValue(ctx, arguments[0]) },
            };
            let arguments = Value {
                ctx: context,
                raw: unsafe { qjs::JS_DupValue(ctx, arguments[1]) },
            };
            let name = name.text()?;
            let arguments = arguments.text()?;
            if name.as_str()? == callback.recursive.value().as_str() {
                return Err(rejected("code tool cannot invoke itself"));
            }
            let current = callback.ordinal;
            callback.ordinal = current
                .checked_add(1)
                .ok_or_else(|| rejected("code host-call ordinal overflow"))?;
            let name = callback.buffers.text(name.as_str()?)?;
            let arguments = callback.buffers.text(arguments.as_str()?)?;
            let ordinal = callback.buffers.format(format_args!("{current}"))?;
            lock(&callback.queue).push(QueuedCall {
                ordinal: current,
                name,
                arguments,
            })?;
            let value = context.string(&ordinal.value)?;
            let raw = value.raw;
            std::mem::forget(value);
            Ok(raw)
        })();
        match result {
            Ok(value) => Ok(value),
            Err(error) => {
                lock(&callback.queue).fail(error);
                Err(rejected("code host call refused"))
            }
        }
    }));
    match result {
        Ok(Ok(value)) => value,
        Err(_) => {
            if let Ok(callback) = unsafe { callback(ctx) } {
                lock(&callback.queue).fail(rejected("code host callback panicked"));
            }
            unsafe { throw(ctx) }
        }
        Ok(Err(_)) => unsafe { throw(ctx) },
    }
}
unsafe extern "C" fn admit_callback(
    ctx: *mut qjs::JSContext,
    _: qjs::JSValue,
    _: i32,
    _: *mut qjs::JSValue,
) -> qjs::JSValue {
    let result = catch_unwind(AssertUnwindSafe(|| {
        unsafe { callback(ctx) }.and_then(|callback| callback.protocol.consume())
    }));
    match result {
        Ok(Ok(())) => qjs::JS_UNDEFINED,
        Err(_) => {
            if let Ok(callback) = unsafe { callback(ctx) } {
                callback.protocol.forged.store(true, Ordering::Release);
            }
            unsafe { throw(ctx) }
        }
        Ok(Err(_)) => unsafe { throw(ctx) },
    }
}
