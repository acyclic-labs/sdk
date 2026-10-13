include!("cases_0.rs");

#[cfg(not(target_arch = "wasm32"))]
mod native_read;

#[cfg(not(target_arch = "wasm32"))]
mod native_commit;

#[cfg(not(target_arch = "wasm32"))]
mod native_controls;
