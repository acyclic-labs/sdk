//! Plugins: the unit of default functionality (FILESYSTEM §2.6).
//!
//! A plugin is a directory, `plugins/<name>/`, laid out like a home: a
//! `plugin.ts` manifest, `hooks/`, skills, commands and tools. `plugins.ts`
//! is the one manifest that enables plugins and holds their settings; it
//! composes over the layers like every settings entry point
//! ([`crate::config::compose`]). An enabled plugin's directory is a layer
//! fragment just under the layer that enabled it. A fragment never
//! contributes `plugins.ts` or `policy.ts`: the loader reads only the files
//! named here, so the inclusion graph is flat.
//!
//! The loader turns the enabled set into hook rules for the one hook runner
//! ([`crate::hooks`]): a plugin's `hooks/before-tool.{sh,ps1}` becomes a
//! before-tool hook judged by the rewrite-safe chain, pinned by its bytes and
//! its settings. A hook this host cannot run yet makes `plugins.ts` invalid,
//! so the host retains the last admitted set or refuses the session. The
//! admission names the hook; an enabled hook is never silently skipped.
//! The authoring types are generated
//! from these decode shapes by [`crate::config::typescript`].
#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(not(target_arch = "wasm32"))]
pub use native::*;
#[cfg(not(target_arch = "wasm32"))]
mod after;
#[cfg(not(target_arch = "wasm32"))]
pub use after::{AfterHook, seed};

#[cfg(not(target_arch = "wasm32"))]
mod program;
#[cfg(not(target_arch = "wasm32"))]
pub use program::{BoundHook, LoadedProgram, load_program};
