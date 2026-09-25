//! Model adapters that implement the Harness `ModelProvider` over third-party
//! inference APIs.
//!
//! They run wherever the agent runs (a customer machine or a managed sandbox),
//! so they are public here rather than in a private service.
//! [`OpenAiCompatibleProvider`] streams the OpenAI-compatible chat completions
//! API with tool calling. [`ProviderConfig::openrouter`] selects the
//! `OpenRouter` dialect: app attribution headers (`HTTP-Referer`, `X-Title`), a
//! per-request `user` attribution id, inline usage accounting whose `cost` lands
//! in the `ModelEvent::Completed` metadata as a typed [`CompletionMetadata`], and
//! a typed [`ProviderError`] that separates an exhausted credit or key budget
//! (stop the work) from a rate limit (retry). Provider-specific types never
//! enter the Harness core; this module is compiled only with the `models`
//! feature.

pub mod config;
pub mod error;
pub mod openai_compat;
pub mod usage;

pub use config::{AppAttribution, Dialect, OPENROUTER_BASE_URL, ProviderConfig, RetryPolicy};
pub use error::{BudgetReason, ProviderError, ProviderErrorCode};
pub use openai_compat::OpenAiCompatibleProvider;
pub use usage::{CompletionMetadata, Usage};
