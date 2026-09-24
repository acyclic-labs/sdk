#![deny(unsafe_code)]
#![doc = include_str!("../README.md")]

pub mod config;
pub mod error;
pub mod openai_compat;
pub mod usage;

pub use config::{AppAttribution, Dialect, OPENROUTER_BASE_URL, ProviderConfig, RetryPolicy};
pub use error::{BudgetReason, ProviderError, ProviderErrorCode};
pub use openai_compat::OpenAiCompatibleProvider;
pub use usage::{CompletionMetadata, Usage};
