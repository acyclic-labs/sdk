//! Typed usage, cost, and completion metadata carried by `ModelEvent::Completed`.

use acyclic_harness::model::ModelEvent;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::ops::AddAssign;

/// Token usage and provider-reported cost of one model request.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Usage {
    /// Prompt tokens billed.
    pub prompt_tokens: u64,
    /// Completion tokens billed.
    pub completion_tokens: u64,
    /// Total tokens billed.
    pub total_tokens: u64,
    /// Prompt tokens served from the provider cache.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_tokens: Option<u64>,
    /// Prompt tokens written to the provider cache.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_tokens: Option<u64>,
    /// Completion tokens spent on reasoning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u64>,
    /// Credits charged by the provider for this request (`OpenRouter`: USD).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost: Option<f64>,
    /// Cost charged by the upstream inference provider, when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_inference_cost: Option<f64>,
    /// Whether the request ran on a provider-side bring-your-own key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_byok: Option<bool>,
}

impl Usage {
    /// Reads an OpenAI-compatible `usage` object, including `OpenRouter`'s
    /// `cost`, `is_byok`, `cost_details`, and token detail extensions.
    ///
    /// Returns `None` for `null` or a non-object value.
    #[must_use]
    pub fn from_wire(usage: &Value) -> Option<Self> {
        usage.as_object()?;
        let count = |pointer: &str| usage.pointer(pointer).and_then(Value::as_u64);
        let prompt_tokens = count("/prompt_tokens").unwrap_or(0);
        let completion_tokens = count("/completion_tokens").unwrap_or(0);
        Some(Self {
            prompt_tokens,
            completion_tokens,
            total_tokens: count("/total_tokens").unwrap_or(prompt_tokens + completion_tokens),
            cached_tokens: count("/prompt_tokens_details/cached_tokens"),
            cache_write_tokens: count("/prompt_tokens_details/cache_write_tokens"),
            reasoning_tokens: count("/completion_tokens_details/reasoning_tokens"),
            cost: usage.get("cost").and_then(Value::as_f64),
            upstream_inference_cost: usage
                .pointer("/cost_details/upstream_inference_cost")
                .and_then(Value::as_f64),
            is_byok: usage.get("is_byok").and_then(Value::as_bool),
        })
    }

    /// Sums the usage of every `Completed` event in `events`, for example one
    /// agent's replayed Harness journal.
    pub fn total<'a>(events: impl IntoIterator<Item = &'a ModelEvent>) -> Self {
        let mut total = Self::default();
        for event in events {
            if let Some(Ok(metadata)) = CompletionMetadata::from_event(event)
                && let Some(usage) = metadata.usage
            {
                total += usage;
            }
        }
        total
    }
}

impl AddAssign for Usage {
    fn add_assign(&mut self, other: Self) {
        fn sum<T: std::ops::Add<Output = T>>(a: Option<T>, b: Option<T>) -> Option<T> {
            match (a, b) {
                (Some(a), Some(b)) => Some(a + b),
                (a, b) => a.or(b),
            }
        }
        self.prompt_tokens += other.prompt_tokens;
        self.completion_tokens += other.completion_tokens;
        self.total_tokens += other.total_tokens;
        self.cached_tokens = sum(self.cached_tokens, other.cached_tokens);
        self.cache_write_tokens = sum(self.cache_write_tokens, other.cache_write_tokens);
        self.reasoning_tokens = sum(self.reasoning_tokens, other.reasoning_tokens);
        self.cost = sum(self.cost, other.cost);
        self.upstream_inference_cost =
            sum(self.upstream_inference_cost, other.upstream_inference_cost);
        self.is_byok = match (self.is_byok, other.is_byok) {
            (Some(a), Some(b)) => Some(a || b),
            (a, b) => a.or(b),
        };
    }
}

/// Metadata of `ModelEvent::Completed` emitted by this crate's providers.
///
/// It is durable: the stock executor journals it verbatim and returns the final
/// step's copy as `TurnOutput::metadata`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CompletionMetadata {
    /// Provider generation id.
    pub id: Option<String>,
    /// Model that actually served the request.
    pub model: Option<String>,
    /// Upstream provider that served the request, when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// Provider finish reason.
    pub finish_reason: Option<String>,
    /// Attribution id sent as the request `user`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// Usage and cost, when the provider reported them.
    pub usage: Option<Usage>,
}

impl CompletionMetadata {
    /// Decodes the metadata of a `Completed` event; `None` for other events.
    #[must_use]
    pub fn from_event(event: &ModelEvent) -> Option<serde_json::Result<Self>> {
        match event {
            ModelEvent::Completed { metadata } => Some(serde_json::from_value(metadata.clone())),
            _ => None,
        }
    }

    /// Encodes the metadata as the `Completed` event payload.
    #[must_use]
    pub fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}
