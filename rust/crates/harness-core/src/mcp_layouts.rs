//! Heap layouts of the rmcp model types MCP responses and requests decode.
use crate::heap_layout::{Alloc, HeapLayout, JsonLayout};
use rmcp::model::{
    Annotations, AudioContent, CacheScope, CallToolRequestParams, CallToolResult, ContentBlock,
    EmbeddedResource, GetPromptRequestParams, GetPromptResult, Icon, IconTheme, ImageContent,
    InputResponses, JsonObject, ListPromptsResult, ListResourceTemplatesResult,
    ListResourcesResult, ListToolsResult, MetaObject, PaginatedRequestParams, Prompt,
    PromptArgument, PromptMessage, ReadResourceRequestParams, ReadResourceResult,
    RequestMetaObject, Resource, ResourceContents, ResourceTemplate, ResultType, Role, TextContent,
    Tool, ToolAnnotations,
};
use serde_json::Value;
use std::borrow::Cow;
use std::sync::Arc;

/// The layout of a type, as `#[derive(HeapLayout)]` names a field's layout.
macro_rules! of {
    ($ty:ty) => {
        <$ty as HeapLayout>::LAYOUT
    };
}

/// A struct as `#[derive(HeapLayout)]` lays it out without flattened members:
/// an object of its fields by serde name, or an array of its fields in
/// declaration order followed by a scalar that bounds the array's length.
macro_rules! record {
    ($($name:literal => $layout:expr),* $(,)?) => {
        JsonLayout::Node {
            seq: Some(Alloc::INLINE),
            map: Some(Alloc::INLINE),
            tagged: false,
            fields: &[$(($name, $layout)),*],
            elements: &[$($layout,)* &JsonLayout::Scalar],
            flatten: &[],
        }
    };
}

/// A non-generic layout in a static, as the derive emits it.
macro_rules! layout {
    ($value:expr) => {{
        static LAYOUT: JsonLayout = $value;
        &LAYOUT
    }};
}

/// A field `deserialize_cache_scope` decodes: it reads `Option<Value>` and,
/// unless null or `""`, converts it with `CacheScope::deserialize(value)`.
/// The conversion consumes the value and `CacheScope` holds no heap, so the
/// decoded `Value` is the whole allocation: no copy is re-encoded, which is
/// why this is not `Canonical<CacheScope>`.
const CACHE_SCOPE_FIELD: &JsonLayout = of!(Value);
/// A field `deserialize_ttl_ms` decodes: `Option<i64>`, clamped to `u64`.
const TTL_MS_FIELD: &JsonLayout = &JsonLayout::Scalar;
/// A field `serde_impl::json_float::option_f32` or `option_f64` decodes:
/// `Option<serde_json::Number>`, which holds no heap without
/// `arbitrary_precision`.
const FLOAT_FIELD: &JsonLayout = &JsonLayout::Scalar;
/// A field `deserialize_present_value` decodes: `Value::deserialize`, kept
/// as `Some(value)` without conversion.
const PRESENT_VALUE_FIELD: &JsonLayout = of!(Value);

/// `ResultType` has a hand-written `Deserialize` that decodes a `String`
/// and keeps it as `Cow::Owned` unless it is a known constant.
impl HeapLayout for ResultType {
    const LAYOUT: &'static JsonLayout = &JsonLayout::Scalar;
}

/// `#[serde(transparent)] struct MetaObject(pub JsonObject)`.
impl HeapLayout for MetaObject {
    const LAYOUT: &'static JsonLayout = of!(JsonObject);
}

/// `#[serde(transparent)] struct RequestMetaObject(pub MetaObject)`.
impl HeapLayout for RequestMetaObject {
    const LAYOUT: &'static JsonLayout = of!(MetaObject);
}

/// `#[serde(rename_all = "camelCase")] enum Role { User, Assistant }`,
/// externally tagged unit variants.
impl HeapLayout for Role {
    const LAYOUT: &'static JsonLayout = layout!(JsonLayout::Node {
        seq: None,
        map: Some(Alloc::INLINE),
        tagged: false,
        fields: &[
            ("user", &JsonLayout::Scalar),
            ("assistant", &JsonLayout::Scalar)
        ],
        elements: &[],
        flatten: &[],
    });
}

/// `#[serde(rename_all = "lowercase")] enum IconTheme { Light, Dark }`,
/// externally tagged unit variants.
impl HeapLayout for IconTheme {
    const LAYOUT: &'static JsonLayout = layout!(JsonLayout::Node {
        seq: None,
        map: Some(Alloc::INLINE),
        tagged: false,
        fields: &[
            ("light", &JsonLayout::Scalar),
            ("dark", &JsonLayout::Scalar)
        ],
        elements: &[],
        flatten: &[],
    });
}

/// `#[serde(rename_all = "lowercase")] enum CacheScope { Public, Private }`,
/// externally tagged unit variants. Results decode their `cacheScope` field
/// through `deserialize_cache_scope` instead, which reads a `Value` first.
impl HeapLayout for CacheScope {
    const LAYOUT: &'static JsonLayout = layout!(JsonLayout::Node {
        seq: None,
        map: Some(Alloc::INLINE),
        tagged: false,
        fields: &[
            ("public", &JsonLayout::Scalar),
            ("private", &JsonLayout::Scalar)
        ],
        elements: &[],
        flatten: &[],
    });
}

/// `#[serde(rename_all = "camelCase")] struct Icon { src, mime_type, sizes,
/// theme }`.
impl HeapLayout for Icon {
    const LAYOUT: &'static JsonLayout = layout!(record![
        "src" => of!(String),
        "mimeType" => of!(Option<String>),
        "sizes" => of!(Option<Vec<String>>),
        "theme" => of!(Option<IconTheme>),
    ]);
}

/// `#[serde(rename_all = "camelCase")] struct Annotations`: `audience`,
/// `priority` through `json_float::option_f32`, and `last_modified` renamed
/// `"lastModified"`.
impl HeapLayout for Annotations {
    const LAYOUT: &'static JsonLayout = layout!(record![
        "audience" => of!(Option<Vec<Role>>),
        "priority" => FLOAT_FIELD,
        "lastModified" => of!(Option<String>),
    ]);
}

/// `#[serde(rename_all = "camelCase")] struct ToolAnnotations`.
impl HeapLayout for ToolAnnotations {
    const LAYOUT: &'static JsonLayout = layout!(record![
        "title" => of!(Option<String>),
        "readOnlyHint" => of!(Option<bool>),
        "destructiveHint" => of!(Option<bool>),
        "idempotentHint" => of!(Option<bool>),
        "openWorldHint" => of!(Option<bool>),
    ]);
}

/// `#[serde(rename_all = "camelCase")] struct Tool`, with `meta` renamed
/// `"_meta"`; both schemas are `Arc<JsonObject>`.
impl HeapLayout for Tool {
    const LAYOUT: &'static JsonLayout = layout!(record![
        "name" => of!(Cow<'static, str>),
        "title" => of!(Option<String>),
        "description" => of!(Option<Cow<'static, str>>),
        "inputSchema" => of!(Arc<JsonObject>),
        "outputSchema" => of!(Option<Arc<JsonObject>>),
        "annotations" => of!(Option<ToolAnnotations>),
        "icons" => of!(Option<Vec<Icon>>),
        "_meta" => of!(Option<MetaObject>),
    ]);
}

/// `struct PromptArgument` without `rename_all`: `snake_case` field names.
impl HeapLayout for PromptArgument {
    const LAYOUT: &'static JsonLayout = layout!(record![
        "name" => of!(String),
        "title" => of!(Option<String>),
        "description" => of!(Option<String>),
        "required" => of!(Option<bool>),
    ]);
}

/// `#[serde(rename_all = "camelCase")] struct Prompt`, with `meta` renamed
/// `"_meta"`.
impl HeapLayout for Prompt {
    const LAYOUT: &'static JsonLayout = layout!(record![
        "name" => of!(String),
        "title" => of!(Option<String>),
        "description" => of!(Option<String>),
        "arguments" => of!(Option<Vec<PromptArgument>>),
        "icons" => of!(Option<Vec<Icon>>),
        "_meta" => of!(Option<MetaObject>),
    ]);
}

/// `#[serde(rename_all = "camelCase")] struct Resource`, with `meta`
/// renamed `"_meta"`.
impl HeapLayout for Resource {
    const LAYOUT: &'static JsonLayout = layout!(record![
        "uri" => of!(String),
        "name" => of!(String),
        "title" => of!(Option<String>),
        "description" => of!(Option<String>),
        "mimeType" => of!(Option<String>),
        "size" => of!(Option<u64>),
        "icons" => of!(Option<Vec<Icon>>),
        "_meta" => of!(Option<MetaObject>),
        "annotations" => of!(Option<Annotations>),
    ]);
}

/// `#[serde(rename_all = "camelCase")] struct ResourceTemplate`, with `meta`
/// renamed `"_meta"`.
impl HeapLayout for ResourceTemplate {
    const LAYOUT: &'static JsonLayout = layout!(record![
        "uriTemplate" => of!(String),
        "name" => of!(String),
        "title" => of!(Option<String>),
        "description" => of!(Option<String>),
        "mimeType" => of!(Option<String>),
        "icons" => of!(Option<Vec<Icon>>),
        "_meta" => of!(Option<MetaObject>),
        "annotations" => of!(Option<Annotations>),
    ]);
}

/// `#[serde(untagged)] enum ResourceContents`, each struct variant
/// `#[serde(rename_all = "camelCase")]` with `meta` renamed `"_meta"`:
/// buffered as `Content`, then tried as `TextResourceContents { uri,
/// mime_type, text, meta }` and `BlobResourceContents { uri, mime_type,
/// blob, meta }`.
impl HeapLayout for ResourceContents {
    const LAYOUT: &'static JsonLayout = layout!(JsonLayout::Any {
        tagged: true,
        of: &[
            &record![
                "uri" => of!(String),
                "mimeType" => of!(Option<String>),
                "text" => of!(String),
                "_meta" => of!(Option<MetaObject>),
            ],
            &record![
                "uri" => of!(String),
                "mimeType" => of!(Option<String>),
                "blob" => of!(String),
                "_meta" => of!(Option<MetaObject>),
            ],
        ],
    });
}

/// `#[serde(rename_all = "camelCase")] struct TextContent`, with `meta`
/// renamed `"_meta"`.
impl HeapLayout for TextContent {
    const LAYOUT: &'static JsonLayout = layout!(record![
        "text" => of!(String),
        "_meta" => of!(Option<MetaObject>),
        "annotations" => of!(Option<Annotations>),
    ]);
}

/// `#[serde(rename_all = "camelCase")] struct ImageContent`, with `meta`
/// renamed `"_meta"`.
impl HeapLayout for ImageContent {
    const LAYOUT: &'static JsonLayout = layout!(record![
        "data" => of!(String),
        "mimeType" => of!(String),
        "_meta" => of!(Option<MetaObject>),
        "annotations" => of!(Option<Annotations>),
    ]);
}

/// `#[serde(rename_all = "camelCase")] struct AudioContent`, with `meta`
/// renamed `"_meta"`.
impl HeapLayout for AudioContent {
    const LAYOUT: &'static JsonLayout = layout!(record![
        "data" => of!(String),
        "mimeType" => of!(String),
        "_meta" => of!(Option<MetaObject>),
        "annotations" => of!(Option<Annotations>),
    ]);
}

/// `#[serde(rename_all = "camelCase")] struct EmbeddedResource`, with `meta`
/// renamed `"_meta"`.
impl HeapLayout for EmbeddedResource {
    const LAYOUT: &'static JsonLayout = layout!(record![
        "resource" => of!(ResourceContents),
        "_meta" => of!(Option<MetaObject>),
        "annotations" => of!(Option<Annotations>),
    ]);
}

/// `#[serde(tag = "type", rename_all = "snake_case")] enum ContentBlock`
/// with newtype variants `Text(TextContent)`, `Image(ImageContent)`,
/// `Audio(AudioContent)`, `Resource(EmbeddedResource)` and
/// `ResourceLink(Resource)`: internally tagged, so the object is buffered
/// as `Content`, carries `"type"`, flattens every variant's members, and is
/// refused as an array.
impl HeapLayout for ContentBlock {
    const LAYOUT: &'static JsonLayout = layout!(JsonLayout::Node {
        seq: None,
        map: Some(Alloc::INLINE),
        tagged: true,
        fields: &[("type", &JsonLayout::Scalar)],
        elements: &[],
        flatten: &[
            of!(TextContent),
            of!(ImageContent),
            of!(AudioContent),
            of!(EmbeddedResource),
            of!(Resource),
        ],
    });
}

/// `struct PromptMessage { role, content }` without `rename_all`.
impl HeapLayout for PromptMessage {
    const LAYOUT: &'static JsonLayout = layout!(record![
        "role" => of!(Role),
        "content" => of!(ContentBlock),
    ]);
}

/// The `paginated_result!` struct: `#[serde(rename_all = "camelCase")]`
/// with `result_type`, `meta` renamed `"_meta"`, `next_cursor`, `ttl_ms`
/// through `deserialize_ttl_ms`, `cache_scope` through
/// `deserialize_cache_scope`, then the item field.
macro_rules! paginated {
    ($ty:ty, $items:literal => $item:ty) => {
        impl HeapLayout for $ty {
            const LAYOUT: &'static JsonLayout = layout!(record![
                "resultType" => of!(Option<ResultType>),
                "_meta" => of!(Option<MetaObject>),
                "nextCursor" => of!(Option<String>),
                "ttlMs" => TTL_MS_FIELD,
                "cacheScope" => CACHE_SCOPE_FIELD,
                $items => of!($item),
            ]);
        }
    };
}
paginated!(ListToolsResult, "tools" => Vec<Tool>);
paginated!(ListResourcesResult, "resources" => Vec<Resource>);
paginated!(ListResourceTemplatesResult, "resourceTemplates" => Vec<ResourceTemplate>);
paginated!(ListPromptsResult, "prompts" => Vec<Prompt>);

/// `#[serde(rename_all = "camelCase")] struct ReadResourceResult`:
/// `result_type`, `ttl_ms` through `deserialize_ttl_ms`, `cache_scope`
/// through `deserialize_cache_scope`, `contents`, and `meta` renamed
/// `"_meta"`.
impl HeapLayout for ReadResourceResult {
    const LAYOUT: &'static JsonLayout = layout!(record![
        "resultType" => of!(Option<ResultType>),
        "ttlMs" => TTL_MS_FIELD,
        "cacheScope" => CACHE_SCOPE_FIELD,
        "contents" => of!(Vec<ResourceContents>),
        "_meta" => of!(Option<MetaObject>),
    ]);
}

/// `#[serde(rename_all = "camelCase")] struct GetPromptResult`:
/// `result_type`, `description`, `messages`, and `meta` renamed `"_meta"`.
impl HeapLayout for GetPromptResult {
    const LAYOUT: &'static JsonLayout = layout!(record![
        "resultType" => of!(Option<ResultType>),
        "description" => of!(Option<String>),
        "messages" => of!(Vec<PromptMessage>),
        "_meta" => of!(Option<MetaObject>),
    ]);
}

/// `CallToolResult` has a hand-written `Deserialize` that decodes a derived
/// `#[serde(rename_all = "camelCase")] struct Helper { result_type, content:
/// Option<Vec<ContentBlock>>, structured_content (through
/// deserialize_present_value), is_error, meta (renamed "_meta") }` and moves
/// its fields into the result without allocating.
impl HeapLayout for CallToolResult {
    const LAYOUT: &'static JsonLayout = layout!(record![
        "resultType" => of!(Option<ResultType>),
        "content" => of!(Option<Vec<ContentBlock>>),
        "structuredContent" => PRESENT_VALUE_FIELD,
        "isError" => of!(Option<bool>),
        "_meta" => of!(Option<MetaObject>),
    ]);
}

/// `#[serde(rename_all = "camelCase")] struct PaginatedRequestParams`:
/// `meta` renamed `"_meta"`, then `cursor`.
impl HeapLayout for PaginatedRequestParams {
    const LAYOUT: &'static JsonLayout = layout!(record![
        "_meta" => of!(Option<RequestMetaObject>),
        "cursor" => of!(Option<String>),
    ]);
}

/// `#[serde(rename_all = "camelCase")] struct ReadResourceRequestParams`:
/// `meta` renamed `"_meta"`, `uri`, `input_responses` (a
/// `BTreeMap<String, Value>`) and `request_state`.
impl HeapLayout for ReadResourceRequestParams {
    const LAYOUT: &'static JsonLayout = layout!(record![
        "_meta" => of!(Option<RequestMetaObject>),
        "uri" => of!(String),
        "inputResponses" => of!(Option<InputResponses>),
        "requestState" => of!(Option<String>),
    ]);
}

/// `#[serde(rename_all = "camelCase")] struct GetPromptRequestParams`:
/// `meta` renamed `"_meta"`, `name`, `arguments`, `input_responses` and
/// `request_state`.
impl HeapLayout for GetPromptRequestParams {
    const LAYOUT: &'static JsonLayout = layout!(record![
        "_meta" => of!(Option<RequestMetaObject>),
        "name" => of!(String),
        "arguments" => of!(Option<JsonObject>),
        "inputResponses" => of!(Option<InputResponses>),
        "requestState" => of!(Option<String>),
    ]);
}

/// `#[serde(rename_all = "camelCase")] struct CallToolRequestParams`:
/// `meta` renamed `"_meta"`, `name` as `Cow<'static, str>`, `arguments`,
/// `input_responses` and `request_state`.
impl HeapLayout for CallToolRequestParams {
    const LAYOUT: &'static JsonLayout = layout!(record![
        "_meta" => of!(Option<RequestMetaObject>),
        "name" => of!(Cow<'static, str>),
        "arguments" => of!(Option<JsonObject>),
        "inputResponses" => of!(Option<InputResponses>),
        "requestState" => of!(Option<String>),
    ]);
}
