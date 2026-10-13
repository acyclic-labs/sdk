use super::*;

/// Static contracts are decoded by the original guarded SDK constructor, not assembled as unobserved JSON values and adopted after allocation.
pub(crate) fn builtin_tool_definition_with_native_work(
    work: &NativeMaterializationWork,
) -> Result<(acyclic_harness::tool::ToolDefinition, NativeAllocation)> {
    acyclic_harness::tool::ToolDefinition::new(
        PI_BUILTIN_COMMAND_KIND,
        PI_BUILTIN_TOOL_REVISION,
        "Pi builtin IO through the original task's declared SOURCE, PROJECT or SCRATCH",
        r#"{
          "type":"object","additionalProperties":false,
          "required":["caller","sdk_operation","path","io"],
          "properties":{
            "caller":{"type":"string","minLength":1,"maxLength":128},
            "sdk_operation":{"type":"string","minLength":1,"maxLength":512},
            "path":{"type":"string","minLength":1,"maxLength":4096},
            "io":{"oneOf":[
              {"type":"object","additionalProperties":false,"required":["kind","maximum_bytes"],
               "properties":{"kind":{"const":"read"},
                 "maximum_bytes":{"type":"integer","minimum":1,"maximum":262144},
                 "variants":{"type":"array","maxItems":4,
                   "items":{"type":"string","minLength":1,"maxLength":4096}}}},
              {"type":"object","additionalProperties":false,"required":["kind","content"],
               "properties":{"kind":{"const":"write"},"content":{"type":"string","maxLength":262144}}},
              {"type":"object","additionalProperties":false,"required":["kind","maximum_bytes"],
               "properties":{"kind":{"const":"edit_read"},
                 "maximum_bytes":{"type":"integer","minimum":1,"maximum":262144}}},
              {"type":"object","additionalProperties":false,"required":["kind","content","expected_sha256"],
               "properties":{"kind":{"const":"edit_write"},"content":{"type":"string","maxLength":262144},
                 "expected_sha256":{"type":"string","pattern":"^[0-9a-fA-F]{64}$"}}}
            ]}
          }
        }"#,
        r#"{
          "oneOf":[
            {"type":"object","additionalProperties":false,"required":["content_base64","sha256","path","volume_role"],
             "properties":{"content_base64":{"type":"string","maxLength":349528},
               "sha256":{"type":"string","pattern":"^[0-9a-f]{64}$"},
               "path":{"type":"string","minLength":1,"maxLength":4096},
               "volume_role":{"enum":["SOURCE","PROJECT","SCRATCH"]}}},
            {"type":"object","additionalProperties":false,"required":["written","path","volume_role"],
             "properties":{"written":{"const":true},"path":{"type":"string","minLength":1,"maxLength":4096},
               "volume_role":{"enum":["PROJECT","SCRATCH"]}}}
          ]
        }"#,
        r#"{
          "type":"object","additionalProperties":false,"required":["kind","value"],
          "properties":{"kind":{"const":"json"},"value":{"oneOf":[
            {"type":"object","additionalProperties":false,"required":["content_base64","sha256","path","volume_role"],
             "properties":{"content_base64":{"type":"string","maxLength":349528},
               "sha256":{"type":"string","pattern":"^[0-9a-f]{64}$"},
               "path":{"type":"string","minLength":1,"maxLength":4096},
               "volume_role":{"enum":["SOURCE","PROJECT","SCRATCH"]}}},
            {"type":"object","additionalProperties":false,"required":["written","path","volume_role"],
             "properties":{"written":{"const":true},"path":{"type":"string","minLength":1,"maxLength":4096},
               "volume_role":{"enum":["PROJECT","SCRATCH"]}}}
          ]}}
        }"#,
        16_384,
        work,
    )
}
