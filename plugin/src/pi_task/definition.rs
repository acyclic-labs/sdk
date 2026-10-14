use super::*;

impl PiTaskMachine {
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn new(
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<Arc<Self>> {
        let source_bytes = machine_sources()
            .iter()
            .map(|source| source.len() as u64 + 8)
            .sum();
        work.charge(acyclic_fs::WorkCounters {
            items_examined: machine_sources().len() as u64,
            bytes_hashed: source_bytes,
            ..acyclic_fs::WorkCounters::default()
        })?;
        let mut name = work.display("acyclic.pi")?;
        let mut version = work.display("1")?;
        name.1.absorb(&mut version.1)?;
        let mut identity = (
            MachineIdentity {
                name: name.0,
                version: version.0,
                digest: machine_digest(),
            },
            name.1,
        );
        identity.0.validate(&mut Some(
            &mut &acyclic_harness::filesystem::PreparationIo::from_native_work(work)?,
        ))?;
        let mut schema = crate::pi_root_provider::guarded_pi_decode(
            STATE_SCHEMA.as_bytes(),
            NATIVE_METADATA_MAXIMUM,
            std::mem::size_of::<Value>() as u64,
            acyclic_harness::wire::ErrorCode::Invalid,
            acyclic_harness::JsonValueSchema::Root,
            0,
            work,
        )?;
        identity.1.absorb(&mut schema.1)?;
        let alignment = std::mem::align_of::<Self>();
        let header = 2 * std::mem::size_of::<usize>();
        let bytes = (header.div_ceil(alignment) * alignment + std::mem::size_of::<Self>()) as u64;
        let mut publication = work.claim(bytes, 1, acyclic_fs::WorkCounters::UNCHARGED)?;
        identity.1.absorb(&mut publication)?;
        Ok(Arc::new(Self {
            identity: identity.0,
            schema: schema.0,
            // Data fields are declared before their original custody.
            _native_custody: identity.1,
        }))
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn definition(
        self: &Arc<Self>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<TaskDefinition<PiTaskInput, FileRef>> {
        let input_json = work.display(&format_args!(r#"{{
  "type":"object",
  "additionalProperties":false,
  "required":["binding","execution"],
  "properties":{{
    "binding":{{"type":"object"}},
    "execution":{{
      "oneOf":[
        {{
          "type":"object",
          "additionalProperties":false,
          "required":["kind","root","preparation"],
          "properties":{{
            "kind":{{"const":"host"}},
            "root":{{
              "oneOf":[
                {{"type":"object","additionalProperties":false,"required":["kind"],"properties":{{"kind":{{"const":"scratch_only"}}}}}},
                {{
                  "type":"object",
                  "additionalProperties":false,
                  "required":["kind","context_id","root_id","paths"],
                  "properties":{{
                    "kind":{{"const":"project"}},
                    "context_id":{{"type":"array","minItems":16,"maxItems":16,"items":{{"type":"integer","minimum":0,"maximum":255}}}},
                    "root_id":{{"type":"array","minItems":16,"maxItems":16,"items":{{"type":"integer","minimum":0,"maximum":255}}}},
                    "paths":{{
                      "type":"object",
                      "minProperties":1,
                      "maxProperties":{},
                      "propertyNames":{{"pattern":"^[0-9a-f]{{32}}$"}},
                      "additionalProperties":{{"type":"string","minLength":1,"maxLength":{}}}
                    }}
                  }}
                }}
              ]
            }},
            "preparation":{{
              "type":"object",
              "additionalProperties":false,
              "required":["operation_id","kind","payload"],
              "properties":{{"operation_id":{{"type":"string","format":"uuid"}},"kind":{{"enum":["acyclic.effect.v1","acyclic.pi.native.launch.v1"]}},"payload":{{"type":"object"}}}}
            }}
          }}
        }},
        {{
          "type":"object",
          "additionalProperties":false,
          "required":["kind","launch"],
          "properties":{{
            "kind":{{"const":"child"}},
            "launch":{{
              "type":"object",
              "additionalProperties":false,
              "required":["operation_id","kind","payload"],
              "properties":{{"operation_id":{{"type":"string","format":"uuid"}},"kind":{{"enum":["acyclic.effect.v1","acyclic.pi.native.launch.v1"]}},"payload":{{"type":"object"}}}}
            }}
          }}
        }}
      ]
    }}
  }}
}}"#, crate::MAXIMUM_ADAPTER_ROOTS, crate::pi_root::MAXIMUM_ROOT_PATH_BYTES))?;
        let mut input = crate::pi_root_provider::guarded_pi_decode(
            input_json.0.as_bytes(),
            NATIVE_METADATA_MAXIMUM,
            std::mem::size_of::<Value>() as u64,
            acyclic_harness::wire::ErrorCode::Invalid,
            acyclic_harness::JsonValueSchema::Root,
            0,
            work,
        )?;
        drop(input_json.0);
        drop(input_json.1);
        let mut output = crate::pi_root_provider::guarded_pi_decode(
            br#"{"type":"object"}"#,
            NATIVE_METADATA_MAXIMUM,
            std::mem::size_of::<Value>() as u64,
            acyclic_harness::wire::ErrorCode::Invalid,
            acyclic_harness::JsonValueSchema::Root,
            0,
            work,
        )?;
        input.1.absorb(&mut output.1)?;
        TaskDefinition::resumable_with_native_work(self.clone(), input.0, output.0, input.1, work)
    }
}

fn machine_sources() -> [&'static [u8]; 7] {
    [
        include_bytes!("mod.rs").as_slice(),
        include_bytes!("definition.rs").as_slice(),
        include_bytes!("execution.rs").as_slice(),
        include_bytes!("state.rs").as_slice(),
        include_bytes!("codec.rs").as_slice(),
        include_bytes!("../pi_binding.rs").as_slice(),
        include_bytes!("../pi_root.rs").as_slice(),
    ]
}

fn machine_digest() -> [u8; 32] {
    // Pin actual deterministic behavior/binding validation, not process setup.
    let mut digest = blake3::Hasher::new();
    for source in machine_sources() {
        digest.update(&(source.len() as u64).to_le_bytes());
        digest.update(source);
    }
    *digest.finalize().as_bytes()
}

pub(super) const STATE_SCHEMA: &str = r#"{
  "oneOf":[
    {
      "type":"object",
      "additionalProperties":false,
      "required":["state","command"],
      "properties":{
        "state":{"const":"preparing"},
        "command":{
          "type":"object",
          "additionalProperties":false,
          "required":["operation_id","kind","payload"],
          "properties":{"operation_id":{"type":"string","format":"uuid"},"kind":{"enum":["acyclic.effect.v1","acyclic.pi.native.launch.v1"]},"payload":{"type":"object"}}
        }
      }
    },
    {"type":"object","additionalProperties":false,"required":["state","operation"],"properties":{"state":{"const":"allocating"},"operation":{"type":"string","format":"uuid"}}},
    {
      "type":"object",
      "additionalProperties":false,
      "required":["state","allocation"],
      "properties":{
        "state":{"const":"hosting"},
        "allocation":{"type":"object"},
        "reply":{"type":["object","null"],"additionalProperties":false,"required":["operation_id","value"],"properties":{"operation_id":{"type":"string","format":"uuid"},"value":{}}}
      }
    },
    {
      "type":"object",
      "additionalProperties":false,
      "required":["state","allocation","operation"],
      "properties":{"state":{"const":"controlling"},"allocation":{"type":"object"},"operation":{"type":"string","format":"uuid"}}
    },
    {
      "type":"object",
      "additionalProperties":false,
      "required":["state","command"],
      "properties":{
        "state":{"const":"ready"},
        "command":{
          "type":"object",
          "additionalProperties":false,
          "required":["operation_id","kind","payload"],
          "properties":{"operation_id":{"type":"string","format":"uuid"},"kind":{"enum":["acyclic.effect.v1","acyclic.pi.native.launch.v1"]},"payload":{"type":"object"}}
        }
      }
    },
    {"type":"object","additionalProperties":false,"required":["state","operation"],"properties":{"state":{"const":"awaiting"},"operation":{"type":"string","format":"uuid"}}},
    {"type":"object","additionalProperties":false,"required":["state","result"],"properties":{"state":{"const":"completed"},"result":{"type":"object"}}},
    {"type":"object","additionalProperties":false,"required":["state","message"],"properties":{"state":{"const":"failed"},"message":{"type":"string"}}}
  ]
}"#;
