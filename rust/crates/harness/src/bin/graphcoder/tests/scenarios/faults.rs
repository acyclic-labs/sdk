//! HTTP faults are replies, not model turns: a retry must consume another HTTP
//! attempt before it can consume a successful scripted turn.
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Fault {
    Status {
        code: u16,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retry_after: Option<u32>,
    },
    Reset {
        prefix: Vec<String>,
    },
    Malformed {
        body: String,
    },
}

impl Fault {
    pub fn validate(&self) -> Result<(), &'static str> {
        match self {
            Self::Status { code, .. } if !(400..=599).contains(code) => {
                Err("fault status must be an HTTP error (400..=599)")
            }
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn fault_codec_preserves_every_reply_byte(
            code in 400u16..600, retry_after in prop::option::of(any::<u32>()),
            text in ".{0,64}", prefix in prop::collection::vec(".{0,16}", 0..4),
        ) {
            for fault in [Fault::Status {code, retry_after}, Fault::Reset {prefix}, Fault::Malformed {body:text}] {
                prop_assert!(fault.validate().is_ok());
                let bytes = serde_json::to_vec(&fault)?;
                let decoded: Fault = serde_json::from_slice(&bytes)?;
                prop_assert_eq!(&decoded, &fault);
                let mut value = serde_json::to_value(&fault)?;
                value["unrecognized"] = true.into();
                prop_assert!(serde_json::from_value::<Fault>(value).is_err());
            }
        }

        #[test]
        fn status_faults_cannot_silently_become_successes(code in any::<u16>()) {
            let fault = Fault::Status {code, retry_after:None};
            prop_assert_eq!(fault.validate().is_ok(), (400..=599).contains(&code));
        }
    }

    #[test]
    fn fault_tags_and_fields_are_unambiguous() {
        for input in [
            r#"{"type":"status","code":429,"body":"hidden"}"#,
            r#"{"type":"reset","prefix":[],"code":500}"#,
            r#"{"type":"malformed","body":"x","retry_after":0}"#,
            r#"{"type":"unknown"}"#,
        ] {
            assert!(serde_json::from_str::<Fault>(input).is_err(), "{input}");
        }
    }
}
