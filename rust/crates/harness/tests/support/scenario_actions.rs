use acyclic_harness::{
    Error, Result,
    task_volume::{ChildFile, Component, Control, FileKey, FileValue},
};
pub use super::action_record::Action;

pub fn file(action: &Action) -> Result<FileValue> {
    if action.model.is_empty() || action.request == 0 {
        return Err(Error::Invalid(
            "scenario action requires a model and positive request index".into(),
        ));
    }
    let invalid = |error: acyclic_harness::task_volume::Error| Error::Invalid(error.to_string());
    let reference = Component::new(action.agent.clone()).map_err(invalid)?;
    let (file, bytes) = match (action.control, action.message.as_ref()) {
        (Some(control), None) => (ChildFile::Control, control.bytes().to_vec()),
        (None, Some(message)) if !message.is_empty() => (
            ChildFile::Inbox(
                Component::new(format!("scenario-{}-{}", action.model, action.request))
                    .map_err(invalid)?,
            ),
            message.as_bytes().to_vec(),
        ),
        _ => {
            return Err(Error::Invalid(
                "scenario action needs exactly one control or nonempty message".into(),
            ));
        }
    };
    Ok(FileValue::new(FileKey::Child { reference, file }, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn action_codec_binds_exact_child_path_control_and_message(
            model in "[a-z]{1,12}", agent in "[a-z]{1,12}", request in 1usize..100,
            message in ".{1,64}", selection in 0usize..6,
        ) {
            let controls = [Control::Pause, Control::Resume, Control::Interrupt, Control::Cancel, Control::Archive];
            let action = Action { model, agent, request, control: controls.get(selection).copied(), message: (selection == 5).then_some(message) };
            let encoded = serde_json::to_vec(&action)?;
            let decoded: Action = serde_json::from_slice(&encoded)?;
            prop_assert_eq!(&decoded, &action);
            let value = file(&action).map_err(|error| TestCaseError::fail(error.to_string()))?;
            prop_assert_eq!(FileKey::parse(&value.key.path()).unwrap(), value.key.clone());
            if let Some(control) = action.control {
                prop_assert_eq!(Control::parse(&value.bytes).unwrap(), control);
            } else {
                prop_assert_eq!(value.bytes.as_ref(), action.message.as_ref().unwrap().as_bytes());
            }
            let mut ambiguous = action.clone();
            ambiguous.control = Some(Control::Pause); ambiguous.message = Some("ambiguous".into());
            prop_assert!(file(&ambiguous).is_err());
            let mut escaped = action; escaped.agent = "../outside".into();
            prop_assert!(file(&escaped).is_err());
        }
    }
}
