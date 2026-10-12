use super::*;

use crate::kernel::{FileMetadata, MetadataField};

use crate::{Digest, Fs, WorkspaceName};

use std::sync::Mutex;

#[derive(Debug, Error)]
#[error("test executor failure")]
pub(super) struct TestExecutorError;

pub(super) struct TestExecutor {
    result: Result<GitFilesystemResult, TestExecutorError>,
    operations: Mutex<Vec<OperationId>>,
}

impl TestExecutor {
    fn returning(result: GitFilesystemResult) -> Self {
        Self { result: Ok(result), operations: Mutex::new(Vec::new()) }
    }

    fn failing() -> Self {
        Self { result: Err(TestExecutorError), operations: Mutex::new(Vec::new()) }
    }
}

impl GitFilesystemExecutor for TestExecutor {
    type Error = TestExecutorError;

    async fn execute(
        &self,
        operation_id: OperationId,
        _action: &GitFilesystemAction,
    ) -> Result<GitFilesystemResult, Self::Error> {
        self.operations.lock().expect("operation lock").push(operation_id);
        self.result.as_ref().map(Clone::clone).map_err(|_| TestExecutorError)
    }
}

async fn publish_fixture(
    mut transaction: crate::Transaction<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>,
) -> Generation<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend> {
    let outcome = transaction
        .commit(None, crate::PublicationPermit::Unrestricted, WorkBudget::UNBOUNDED, &CancellationToken::new(), |_| {
            std::future::ready(Ok::<_, crate::OperationFailure<WorkspaceError>>(crate::OperationReceipt {
                value: (),
                work: crate::WorkCounters::UNCHARGED,
            }))
        })
        .await
        .expect("publish fixture")
        .value;
    let TransactionCommit::Committed(generation) = outcome else {
        panic!("fixture did not commit");
    };
    generation
}

pub(super) fn generation(byte: u8) -> GenerationId {
    GenerationId::new(Digest::from_bytes([byte; 32]))
}

pub(super) fn workspace() -> WorkspaceId {
    WorkspaceId::derive(
        [9; 16],
        &WorkspaceName::new("repo", WorkBudget::UNBOUNDED, &CancellationToken::new())
            .expect("valid test workspace")
            .value,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .expect("workspace identity")
    .value
}

pub(super) fn tree(byte: u8) -> GitTreeRef {
    GitTreeRef::exact(workspace(), generation(byte))
}

pub(super) fn declared_fields(declaration: &str, variant: &str) -> BTreeSet<String> {
    declaration
        .split("readonly ")
        .skip(1)
        .filter_map(|field| field.split_once(':').map(|(name, _)| name.trim()))
        .filter(|name| *name != variant)
        .map(|name| name.trim_end_matches('?').to_owned())
        .collect()
}

pub(super) fn direct_declared_fields(declaration: &str) -> BTreeSet<String> {
    let mut fields = BTreeSet::new();
    let mut depth = 0_usize;
    let bytes = declaration.as_bytes();
    let mut index = 0_usize;
    while index < bytes.len() {
        match bytes[index] {
            b'{' => depth += 1,
            b'}' => depth = depth.saturating_sub(1),
            _ => {}
        }
        let previous = bytes[..index].iter().rev().find(|byte| !byte.is_ascii_whitespace()).copied();
        if depth == 1 && bytes[index..].starts_with(b"readonly ") && matches!(previous, None | Some(b'{') | Some(b';'))
        {
            let start = index + b"readonly ".len();
            if let Some(end) = bytes[start..].iter().position(|byte| *byte == b':') {
                let name = String::from_utf8_lossy(&bytes[start..start + end]);
                fields.insert(name.trim().trim_end_matches('?').to_owned());
            }
        }
        index += 1;
    }
    fields
}

pub(super) fn assert_typescript_fields_match_serde(
    label: &str,
    declarations: &[(&str, &str)],
    inventory: &[&str],
    values: impl IntoIterator<Item = (String, serde_json::Value)>,
) {
    let values = values.into_iter().collect::<BTreeMap<_, _>>();
    let declared_names = declarations.iter().map(|(variant, _)| *variant).collect::<BTreeSet<_>>();
    let inventory_names = inventory.iter().copied().collect::<BTreeSet<_>>();
    let fixture_names = values.keys().map(String::as_str).collect::<BTreeSet<_>>();
    assert_eq!(declared_names, inventory_names, "{label} inventory drift");
    assert_eq!(fixture_names, inventory_names, "{label} fixture drift");
    for (variant, declaration) in declarations {
        let Some(value) = values.get(*variant) else {
            panic!("missing {label} fixture for {variant}");
        };
        let Some(payload) = value.get(*variant).and_then(serde_json::Value::as_object) else {
            continue;
        };
        let declared = declared_fields(declaration, variant);
        if declared.is_empty() {
            continue;
        }
        let actual = payload.keys().cloned().collect::<BTreeSet<_>>();
        assert_eq!(declared, actual, "{label} TypeScript schema drift for {variant}");
    }
}

pub(super) fn assert_command_typescript_fields_match_serde(commands: Vec<GitCommand>) {
    let values = commands.into_iter().map(|command| {
        let variant_name = command.variant_name();
        let value = serde_json::to_value(&command).expect("command JSON");
        let variant = match &value {
            serde_json::Value::String(variant) => variant.clone(),
            serde_json::Value::Object(object) => object.keys().next().expect("command tag").clone(),
            _ => panic!("invalid command JSON"),
        };
        assert_eq!(variant, variant_name, "command serde tag drift");
        (variant, value)
    });
    let values = values.collect::<BTreeMap<_, _>>();
    let declared_names =
        GIT_COMPAT_COMMAND_TYPESCRIPT_TYPES.iter().map(|(variant, _)| *variant).collect::<BTreeSet<_>>();
    let inventory_names = GIT_COMPAT_COMMAND_VARIANTS.iter().copied().collect::<BTreeSet<_>>();
    let fixture_names = values.keys().map(String::as_str).collect::<BTreeSet<_>>();
    assert_eq!(declared_names, inventory_names, "command inventory drift");
    assert_eq!(fixture_names, inventory_names, "command fixture drift");
    for (variant, declaration) in GIT_COMPAT_COMMAND_TYPESCRIPT_TYPES {
        let Some(value) = values.get(*variant) else {
            panic!("missing command fixture for {variant}");
        };
        let Some(payload) = value.get(*variant).and_then(serde_json::Value::as_object) else {
            continue;
        };
        let declared = declared_fields(declaration, "kind")
            .into_iter()
            .map(|field| match field.as_str() {
                "authoredAtSeconds" => "authored_at_seconds".to_owned(),
                "dryRun" => "dry_run".to_owned(),
                other => other.to_owned(),
            })
            .collect::<BTreeSet<_>>();
        let actual = payload.keys().cloned().collect::<BTreeSet<_>>();
        assert_eq!(declared, actual, "command TypeScript schema drift for {variant}");
    }
}

pub(super) fn natural_field_name(name: &str) -> &str {
    match name {
        "all_changes_staged" => "allChangesStaged",
        other => other,
    }
}

pub(super) fn declared_field_type<'a>(declaration: &'a str, field: &str) -> Option<&'a str> {
    let marker = format!("readonly {field}");
    let start = declaration.find(&marker)? + marker.len();
    let rest = declaration.get(start..)?;
    let rest = rest.strip_prefix('?').unwrap_or(rest);
    let rest = rest.strip_prefix(':')?.trim_start();
    Some(rest.split(';').next().unwrap_or(rest).trim())
}

pub(super) fn assert_json_value_matches_typescript_hint(declaration: &str, field: &str, value: &serde_json::Value) {
    let Some(type_hint) = declared_field_type(declaration, field) else {
        panic!("TypeScript schema is missing nested field {field}");
    };
    let matches = match value {
        serde_json::Value::Null => type_hint.contains("undefined"),
        serde_json::Value::Bool(_) => type_hint.contains("boolean") || type_hint == "true" || type_hint == "false",
        serde_json::Value::Number(_) => type_hint.contains("number") || type_hint.contains("bigint"),
        serde_json::Value::String(_) => {
            type_hint.contains("string")
                || type_hint.contains("Identity")
                || type_hint.contains("GitDirtyState")
                || type_hint.starts_with('"')
        }
        serde_json::Value::Array(_) => {
            type_hint.contains("[]") || type_hint.contains("Uint8Array") || type_hint.contains("Identity")
        }
        serde_json::Value::Object(_) => {
            type_hint.contains("Git") || type_hint.contains('{') || type_hint.contains("Record")
        }
    };
    assert!(matches, "TypeScript type hint for {field} ({type_hint}) does not match serde value {value}");
}

pub(super) fn assert_nested_typescript_projection(type_name: &str, declaration: &str, value: &serde_json::Value) {
    let Some(object) = value.as_object() else {
        panic!("{type_name} fixture is not an object");
    };
    let declared = direct_declared_fields(declaration);
    let actual = object.keys().map(|key| natural_field_name(key).to_owned()).collect::<BTreeSet<_>>();
    assert_eq!(declared, actual, "nested {type_name} TypeScript schema drift");
    for (key, child) in object {
        let field = natural_field_name(key);
        assert_json_value_matches_typescript_hint(declaration, field, child);
    }
}

mod argv;
mod capture;
mod history;
mod incremental;
mod state;
mod tree;
mod wire;
