//! Generate runtime and TypeScript method admission from the same descriptors.
use acyclic_auth_action::Action;
use prost_reflect::{DescriptorPool, DynamicMessage};
use std::{collections::BTreeMap, fmt::Write as _, io};

type Error = Box<dyn std::error::Error>;

fn typescript(rows: &[serde_json::Value], method_type: &str) -> Result<String, Error> {
    Ok(format!(
        "// Generated from the canonical service authorization options.\nexport const AUTHORIZATION = {} as const;\nexport type {method_type} = typeof AUTHORIZATION[number][\"method\"];\n",
        serde_json::to_string_pretty(rows)?
    ))
}

/// Project the actual runtime client table, including Session method aliases,
/// into the packaged TypeScript table. No second client catalog is maintained.
///
/// # Errors
/// Returns an output encoding error.
pub fn typescript_authorization<M: std::fmt::Debug + 'static>(
    rows: &[acyclic_auth_action::method::MethodAuth<M>],
    method_type: &str,
) -> Result<String, Error> {
    use acyclic_auth_action::method::{Resources, Scope};
    let rows = rows.iter().map(|row| {
        let scope = match row.scope {
            Scope::Exact => "exact",
            Scope::Follow => "follow",
            Scope::Children => "children",
            Scope::ResourceFollow => "resourceFollow",
        };
        let rules = row.rules.iter().map(|rule| {
            let resources = match rule.resources {
                Resources::Template(template) => serde_json::json!({"template":template}),
                Resources::Extractor {name,possible_actions} => serde_json::json!({"extractor":name,"possibleActions":possible_actions.iter().map(|action| action.as_str()).collect::<Vec<_>>()}),
            };
            serde_json::json!({"admitted":rule.admitted.iter().map(|action| action.as_str()).collect::<Vec<_>>(),"resources":resources})
        }).collect::<Vec<_>>();
        serde_json::json!({"method":format!("{:?}",row.method),"route":row.route,"admitted":row.admitted.iter().map(|action| action.as_str()).collect::<Vec<_>>(),"scope":scope,"rules":rules})
    }).collect::<Vec<_>>();
    typescript(&rows, method_type)
}

/// Write the Rust projection from one compiled service descriptor. The packaged
/// TypeScript projection reads that compiled table through [`typescript_authorization`].
/// The caller supplies an existing output directory; no protocol is recompiled.
///
/// # Errors
/// Returns descriptor admission and output write errors.
pub fn write_authorization(
    bytes: &[u8],
    service: &str,
    method_type: &str,
    output: &std::path::Path,
) -> Result<(), Error> {
    let (rust, _) = authorization(bytes, service, method_type)?;
    std::fs::write(output.join("authorization.rs"), rust)?;
    Ok(())
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn string(message: &DynamicMessage, field: &str) -> String {
    message
        .get_field_by_name(field)
        .and_then(|value| value.as_str().map(str::to_owned))
        .unwrap_or_default()
}

fn actions(message: &DynamicMessage, field: &str) -> Result<Vec<Action>, Error> {
    message
        .get_field_by_name(field)
        .and_then(|value| value.as_list().cloned())
        .unwrap_or_default()
        .iter()
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| invalid("non-string authorization action"))?
                .parse::<Action>()
                .map_err(Into::into)
        })
        .collect()
}

fn rust_action(action: Action) -> String {
    use acyclic_auth_action::Action;
    let (product, kind, variant) = match action {
        Action::Actors(value) => ("Actors", "ActorsAction", format!("{value:?}")),
        Action::Billing(value) => ("Billing", "BillingAction", format!("{value:?}")),
        Action::Execution(value) => ("Execution", "ExecutionAction", format!("{value:?}")),
        Action::Fs(value) => ("Fs", "FsAction", format!("{value:?}")),
        Action::Infer(value) => ("Infer", "InferAction", format!("{value:?}")),
        Action::Machines(value) => ("Machines", "MachinesAction", format!("{value:?}")),
        Action::Objects(value) => ("Objects", "ObjectsAction", format!("{value:?}")),
        Action::Secrets(value) => ("Secrets", "SecretsAction", format!("{value:?}")),
        Action::Streams(value) => ("Streams", "StreamsAction", format!("{value:?}")),
        Action::Workers(value) => ("Workers", "WorkersAction", format!("{value:?}")),
    };
    format!("auth_action::Action::{product}(auth_action::{kind}::{variant})")
}

fn rust_actions(actions: &[Action]) -> String {
    format!(
        "&[{}]",
        actions
            .iter()
            .copied()
            .map(rust_action)
            .collect::<Vec<_>>()
            .join(",")
    )
}

struct Extractor {
    resources_only: bool,
    possible: Vec<Action>,
}

/// Generate a service's exhaustive method enum and admission table, plus its
/// TypeScript projection. Dynamic extractor vocabularies remain output ceilings;
/// they are never promoted to unconditional required permissions.
///
/// # Errors
/// Refuses undeclared actions, unknown extractors, missing admission rules and
/// rules that disagree with their registered static or dynamic extractor.
pub fn authorization(
    bytes: &[u8],
    service: &str,
    method_type: &str,
) -> Result<(String, String), Error> {
    product_authorization(bytes, service, method_type, None)
}

/// Generate the declared subset owned by a product on a shared service.
/// Selection uses the actual admitted or possible actions, never method names.
///
/// # Errors
/// Returns the same descriptor and declaration errors as [`authorization`].
pub fn product_authorization(
    bytes: &[u8],
    service: &str,
    method_type: &str,
    product: Option<&str>,
) -> Result<(String, String), Error> {
    if method_type.is_empty()
        || !method_type
            .bytes()
            .enumerate()
            .all(|(index, byte)| byte.is_ascii_alphabetic() || (index > 0 && byte.is_ascii_digit()))
    {
        return Err(invalid("method type must be an ASCII Rust identifier").into());
    }
    let pool = DescriptorPool::decode(bytes)?;
    crate::authorization::validate_pool(&pool)?;
    let extension = pool
        .get_extension_by_name("acyclic.validation.v1.auth")
        .ok_or_else(|| invalid("method authorization extension is missing"))?;
    let registry = pool
        .get_extension_by_name("acyclic.validation.v1.extractors")
        .ok_or_else(|| invalid("authorization extractor registry is missing"))?;
    let mut extractors = BTreeMap::new();
    for file in pool.files() {
        let options = file.options();
        let value = options.get_extension(&registry);
        for value in value.as_list().into_iter().flatten() {
            let declaration = value
                .as_message()
                .ok_or_else(|| invalid("invalid extractor"))?;
            let name = string(declaration, "name");
            let resources_only = declaration
                .get_field_by_name("resources_only")
                .and_then(|value| value.as_bool())
                .unwrap_or_default();
            let extractor = Extractor {
                resources_only,
                possible: actions(declaration, "possible_actions")?,
            };
            if name.is_empty()
                || extractor.possible.is_empty()
                || extractors.insert(name, extractor).is_some()
            {
                return Err(invalid("empty or duplicate authorization extractor").into());
            }
        }
    }
    let service = pool
        .get_service_by_name(service)
        .ok_or_else(|| invalid("authorization service is missing"))?;
    let mut variants = Vec::new();
    let mut rows = Vec::new();
    let mut javascript = Vec::new();
    for method in service.methods() {
        let name = method.name();
        let route = format!("/{}/{}", service.full_name(), name);
        let options = method.options();
        let value = options.get_extension(&extension);
        let mut required = Vec::new();
        let mut possible = Vec::new();
        let mut rules = Vec::new();
        let mut js_rules = Vec::new();
        for value in value.as_list().into_iter().flatten() {
            let declaration = value
                .as_message()
                .ok_or_else(|| invalid("invalid authorization rule"))?;
            let admitted = actions(declaration, "action")?;
            let resource = string(declaration, "resource");
            let extractor = string(declaration, "extractor");
            let (resources, js_resources) = match (resource.is_empty(), extractor.is_empty()) {
                (false, true) if !admitted.is_empty() => (
                    format!("auth_action::method::Resources::Template({resource:?})"),
                    serde_json::json!({"template": resource}),
                ),
                (true, false) => {
                    let registered = extractors.get(&extractor).ok_or_else(|| {
                        invalid(format!("unknown authorization extractor {extractor}"))
                    })?;
                    possible.extend_from_slice(&registered.possible);
                    if registered.resources_only == admitted.is_empty()
                        || admitted
                            .iter()
                            .any(|action| !registered.possible.contains(action))
                    {
                        return Err(invalid(format!(
                            "{} disagrees with extractor {extractor}",
                            method.full_name()
                        ))
                        .into());
                    }
                    (
                        format!(
                            "auth_action::method::Resources::Extractor {{name:{extractor:?},possible_actions:{}}}",
                            rust_actions(&registered.possible)
                        ),
                        serde_json::json!({"extractor": extractor,"possibleActions":registered.possible.iter().map(|action| action.as_str()).collect::<Vec<_>>()}),
                    )
                }
                _ => {
                    return Err(invalid(format!(
                        "{} has no unique resource selector",
                        method.full_name()
                    ))
                    .into());
                }
            };
            for action in &admitted {
                if !required.contains(action) {
                    required.push(*action);
                }
            }
            possible.extend_from_slice(&admitted);
            rules.push(format!(
                "auth_action::method::Rule {{admitted:{},resources:{resources}}}",
                rust_actions(&admitted)
            ));
            js_rules.push(serde_json::json!({"admitted":admitted.iter().map(|action| action.as_str()).collect::<Vec<_>>(),"resources":js_resources}));
        }
        if rules.is_empty() {
            return Err(invalid(format!("{} has no admission rule", method.full_name())).into());
        }
        if product.is_some_and(|product| !possible.iter().any(|action| action.product() == product))
        {
            continue;
        }
        variants.push(name.to_owned());
        rows.push(format!("auth_action::method::MethodAuth {{method:{method_type}::{name},route:{route:?},admitted:{},scope: auth_action::method::Scope::Exact,rules:&[{}]}}", rust_actions(&required), rules.join(",")));
        javascript.push(serde_json::json!({"method":name,"route":route,"admitted":required.iter().map(|action| action.as_str()).collect::<Vec<_>>(),"scope":"exact","rules":js_rules}));
    }
    let mut rust = String::from("// Generated from the canonical service authorization options.\n");
    writeln!(
        rust,
        "#[doc=\"Exhaustive compiler-linked public service methods.\"] #[derive(Clone,Copy,Debug,PartialEq,Eq)] pub enum {method_type} {{ {} }}",
        variants
            .iter()
            .map(|name| format!("#[doc={name:?}] {name}"))
            .collect::<Vec<_>>()
            .join(",")
    )?;
    writeln!(
        rust,
        "#[doc=\"Canonical method admission from the service contract.\"] pub const AUTHORIZATION: &[auth_action::method::MethodAuth<{method_type}>] = &[{}];",
        variants
            .iter()
            .map(|name| format!("{method_type}::{name}.authorization()"))
            .collect::<Vec<_>>()
            .join(",")
    )?;
    writeln!(
        rust,
        "impl {method_type} {{ #[doc=\"Every declared public service method.\"] pub const ALL: &'static [Self] = &[{}]; #[doc=\"This method's canonical admission row.\"] pub const fn authorization(self) -> auth_action::method::MethodAuth<Self> {{ match self {{ {} }} }} #[doc=\"Resolve a canonical route without a scan.\"] pub fn from_route(route: &str) -> Option<Self> {{ match route {{ {} _ => None }} }} }}",
        variants
            .iter()
            .map(|name| format!("Self::{name}"))
            .collect::<Vec<_>>()
            .join(","),
        variants
            .iter()
            .zip(&rows)
            .map(|(name, row)| format!("Self::{name} => {row}"))
            .collect::<Vec<_>>()
            .join(","),
        variants
            .iter()
            .map(|name| format!(
                "{route:?} => Some(Self::{name}),",
                route = format!("/{}/{name}", service.full_name())
            ))
            .collect::<String>()
    )?;
    if variants.is_empty() {
        return Err(invalid("authorization selection has no methods").into());
    }
    Ok((rust, typescript(&javascript, method_type)?))
}
