//! Admit descriptor actions through the same vocabulary as runtime SDK clients.
use acyclic_auth_action::Action;
use prost_reflect::{DescriptorPool, DynamicMessage, Value};
use std::{collections::BTreeSet, io};

fn invalid(context: &str, action: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("{context}: undeclared capability action {action:?}"),
    )
}

fn actions(message: &DynamicMessage, field: &str, context: &str) -> Result<(), io::Error> {
    if let Some(values) = message.get_field_by_name(field) {
        for value in values.as_list().into_iter().flatten() {
            let spelling = value
                .as_str()
                .ok_or_else(|| invalid(context, "<non-string>"))?;
            spelling
                .parse::<Action>()
                .map_err(|_| invalid(context, spelling))?;
        }
    }
    Ok(())
}

fn rules(value: &Value, context: &str) -> Result<(), io::Error> {
    for rule in value.as_list().into_iter().flatten() {
        let message = rule
            .as_message()
            .ok_or_else(|| invalid(context, "<non-message>"))?;
        actions(message, "action", context)?;
    }
    Ok(())
}

pub(crate) fn validate(bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    let pool = DescriptorPool::decode(bytes)?;
    validate_pool(&pool)
}

pub(crate) fn validate_pool(pool: &DescriptorPool) -> Result<(), Box<dyn std::error::Error>> {
    let Some(auth) = pool.get_extension_by_name("acyclic.validation.v1.auth") else {
        return Ok(());
    };
    for service in pool.services() {
        for method in service.methods() {
            rules(&method.options().get_extension(&auth), method.full_name())?;
        }
    }
    let products = pool.get_extension_by_name("acyclic.validation.v1.products");
    let extractors = pool.get_extension_by_name("acyclic.validation.v1.extractors");
    let routes = pool.get_extension_by_name("acyclic.validation.v1.http_routes");
    let mut declared = BTreeSet::new();
    for file in pool.files() {
        let options = file.options();
        if let Some(extension) = &products {
            let value = options.get_extension(extension);
            for product in value.as_list().into_iter().flatten() {
                if let Some(product) = product.as_message() {
                    actions(product, "actions", file.name())?;
                    if let Some(values) = product.get_field_by_name("actions") {
                        for value in values.as_list().into_iter().flatten() {
                            if let Some(value) = value.as_str() {
                                declared.insert(value.to_owned());
                            }
                        }
                    }
                }
            }
        }
        if let Some(extension) = &extractors {
            let value = options.get_extension(extension);
            for extractor in value.as_list().into_iter().flatten() {
                if let Some(extractor) = extractor.as_message() {
                    actions(extractor, "possible_actions", file.name())?;
                }
            }
        }
        if let Some(extension) = &routes {
            let value = options.get_extension(extension);
            for route in value.as_list().into_iter().flatten() {
                if let Some(route) = route.as_message()
                    && let Some(auth) = route.get_field_by_name("auth")
                {
                    rules(&auth, file.name())?;
                }
            }
        }
    }
    let expected = Action::ALL
        .iter()
        .map(|action| action.as_str().to_owned())
        .collect();
    if declared != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "descriptor capability catalog differs from the typed SDK action vocabulary",
        )
        .into());
    }
    Ok(())
}
