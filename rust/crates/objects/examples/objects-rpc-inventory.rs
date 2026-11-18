//! Emit compiler-linked Objects methods and canonical HTTP routes.

#[path = "../src/route_inventory.rs"]
mod route_inventory;

macro_rules! routes {
    (
        unary { $($method:ident => ($route:literal, $input:ident, $output:ident)),+ $(,)? }
        streaming { $($stream:ident => ($stream_route:literal, $stream_in:ident, $stream_out:ident)),+ $(,)? }
    ) => {
        const ROUTES: &[(&str, &str, &str)] = &[
            $(($route, stringify!($input), stringify!($output)),)+
            $(($stream_route, stringify!($stream_in), stringify!($stream_out)),)+
        ];
    };
}
route_inventory::objects_http_routes!(routes);

fn methods(source: &str, owner: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut found = None;
    for item in syn::parse_file(source)?.items {
        let syn::Item::Impl(implementation) = item else {
            continue;
        };
        let syn::Type::Path(path) = implementation.self_ty.as_ref() else {
            continue;
        };
        if !path.path.is_ident(owner)
            || !implementation.attrs.iter().any(|attribute| {
                attribute
                    .path()
                    .segments
                    .last()
                    .is_some_and(|segment| segment.ident == "implementation_methods")
            })
        {
            continue;
        }
        if found.is_some()
            || acyclic_implementation_inventory::conditions(&implementation.attrs)
                .next()
                .is_some()
        {
            return Err(
                format!("{owner} inventory has ambiguous or conditional implementations").into(),
            );
        }
        let mut names = Vec::new();
        for method in acyclic_implementation_inventory::methods(&implementation) {
            if acyclic_implementation_inventory::conditions(&method.attrs)
                .next()
                .is_some()
            {
                return Err(format!("{owner} inventory has a conditional method").into());
            }
            names.push(method.sig.ident.to_string());
        }
        found = Some(names);
    }
    found.ok_or_else(|| format!("missing annotated implementation for {owner}").into())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let inventory = serde_json::json!({
        "grpc": methods(include_str!("../src/grpc.rs"), "GrpcObjects")?,
        "http": methods(include_str!("../src/http.rs"), "HttpObjects")?,
        "routes": ROUTES,
    });
    serde_json::to_writer(std::io::stdout(), &inventory)?;
    Ok(())
}
