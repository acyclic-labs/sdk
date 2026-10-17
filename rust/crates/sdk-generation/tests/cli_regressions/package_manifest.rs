// Overlay source-owned manifest values on the one actual Cargo normalization.
// Use TOML values throughout, preserving nested metadata and feature tables.
pub(super) fn normalize(template: &[u8], original: &[u8]) -> Option<Vec<u8>> {
    let mut normalized: toml::Value = toml::from_str(std::str::from_utf8(template).ok()?).ok()?;
    let original: toml::Value = toml::from_str(std::str::from_utf8(original).ok()?).ok()?;
    let table = normalized.as_table_mut()?;
    for (name, value) in original.as_table()? {
        if matches!(name.as_str(), "package" | "lib") {
            let section = table.get_mut(name)?.as_table_mut()?;
            section.extend(value.as_table()?.clone());
        } else {
            table.insert(name.clone(), value.clone());
        }
    }
    let package = original.get("package")?.get("name")?.as_str()?;
    let name = original
        .get("lib")
        .and_then(|lib| lib.get("name"))
        .and_then(toml::Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| package.replace('-', "_"));
    table
        .get_mut("lib")?
        .as_table_mut()?
        .insert("name".into(), name.into());
    Some(toml::to_string(&normalized).ok()?.into_bytes())
}

#[cfg(test)]
proptest::proptest! {
    #[test]
    fn shared_package_scaffold_preserves_source_manifest_values(
        name in "[a-z]{1,10}(-[a-z]{1,10})?", build in proptest::bool::ANY, no_defaults in proptest::bool::ANY,
    ) {
        crate::runtime::time_control();
        let template = b"[package]\nname='demo'\nversion='1.0.0'\nbuild=false\n[lib]\nname='demo'\npath='src/lib.rs'\n";
        let original = format!("[package]\nname='{name}'\nversion='1.0.0'\n{}\n[features]\ndefault=['base']\nbase=[]\nextra=[]\n[package.metadata.docs.rs]\nno-default-features={no_defaults}\nfeatures=['extra']\n", if build { "build='build.rs'" } else { "" });
        let bytes = normalize(template, original.as_bytes()).unwrap();
        let actual: toml::Value = toml::from_str(std::str::from_utf8(&bytes).unwrap()).unwrap();
        let original: toml::Value = toml::from_str(&original).unwrap();
        for (key, value) in original["package"].as_table().unwrap() {
            proptest::prop_assert_eq!(&actual["package"][key], value);
        }
        proptest::prop_assert_eq!(&actual["features"], &original["features"]);
        let crate_name = name.replace('-', "_");
        proptest::prop_assert_eq!(actual["lib"]["name"].as_str(), Some(crate_name.as_str()));
        proptest::prop_assert_eq!(normalize(&bytes, toml::to_string(&original).unwrap().as_bytes()), Some(bytes));
    }
}
