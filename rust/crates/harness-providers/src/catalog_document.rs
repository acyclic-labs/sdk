use crate::ProviderSpec;
use serde::{
    Deserialize, Serialize,
    de::{MapAccess, Visitor},
};
use std::{collections::BTreeMap, fmt};

#[derive(Serialize)]
pub(crate) struct CatalogDocument {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_model: Option<String>,
    #[serde(flatten)]
    pub providers: BTreeMap<String, ProviderSpec>,
}

impl<'de> Deserialize<'de> for CatalogDocument {
    fn deserialize<D: serde::Deserializer<'de>>(decoder: D) -> Result<Self, D::Error> {
        struct CatalogVisitor;
        impl<'de> Visitor<'de> for CatalogVisitor {
            type Value = CatalogDocument;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a provider catalog with an optional default_model")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut document = CatalogDocument {
                    default_model: None,
                    providers: BTreeMap::new(),
                };
                while let Some(key) = map.next_key::<String>()? {
                    if key == "default_model" {
                        if document.default_model.is_some() {
                            return Err(serde::de::Error::duplicate_field("default_model"));
                        }
                        document.default_model = Some(map.next_value()?);
                    } else {
                        if document.providers.contains_key(&key) {
                            return Err(serde::de::Error::custom("duplicate provider"));
                        }
                        document.providers.insert(key, map.next_value()?);
                    }
                }
                Ok(document)
            }
        }
        decoder.deserialize_map(CatalogVisitor)
    }
}

pub(crate) fn parse(bytes: &[u8]) -> acyclic_harness::Result<CatalogDocument> {
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let document = serde_path_to_error::deserialize(&mut decoder)
        .map_err(|error| crate::invalid(format!("provider catalog: {error}")))?;
    decoder
        .end()
        .map_err(|error| crate::invalid(format!("provider catalog: {error}")))?;
    Ok(document)
}
