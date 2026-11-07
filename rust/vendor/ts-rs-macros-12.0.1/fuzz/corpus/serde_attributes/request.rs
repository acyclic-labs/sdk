#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", deny_unknown_fields)]
enum Request {
    Reserve {
        request_id: Bytes,
        units: u64,
        owner: OwnerEvidence,
        deadline_unix_ms: u64,
    },
    Release {
        grant: Grant,
    },
}
