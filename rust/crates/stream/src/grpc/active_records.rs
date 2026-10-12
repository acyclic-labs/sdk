struct ActiveRecords {
    records: obs::Scoped<tonic::Streaming<wire::ReadResponse>>,
    endpoint: usize,
    completion: Completion,
}
