#[derive(Clone)]
struct HistoryWindow {
    records: Arc<RecordBatch>,
    start: usize,
    end: usize,
}
