#[derive(Clone)]
struct HistoryWindow {
    records: Arc<RecordBatch>,
    start: usize,
    end: usize,
}

impl Iterator for HistoryWindow {
    type Item = Record;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start == self.end {
            return None;
        }
        let record = self.records.record(self.start)?;
        self.start += 1;
        Some(record)
    }
}
