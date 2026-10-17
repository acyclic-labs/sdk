use acyclic_stream::{Record, RecordStream, StreamError};

pub(super) fn records(records: Vec<Record>, fault: u8) -> RecordStream {
    struct Misreported {
        records: std::vec::IntoIter<Record>,
        upper: Option<usize>,
    }
    impl Iterator for Misreported {
        type Item = Result<Record, StreamError>;
        fn next(&mut self) -> Option<Self::Item> {
            self.records.next().map(Ok)
        }
        fn size_hint(&self) -> (usize, Option<usize>) {
            (0, self.upper)
        }
    }
    Box::pin(futures::stream::iter(Misreported {
        records: records.into_iter(),
        upper: match fault {
            7 => Some(0),
            8 => None,
            _ => Some(usize::MAX),
        },
    }))
}
