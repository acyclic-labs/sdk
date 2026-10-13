struct FollowState {
    stream: Mutex<Option<crate::RecordStream>>,
    cancel: tokio::sync::watch::Sender<bool>,
}
