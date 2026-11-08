#[derive(Clone, Debug, Serialize)]
#[serde(transparent)]
pub struct WireItem(Record, #[serde(skip)] [Option<GeminiTurn>; 2]);
