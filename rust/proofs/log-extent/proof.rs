#[path = "../../crates/native-runtime/src/recovery/frame_layout.rs"]
mod production;

use production::LogFrame;

include!("../id-codecs/src/frame_extent.rs");
