use super::*;
use crate::task_volume::{Change, Query, Snapshot};

#[wasm_bindgen]
impl WasmClientViews {
    /// Applies the ordinary file-query snapshot through the shared Rust fold.
    #[wasm_bindgen(js_name = querySnapshot)]
    pub fn query_snapshot(
        &mut self,
        query: JsValue,
        snapshot: JsValue,
        maximum_files: usize,
        maximum_bytes: usize,
    ) -> Result<(), JsValue> {
        let query: Query = decode(query).map_err(failure)?;
        let snapshot: Snapshot = decode(snapshot).map_err(failure)?;
        self.views
            .query_snapshot(query, snapshot, maximum_files, maximum_bytes)
            .map_err(|error| js_sys::Error::new(&error.to_string()).into())
    }

    /// Applies one accepted publication, retaining its original source cursor.
    #[wasm_bindgen(js_name = queryChange)]
    pub fn query_change(&mut self, change: JsValue) -> Result<bool, JsValue> {
        let change: Change = decode(change).map_err(failure)?;
        self.views
            .query_change(change)
            .map_err(|error| js_sys::Error::new(&error.to_string()).into())
    }

    /// Associates the same two volume-scoped sources as the native client.
    #[wasm_bindgen(js_name = bindGoalSources)]
    pub fn bind_goal_sources(&mut self, goal: JsValue, request: JsValue) -> Result<(), JsValue> {
        self.views
            .bind_goal_sources(
                decode(goal).map_err(failure)?,
                decode(request).map_err(failure)?,
            )
            .map_err(|error| js_sys::Error::new(&error.to_string()).into())
    }

    /// Exports the shared view, preserving full-width cursors as BigInts.
    pub fn views(&self) -> Result<JsValue, JsValue> {
        self.views
            .serialize(
                &serde_wasm_bindgen::Serializer::new()
                    .serialize_maps_as_objects(true)
                    .serialize_missing_as_null(true)
                    .serialize_large_number_types_as_bigints(true),
            )
            .map_err(|error| js_sys::Error::new(&error.to_string()).into())
    }
}
