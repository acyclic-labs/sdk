//! Façade guarantees through the published filesystem and stream APIs.

mod facade {
    mod tests {
        mod concurrency {
            mod batching {
                include!("facade_rounds/batching.rs");
            }
            mod rounds {
                include!("facade_rounds/rounds.rs");
            }
        }
        mod live {
            include!("facade_rounds/live.rs");
        }
        mod special {
            include!("facade_rounds/special.rs");
        }
    }
}
