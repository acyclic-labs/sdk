/// Keeps successful response headers pending until the unary response is decoded.
struct RpcResponse<T> {
    body: T,
    completion: Completion,
}

impl<T> RpcResponse<T> {
    fn decode<U>(
        mut self,
        decode: impl FnOnce(T) -> Result<U, StreamError>,
    ) -> Result<U, StreamError> {
        // Tonic unary completion already observed the successful trailers.
        obs::record(&self.completion.span, "rpc.code", Code::Ok as u64);
        let result = decode(self.body);
        match &result {
            Ok(_) => self.completion.ok(),
            Err(_) => self.completion.invalid_response(),
        }
        result
    }
}
