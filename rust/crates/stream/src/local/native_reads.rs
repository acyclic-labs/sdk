impl LocalStream {
    async fn native_visible<T>(
        &self,
        read: impl Future<Output = crate::NativeStreamResult<T>>,
    ) -> crate::NativeStreamResult<T> {
        if let Err(error) = self.check_available() {
            return Ok(Err(error));
        }
        let (result, required) = {
            let _visibility = self.inner.visibility.read().await;
            if let Err(error) = self.check_available() {
                return Ok(Err(error));
            }
            let required = match self.inner.required() {
                Ok(required) => required,
                Err(error) => return Ok(Err(error)),
            };
            (read.await, required)
        };
        if let Err(error) = self.await_durable(required).await {
            return Ok(Err(error));
        }
        result
    }
}
