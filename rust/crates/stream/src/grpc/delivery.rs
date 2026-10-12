use super::*;

const DELIVERY_ACK_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);
const DELIVERY_ACK_QUEUE: usize = 16;

impl Client {
    /// Disables automatic transport receipts for translating clients.
    #[must_use]
    pub fn without_delivery_acknowledgements(mut self) -> Self {
        self.acknowledge_deliveries = false;
        self
    }

    /// Reports a hosted transport receipt to its issuing endpoint (in configured endpoint order).
    pub async fn acknowledge_delivery(
        &self,
        endpoint: usize,
        delivery_token: impl AsRef<[u8]>,
    ) -> Result<(), StreamError> {
        let delivery_token = delivery_token.as_ref();
        if delivery_token.len() != 32 || endpoint >= self.channels.len() {
            return Err(StreamError::InvalidArgument);
        }
        let request = self.request(wire::AcknowledgeDeliveryRequest {
            delivery_token: Bytes::copy_from_slice(delivery_token),
        });
        tokio::time::timeout(
            DELIVERY_ACK_TIMEOUT,
            Self::service(&self.channels, endpoint).acknowledge_delivery(request),
        )
        .await
        .map_err(|_| StreamError::Unavailable)?
        .map_err(|error| {
            if error.code() == Code::Unimplemented {
                StreamError::Unsupported
            } else {
                status(&error)
            }
        })?;
        Ok(())
    }
}

/// One bounded worker belongs to the logical reader, including reconnects.
pub(super) struct DeliveryAcknowledgements {
    pub(super) queue: tokio::sync::mpsc::Sender<(usize, Bytes)>,
    worker: tokio::task::JoinHandle<()>,
}

impl DeliveryAcknowledgements {
    pub(super) fn new(client: Client) -> Self {
        let (queue, mut pending) = tokio::sync::mpsc::channel::<(usize, Bytes)>(DELIVERY_ACK_QUEUE);
        let worker = tokio::spawn(async move {
            while let Some((endpoint, token)) = pending.recv().await {
                let _ = client.acknowledge_delivery(endpoint, token).await;
            }
        });
        Self { queue, worker }
    }
}

impl Drop for DeliveryAcknowledgements {
    fn drop(&mut self) {
        self.worker.abort();
    }
}
