//! Canonical HTTP route declarations for both the runtime and its generator.

macro_rules! objects_http_routes {
    ($apply:ident) => {
        $apply! {
            unary {
                PutBlob => ("blobs/put", PutBlobRequest, PutResponse),
                PutBlobs => ("blobs/put-batch", PutBlobsRequest, PutBlobsResponse),
                PutManifest => ("blobs/put-manifest", PutManifestRequest, PutResponse),
                GetBlob => ("blobs/get", GetBlobRequest, GetBlobResponse),
                HeadBlob => ("blobs/head", HeadBlobRequest, BlobInfo),
                DeleteBlob => ("blobs/delete", DeleteBlobRequest, DeleteBlobResponse),
                ListBlobs => ("blobs/list", ListBlobsRequest, ListBlobsResponse),
                Retain => ("blobs/retain", RetainRequest, RetainResponse),
                Release => ("blobs/release", ReleaseRequest, ReleaseResponse),
                RetainFenced => ("blobs/retain-fenced", RetainFencedRequest, RetainResponse),
                ReleaseTerminal => ("blobs/release-terminal", ReleaseTerminalRequest, ReleaseResponse),
                Collect => ("blobs/collect", CollectRequest, CollectResponse),
                Usage => ("blobs/usage", UsageRequest, UsageResponse),
            }
            streaming {
                Availability => ("blobs/availability", AvailabilityRequest, AvailabilityCursor),
            }
        }
    };
}

pub(crate) use objects_http_routes;
