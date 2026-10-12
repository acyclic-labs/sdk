use tonic::codegen::*;

unary_server!(BucketsServiceServer, BucketsService, "acyclic.objects.v1.BucketsService";
    (create_bucket, CreateBucketRequest, super::CreateBucketRequest, Bucket, "CreateBucket", unary, UnaryService),
    (head_bucket, HeadBucketRequest, super::HeadBucketRequest, Bucket, "HeadBucket", unary, UnaryService),
    (delete_bucket, DeleteBucketRequest, super::DeleteBucketRequest, DeleteBucketResponse, "DeleteBucket", unary, UnaryService),
);
