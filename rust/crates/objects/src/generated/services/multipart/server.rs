use tonic::codegen::*;

unary_server!(MultipartServiceServer, MultipartService, "acyclic.objects.v1.MultipartService";
    (create_multipart, CreateMultipartRequest, super::CreateMultipartRequest, MultipartUpload, "CreateMultipart", unary, UnaryService),
    (upload_part, UploadPartRequest, tonic::Streaming<super::UploadPartRequest>, UploadedPart, "UploadPart", client_streaming, ClientStreamingService),
    (list_parts, ListPartsRequest, super::ListPartsRequest, ListPartsResponse, "ListParts", unary, UnaryService),
    (complete_multipart, CompleteMultipartRequest, super::CompleteMultipartRequest, ObjectInfo, "CompleteMultipart", unary, UnaryService),
    (abort_multipart, AbortMultipartRequest, super::AbortMultipartRequest, AbortMultipartResponse, "AbortMultipart", unary, UnaryService),
);
