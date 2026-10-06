// This is a generated file - do not edit.
//
// Generated from objects/v1/objects.proto.

// @dart = 3.3

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names
// ignore_for_file: curly_braces_in_flow_control_structures
// ignore_for_file: deprecated_member_use_from_same_package, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_relative_imports

import 'dart:async' as $async;
import 'dart:core' as $core;

import 'package:grpc/service_api.dart' as $grpc;
import 'package:protobuf/protobuf.dart' as $pb;

import 'objects.pb.dart' as $0;

export 'objects.pb.dart';

@$pb.GrpcServiceName('acyclic.objects.v1.BucketsService')
class BucketsServiceClient extends $grpc.Client {
  /// The hostname for this service.
  static const $core.String defaultHost = '';

  /// OAuth scopes needed for the client.
  static const $core.List<$core.String> oauthScopes = [
    '',
  ];

  BucketsServiceClient(super.channel, {super.options, super.interceptors});

  $grpc.ResponseFuture<$0.Bucket> createBucket(
    $0.CreateBucketRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$createBucket, request, options: options);
  }

  $grpc.ResponseFuture<$0.Bucket> headBucket(
    $0.HeadBucketRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$headBucket, request, options: options);
  }

  $grpc.ResponseFuture<$0.DeleteBucketResponse> deleteBucket(
    $0.DeleteBucketRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$deleteBucket, request, options: options);
  }

  // method descriptors

  static final _$createBucket =
      $grpc.ClientMethod<$0.CreateBucketRequest, $0.Bucket>(
          '/acyclic.objects.v1.BucketsService/CreateBucket',
          ($0.CreateBucketRequest value) => value.writeToBuffer(),
          $0.Bucket.fromBuffer);
  static final _$headBucket =
      $grpc.ClientMethod<$0.HeadBucketRequest, $0.Bucket>(
          '/acyclic.objects.v1.BucketsService/HeadBucket',
          ($0.HeadBucketRequest value) => value.writeToBuffer(),
          $0.Bucket.fromBuffer);
  static final _$deleteBucket =
      $grpc.ClientMethod<$0.DeleteBucketRequest, $0.DeleteBucketResponse>(
          '/acyclic.objects.v1.BucketsService/DeleteBucket',
          ($0.DeleteBucketRequest value) => value.writeToBuffer(),
          $0.DeleteBucketResponse.fromBuffer);
}

@$pb.GrpcServiceName('acyclic.objects.v1.BucketsService')
abstract class BucketsServiceBase extends $grpc.Service {
  $core.String get $name => 'acyclic.objects.v1.BucketsService';

  BucketsServiceBase() {
    $addMethod($grpc.ServiceMethod<$0.CreateBucketRequest, $0.Bucket>(
        'CreateBucket',
        createBucket_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.CreateBucketRequest.fromBuffer(value),
        ($0.Bucket value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.HeadBucketRequest, $0.Bucket>(
        'HeadBucket',
        headBucket_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.HeadBucketRequest.fromBuffer(value),
        ($0.Bucket value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.DeleteBucketRequest, $0.DeleteBucketResponse>(
            'DeleteBucket',
            deleteBucket_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.DeleteBucketRequest.fromBuffer(value),
            ($0.DeleteBucketResponse value) => value.writeToBuffer()));
  }

  $async.Future<$0.Bucket> createBucket_Pre($grpc.ServiceCall $call,
      $async.Future<$0.CreateBucketRequest> $request) async {
    return createBucket($call, await $request);
  }

  $async.Future<$0.Bucket> createBucket(
      $grpc.ServiceCall call, $0.CreateBucketRequest request);

  $async.Future<$0.Bucket> headBucket_Pre($grpc.ServiceCall $call,
      $async.Future<$0.HeadBucketRequest> $request) async {
    return headBucket($call, await $request);
  }

  $async.Future<$0.Bucket> headBucket(
      $grpc.ServiceCall call, $0.HeadBucketRequest request);

  $async.Future<$0.DeleteBucketResponse> deleteBucket_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.DeleteBucketRequest> $request) async {
    return deleteBucket($call, await $request);
  }

  $async.Future<$0.DeleteBucketResponse> deleteBucket(
      $grpc.ServiceCall call, $0.DeleteBucketRequest request);
}

@$pb.GrpcServiceName('acyclic.objects.v1.ObjectsService')
class ObjectsServiceClient extends $grpc.Client {
  /// The hostname for this service.
  static const $core.String defaultHost = '';

  /// OAuth scopes needed for the client.
  static const $core.List<$core.String> oauthScopes = [
    '',
  ];

  ObjectsServiceClient(super.channel, {super.options, super.interceptors});

  $grpc.ResponseFuture<$0.ObjectVersion> putObject(
    $async.Stream<$0.PutObjectRequest> request, {
    $grpc.CallOptions? options,
  }) {
    return $createStreamingCall(_$putObject, request, options: options).single;
  }

  $grpc.ResponseStream<$0.GetObjectResponse> getObject(
    $0.GetObjectRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createStreamingCall(
        _$getObject, $async.Stream.fromIterable([request]),
        options: options);
  }

  $grpc.ResponseFuture<$0.HeadObjectResponse> headObject(
    $0.HeadObjectRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$headObject, request, options: options);
  }

  $grpc.ResponseFuture<$0.DeleteObjectResponse> deleteObject(
    $0.DeleteObjectRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$deleteObject, request, options: options);
  }

  $grpc.ResponseFuture<$0.ListObjectsResponse> listObjects(
    $0.ListObjectsRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$listObjects, request, options: options);
  }

  // method descriptors

  static final _$putObject =
      $grpc.ClientMethod<$0.PutObjectRequest, $0.ObjectVersion>(
          '/acyclic.objects.v1.ObjectsService/PutObject',
          ($0.PutObjectRequest value) => value.writeToBuffer(),
          $0.ObjectVersion.fromBuffer);
  static final _$getObject =
      $grpc.ClientMethod<$0.GetObjectRequest, $0.GetObjectResponse>(
          '/acyclic.objects.v1.ObjectsService/GetObject',
          ($0.GetObjectRequest value) => value.writeToBuffer(),
          $0.GetObjectResponse.fromBuffer);
  static final _$headObject =
      $grpc.ClientMethod<$0.HeadObjectRequest, $0.HeadObjectResponse>(
          '/acyclic.objects.v1.ObjectsService/HeadObject',
          ($0.HeadObjectRequest value) => value.writeToBuffer(),
          $0.HeadObjectResponse.fromBuffer);
  static final _$deleteObject =
      $grpc.ClientMethod<$0.DeleteObjectRequest, $0.DeleteObjectResponse>(
          '/acyclic.objects.v1.ObjectsService/DeleteObject',
          ($0.DeleteObjectRequest value) => value.writeToBuffer(),
          $0.DeleteObjectResponse.fromBuffer);
  static final _$listObjects =
      $grpc.ClientMethod<$0.ListObjectsRequest, $0.ListObjectsResponse>(
          '/acyclic.objects.v1.ObjectsService/ListObjects',
          ($0.ListObjectsRequest value) => value.writeToBuffer(),
          $0.ListObjectsResponse.fromBuffer);
}

@$pb.GrpcServiceName('acyclic.objects.v1.ObjectsService')
abstract class ObjectsServiceBase extends $grpc.Service {
  $core.String get $name => 'acyclic.objects.v1.ObjectsService';

  ObjectsServiceBase() {
    $addMethod($grpc.ServiceMethod<$0.PutObjectRequest, $0.ObjectVersion>(
        'PutObject',
        putObject,
        true,
        false,
        ($core.List<$core.int> value) => $0.PutObjectRequest.fromBuffer(value),
        ($0.ObjectVersion value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.GetObjectRequest, $0.GetObjectResponse>(
        'GetObject',
        getObject_Pre,
        false,
        true,
        ($core.List<$core.int> value) => $0.GetObjectRequest.fromBuffer(value),
        ($0.GetObjectResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.HeadObjectRequest, $0.HeadObjectResponse>(
        'HeadObject',
        headObject_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.HeadObjectRequest.fromBuffer(value),
        ($0.HeadObjectResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.DeleteObjectRequest, $0.DeleteObjectResponse>(
            'DeleteObject',
            deleteObject_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.DeleteObjectRequest.fromBuffer(value),
            ($0.DeleteObjectResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.ListObjectsRequest, $0.ListObjectsResponse>(
            'ListObjects',
            listObjects_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.ListObjectsRequest.fromBuffer(value),
            ($0.ListObjectsResponse value) => value.writeToBuffer()));
  }

  $async.Future<$0.ObjectVersion> putObject(
      $grpc.ServiceCall call, $async.Stream<$0.PutObjectRequest> request);

  $async.Stream<$0.GetObjectResponse> getObject_Pre($grpc.ServiceCall $call,
      $async.Future<$0.GetObjectRequest> $request) async* {
    yield* getObject($call, await $request);
  }

  $async.Stream<$0.GetObjectResponse> getObject(
      $grpc.ServiceCall call, $0.GetObjectRequest request);

  $async.Future<$0.HeadObjectResponse> headObject_Pre($grpc.ServiceCall $call,
      $async.Future<$0.HeadObjectRequest> $request) async {
    return headObject($call, await $request);
  }

  $async.Future<$0.HeadObjectResponse> headObject(
      $grpc.ServiceCall call, $0.HeadObjectRequest request);

  $async.Future<$0.DeleteObjectResponse> deleteObject_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.DeleteObjectRequest> $request) async {
    return deleteObject($call, await $request);
  }

  $async.Future<$0.DeleteObjectResponse> deleteObject(
      $grpc.ServiceCall call, $0.DeleteObjectRequest request);

  $async.Future<$0.ListObjectsResponse> listObjects_Pre($grpc.ServiceCall $call,
      $async.Future<$0.ListObjectsRequest> $request) async {
    return listObjects($call, await $request);
  }

  $async.Future<$0.ListObjectsResponse> listObjects(
      $grpc.ServiceCall call, $0.ListObjectsRequest request);
}

@$pb.GrpcServiceName('acyclic.objects.v1.MultipartService')
class MultipartServiceClient extends $grpc.Client {
  /// The hostname for this service.
  static const $core.String defaultHost = '';

  /// OAuth scopes needed for the client.
  static const $core.List<$core.String> oauthScopes = [
    '',
  ];

  MultipartServiceClient(super.channel, {super.options, super.interceptors});

  $grpc.ResponseFuture<$0.MultipartUpload> createMultipart(
    $0.CreateMultipartRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$createMultipart, request, options: options);
  }

  $grpc.ResponseFuture<$0.UploadedPart> uploadPart(
    $async.Stream<$0.UploadPartRequest> request, {
    $grpc.CallOptions? options,
  }) {
    return $createStreamingCall(_$uploadPart, request, options: options).single;
  }

  $grpc.ResponseFuture<$0.ListPartsResponse> listParts(
    $0.ListPartsRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$listParts, request, options: options);
  }

  $grpc.ResponseFuture<$0.ObjectVersion> completeMultipart(
    $0.CompleteMultipartRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$completeMultipart, request, options: options);
  }

  $grpc.ResponseFuture<$0.AbortMultipartResponse> abortMultipart(
    $0.AbortMultipartRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$abortMultipart, request, options: options);
  }

  // method descriptors

  static final _$createMultipart =
      $grpc.ClientMethod<$0.CreateMultipartRequest, $0.MultipartUpload>(
          '/acyclic.objects.v1.MultipartService/CreateMultipart',
          ($0.CreateMultipartRequest value) => value.writeToBuffer(),
          $0.MultipartUpload.fromBuffer);
  static final _$uploadPart =
      $grpc.ClientMethod<$0.UploadPartRequest, $0.UploadedPart>(
          '/acyclic.objects.v1.MultipartService/UploadPart',
          ($0.UploadPartRequest value) => value.writeToBuffer(),
          $0.UploadedPart.fromBuffer);
  static final _$listParts =
      $grpc.ClientMethod<$0.ListPartsRequest, $0.ListPartsResponse>(
          '/acyclic.objects.v1.MultipartService/ListParts',
          ($0.ListPartsRequest value) => value.writeToBuffer(),
          $0.ListPartsResponse.fromBuffer);
  static final _$completeMultipart =
      $grpc.ClientMethod<$0.CompleteMultipartRequest, $0.ObjectVersion>(
          '/acyclic.objects.v1.MultipartService/CompleteMultipart',
          ($0.CompleteMultipartRequest value) => value.writeToBuffer(),
          $0.ObjectVersion.fromBuffer);
  static final _$abortMultipart =
      $grpc.ClientMethod<$0.AbortMultipartRequest, $0.AbortMultipartResponse>(
          '/acyclic.objects.v1.MultipartService/AbortMultipart',
          ($0.AbortMultipartRequest value) => value.writeToBuffer(),
          $0.AbortMultipartResponse.fromBuffer);
}

@$pb.GrpcServiceName('acyclic.objects.v1.MultipartService')
abstract class MultipartServiceBase extends $grpc.Service {
  $core.String get $name => 'acyclic.objects.v1.MultipartService';

  MultipartServiceBase() {
    $addMethod(
        $grpc.ServiceMethod<$0.CreateMultipartRequest, $0.MultipartUpload>(
            'CreateMultipart',
            createMultipart_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.CreateMultipartRequest.fromBuffer(value),
            ($0.MultipartUpload value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.UploadPartRequest, $0.UploadedPart>(
        'UploadPart',
        uploadPart,
        true,
        false,
        ($core.List<$core.int> value) => $0.UploadPartRequest.fromBuffer(value),
        ($0.UploadedPart value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.ListPartsRequest, $0.ListPartsResponse>(
        'ListParts',
        listParts_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.ListPartsRequest.fromBuffer(value),
        ($0.ListPartsResponse value) => value.writeToBuffer()));
    $addMethod(
        $grpc.ServiceMethod<$0.CompleteMultipartRequest, $0.ObjectVersion>(
            'CompleteMultipart',
            completeMultipart_Pre,
            false,
            false,
            ($core.List<$core.int> value) =>
                $0.CompleteMultipartRequest.fromBuffer(value),
            ($0.ObjectVersion value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.AbortMultipartRequest,
            $0.AbortMultipartResponse>(
        'AbortMultipart',
        abortMultipart_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.AbortMultipartRequest.fromBuffer(value),
        ($0.AbortMultipartResponse value) => value.writeToBuffer()));
  }

  $async.Future<$0.MultipartUpload> createMultipart_Pre($grpc.ServiceCall $call,
      $async.Future<$0.CreateMultipartRequest> $request) async {
    return createMultipart($call, await $request);
  }

  $async.Future<$0.MultipartUpload> createMultipart(
      $grpc.ServiceCall call, $0.CreateMultipartRequest request);

  $async.Future<$0.UploadedPart> uploadPart(
      $grpc.ServiceCall call, $async.Stream<$0.UploadPartRequest> request);

  $async.Future<$0.ListPartsResponse> listParts_Pre($grpc.ServiceCall $call,
      $async.Future<$0.ListPartsRequest> $request) async {
    return listParts($call, await $request);
  }

  $async.Future<$0.ListPartsResponse> listParts(
      $grpc.ServiceCall call, $0.ListPartsRequest request);

  $async.Future<$0.ObjectVersion> completeMultipart_Pre($grpc.ServiceCall $call,
      $async.Future<$0.CompleteMultipartRequest> $request) async {
    return completeMultipart($call, await $request);
  }

  $async.Future<$0.ObjectVersion> completeMultipart(
      $grpc.ServiceCall call, $0.CompleteMultipartRequest request);

  $async.Future<$0.AbortMultipartResponse> abortMultipart_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.AbortMultipartRequest> $request) async {
    return abortMultipart($call, await $request);
  }

  $async.Future<$0.AbortMultipartResponse> abortMultipart(
      $grpc.ServiceCall call, $0.AbortMultipartRequest request);
}

@$pb.GrpcServiceName('acyclic.objects.v1.SnapshotsService')
class SnapshotsServiceClient extends $grpc.Client {
  /// The hostname for this service.
  static const $core.String defaultHost = '';

  /// OAuth scopes needed for the client.
  static const $core.List<$core.String> oauthScopes = [
    '',
  ];

  SnapshotsServiceClient(super.channel, {super.options, super.interceptors});

  $grpc.ResponseFuture<$0.Snapshot> createSnapshot(
    $0.CreateSnapshotRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$createSnapshot, request, options: options);
  }

  $grpc.ResponseFuture<$0.DestroySnapshotResponse> destroySnapshot(
    $0.DestroySnapshotRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$destroySnapshot, request, options: options);
  }

  $grpc.ResponseFuture<$0.Bucket> forkSnapshot(
    $0.ForkSnapshotRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$forkSnapshot, request, options: options);
  }

  $grpc.ResponseFuture<$0.Bucket> forkBucket(
    $0.ForkBucketRequest request, {
    $grpc.CallOptions? options,
  }) {
    return $createUnaryCall(_$forkBucket, request, options: options);
  }

  // method descriptors

  static final _$createSnapshot =
      $grpc.ClientMethod<$0.CreateSnapshotRequest, $0.Snapshot>(
          '/acyclic.objects.v1.SnapshotsService/CreateSnapshot',
          ($0.CreateSnapshotRequest value) => value.writeToBuffer(),
          $0.Snapshot.fromBuffer);
  static final _$destroySnapshot =
      $grpc.ClientMethod<$0.DestroySnapshotRequest, $0.DestroySnapshotResponse>(
          '/acyclic.objects.v1.SnapshotsService/DestroySnapshot',
          ($0.DestroySnapshotRequest value) => value.writeToBuffer(),
          $0.DestroySnapshotResponse.fromBuffer);
  static final _$forkSnapshot =
      $grpc.ClientMethod<$0.ForkSnapshotRequest, $0.Bucket>(
          '/acyclic.objects.v1.SnapshotsService/ForkSnapshot',
          ($0.ForkSnapshotRequest value) => value.writeToBuffer(),
          $0.Bucket.fromBuffer);
  static final _$forkBucket =
      $grpc.ClientMethod<$0.ForkBucketRequest, $0.Bucket>(
          '/acyclic.objects.v1.SnapshotsService/ForkBucket',
          ($0.ForkBucketRequest value) => value.writeToBuffer(),
          $0.Bucket.fromBuffer);
}

@$pb.GrpcServiceName('acyclic.objects.v1.SnapshotsService')
abstract class SnapshotsServiceBase extends $grpc.Service {
  $core.String get $name => 'acyclic.objects.v1.SnapshotsService';

  SnapshotsServiceBase() {
    $addMethod($grpc.ServiceMethod<$0.CreateSnapshotRequest, $0.Snapshot>(
        'CreateSnapshot',
        createSnapshot_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.CreateSnapshotRequest.fromBuffer(value),
        ($0.Snapshot value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.DestroySnapshotRequest,
            $0.DestroySnapshotResponse>(
        'DestroySnapshot',
        destroySnapshot_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.DestroySnapshotRequest.fromBuffer(value),
        ($0.DestroySnapshotResponse value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.ForkSnapshotRequest, $0.Bucket>(
        'ForkSnapshot',
        forkSnapshot_Pre,
        false,
        false,
        ($core.List<$core.int> value) =>
            $0.ForkSnapshotRequest.fromBuffer(value),
        ($0.Bucket value) => value.writeToBuffer()));
    $addMethod($grpc.ServiceMethod<$0.ForkBucketRequest, $0.Bucket>(
        'ForkBucket',
        forkBucket_Pre,
        false,
        false,
        ($core.List<$core.int> value) => $0.ForkBucketRequest.fromBuffer(value),
        ($0.Bucket value) => value.writeToBuffer()));
  }

  $async.Future<$0.Snapshot> createSnapshot_Pre($grpc.ServiceCall $call,
      $async.Future<$0.CreateSnapshotRequest> $request) async {
    return createSnapshot($call, await $request);
  }

  $async.Future<$0.Snapshot> createSnapshot(
      $grpc.ServiceCall call, $0.CreateSnapshotRequest request);

  $async.Future<$0.DestroySnapshotResponse> destroySnapshot_Pre(
      $grpc.ServiceCall $call,
      $async.Future<$0.DestroySnapshotRequest> $request) async {
    return destroySnapshot($call, await $request);
  }

  $async.Future<$0.DestroySnapshotResponse> destroySnapshot(
      $grpc.ServiceCall call, $0.DestroySnapshotRequest request);

  $async.Future<$0.Bucket> forkSnapshot_Pre($grpc.ServiceCall $call,
      $async.Future<$0.ForkSnapshotRequest> $request) async {
    return forkSnapshot($call, await $request);
  }

  $async.Future<$0.Bucket> forkSnapshot(
      $grpc.ServiceCall call, $0.ForkSnapshotRequest request);

  $async.Future<$0.Bucket> forkBucket_Pre($grpc.ServiceCall $call,
      $async.Future<$0.ForkBucketRequest> $request) async {
    return forkBucket($call, await $request);
  }

  $async.Future<$0.Bucket> forkBucket(
      $grpc.ServiceCall call, $0.ForkBucketRequest request);
}
