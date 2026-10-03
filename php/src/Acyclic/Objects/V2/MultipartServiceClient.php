<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Acyclic\Objects\V2;

/**
 * Stages and publishes multipart object uploads.
 */
class MultipartServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Starts a multipart upload.
     * @param \Acyclic\Objects\V2\CreateMultipartRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Objects\V2\MultipartUpload>
     */
    public function CreateMultipart(\Acyclic\Objects\V2\CreateMultipartRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.objects.v2.MultipartService/CreateMultipart',
        $argument,
        ['\Acyclic\Objects\V2\MultipartUpload', 'decode'],
        $metadata, $options);
    }

    /**
     * Uploads one multipart part after an explicit completion frame.
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ClientStreamingCall
     */
    public function UploadPart($metadata = [], $options = []) {
        return $this->_clientStreamRequest('/acyclic.objects.v2.MultipartService/UploadPart',
        ['\Acyclic\Objects\V2\UploadedPart','decode'],
        $metadata, $options);
    }

    /**
     * Lists the uploaded parts for a multipart upload.
     * @param \Acyclic\Objects\V2\ListPartsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Objects\V2\ListPartsResponse>
     */
    public function ListParts(\Acyclic\Objects\V2\ListPartsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.objects.v2.MultipartService/ListParts',
        $argument,
        ['\Acyclic\Objects\V2\ListPartsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Publishes a multipart object from exact ordered part receipts.
     * @param \Acyclic\Objects\V2\CompleteMultipartRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Objects\V2\ObjectInfo>
     */
    public function CompleteMultipart(\Acyclic\Objects\V2\CompleteMultipartRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.objects.v2.MultipartService/CompleteMultipart',
        $argument,
        ['\Acyclic\Objects\V2\ObjectInfo', 'decode'],
        $metadata, $options);
    }

    /**
     * Aborts a multipart upload.
     * @param \Acyclic\Objects\V2\AbortMultipartRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Objects\V2\AbortMultipartResponse>
     */
    public function AbortMultipart(\Acyclic\Objects\V2\AbortMultipartRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.objects.v2.MultipartService/AbortMultipart',
        $argument,
        ['\Acyclic\Objects\V2\AbortMultipartResponse', 'decode'],
        $metadata, $options);
    }

}
