<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Acyclic\Objects\V2;

/**
 * Publishes, reads, lists, and deletes logical objects.
 */
class ObjectsServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Publishes one complete object after an explicit upload completion frame.
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ClientStreamingCall
     */
    public function PutObject($metadata = [], $options = []) {
        return $this->_clientStreamRequest('/acyclic.objects.v2.ObjectsService/PutObject',
        ['\Acyclic\Objects\V2\ObjectInfo','decode'],
        $metadata, $options);
    }

    /**
     * Reads one complete object representation as bounded response frames.
     * @param \Acyclic\Objects\V2\GetObjectRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function GetObject(\Acyclic\Objects\V2\GetObjectRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/acyclic.objects.v2.ObjectsService/GetObject',
        $argument,
        ['\Acyclic\Objects\V2\GetObjectResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Returns object metadata without its body.
     * @param \Acyclic\Objects\V2\HeadObjectRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Objects\V2\HeadObjectResponse>
     */
    public function HeadObject(\Acyclic\Objects\V2\HeadObjectRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.objects.v2.ObjectsService/HeadObject',
        $argument,
        ['\Acyclic\Objects\V2\HeadObjectResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Deletes the current object representation atomically.
     * @param \Acyclic\Objects\V2\DeleteObjectRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Objects\V2\DeleteObjectResponse>
     */
    public function DeleteObject(\Acyclic\Objects\V2\DeleteObjectRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.objects.v2.ObjectsService/DeleteObject',
        $argument,
        ['\Acyclic\Objects\V2\DeleteObjectResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Lists objects through an eventual lexical traversal.
     * @param \Acyclic\Objects\V2\ListObjectsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Objects\V2\ListObjectsResponse>
     */
    public function ListObjects(\Acyclic\Objects\V2\ListObjectsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.objects.v2.ObjectsService/ListObjects',
        $argument,
        ['\Acyclic\Objects\V2\ListObjectsResponse', 'decode'],
        $metadata, $options);
    }

}
