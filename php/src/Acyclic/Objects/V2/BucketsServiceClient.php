<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Acyclic\Objects\V2;

/**
 * Creates, inspects, and deletes logical object buckets.
 */
class BucketsServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Creates a logical bucket.
     * @param \Acyclic\Objects\V2\CreateBucketRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Objects\V2\Bucket>
     */
    public function CreateBucket(\Acyclic\Objects\V2\CreateBucketRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.objects.v2.BucketsService/CreateBucket',
        $argument,
        ['\Acyclic\Objects\V2\Bucket', 'decode'],
        $metadata, $options);
    }

    /**
     * Returns logical bucket metadata.
     * @param \Acyclic\Objects\V2\HeadBucketRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Objects\V2\Bucket>
     */
    public function HeadBucket(\Acyclic\Objects\V2\HeadBucketRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.objects.v2.BucketsService/HeadBucket',
        $argument,
        ['\Acyclic\Objects\V2\Bucket', 'decode'],
        $metadata, $options);
    }

    /**
     * Deletes a logical bucket when it is empty.
     * @param \Acyclic\Objects\V2\DeleteBucketRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Objects\V2\DeleteBucketResponse>
     */
    public function DeleteBucket(\Acyclic\Objects\V2\DeleteBucketRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.objects.v2.BucketsService/DeleteBucket',
        $argument,
        ['\Acyclic\Objects\V2\DeleteBucketResponse', 'decode'],
        $metadata, $options);
    }

}
