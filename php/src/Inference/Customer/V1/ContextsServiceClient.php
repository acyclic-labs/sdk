<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Inference\Customer\V1;

/**
 * Remote operations for the Inference customer v1 contract.
 */
class ContextsServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * ContextsService.Create operation.
     * @param \Inference\Customer\V1\CreateContextRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Inference\Customer\V1\MutationReceipt>
     */
    public function Create(\Inference\Customer\V1\CreateContextRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/inference.customer.v1.ContextsService/Create',
        $argument,
        ['\Inference\Customer\V1\MutationReceipt', 'decode'],
        $metadata, $options);
    }

    /**
     * ContextsService.Inspect operation.
     * @param \Inference\Customer\V1\InspectContextRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Inference\Customer\V1\ContextView>
     */
    public function Inspect(\Inference\Customer\V1\InspectContextRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/inference.customer.v1.ContextsService/Inspect',
        $argument,
        ['\Inference\Customer\V1\ContextView', 'decode'],
        $metadata, $options);
    }

    /**
     * ContextsService.Mutate operation.
     * @param \Inference\Customer\V1\MutateContextRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Inference\Customer\V1\MutationReceipt>
     */
    public function Mutate(\Inference\Customer\V1\MutateContextRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/inference.customer.v1.ContextsService/Mutate',
        $argument,
        ['\Inference\Customer\V1\MutationReceipt', 'decode'],
        $metadata, $options);
    }

}
