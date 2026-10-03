<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Inference\Customer\V1;

/**
 * Remote operations for the Inference customer v1 contract.
 */
class ModelsServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * ModelsService.List operation.
     * @param \Inference\Customer\V1\ListModelsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Inference\Customer\V1\ListModelsResponse>
     */
    public function List(\Inference\Customer\V1\ListModelsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/inference.customer.v1.ModelsService/List',
        $argument,
        ['\Inference\Customer\V1\ListModelsResponse', 'decode'],
        $metadata, $options);
    }

}
