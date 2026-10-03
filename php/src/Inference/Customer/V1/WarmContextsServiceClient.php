<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Inference\Customer\V1;

/**
 * Remote operations for the Inference customer v1 contract.
 */
class WarmContextsServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * WarmContextsService.Retain operation.
     * @param \Inference\Customer\V1\RetainWarmRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Inference\Customer\V1\WarmView>
     */
    public function Retain(\Inference\Customer\V1\RetainWarmRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/inference.customer.v1.WarmContextsService/Retain',
        $argument,
        ['\Inference\Customer\V1\WarmView', 'decode'],
        $metadata, $options);
    }

    /**
     * WarmContextsService.Inspect operation.
     * @param \Inference\Customer\V1\InspectWarmRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Inference\Customer\V1\WarmView>
     */
    public function Inspect(\Inference\Customer\V1\InspectWarmRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/inference.customer.v1.WarmContextsService/Inspect',
        $argument,
        ['\Inference\Customer\V1\WarmView', 'decode'],
        $metadata, $options);
    }

    /**
     * WarmContextsService.Renew operation.
     * @param \Inference\Customer\V1\RenewWarmRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Inference\Customer\V1\WarmView>
     */
    public function Renew(\Inference\Customer\V1\RenewWarmRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/inference.customer.v1.WarmContextsService/Renew',
        $argument,
        ['\Inference\Customer\V1\WarmView', 'decode'],
        $metadata, $options);
    }

    /**
     * WarmContextsService.Release operation.
     * @param \Inference\Customer\V1\ReleaseWarmRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Inference\Customer\V1\WarmView>
     */
    public function Release(\Inference\Customer\V1\ReleaseWarmRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/inference.customer.v1.WarmContextsService/Release',
        $argument,
        ['\Inference\Customer\V1\WarmView', 'decode'],
        $metadata, $options);
    }

}
