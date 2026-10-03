<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Inference\Customer\V1;

/**
 * Remote operations for the Inference customer v1 contract.
 */
class EvaluationsServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * EvaluationsService.Create operation.
     * @param \Inference\Customer\V1\CreateEvaluationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Inference\Customer\V1\EvaluationView>
     */
    public function Create(\Inference\Customer\V1\CreateEvaluationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/inference.customer.v1.EvaluationsService/Create',
        $argument,
        ['\Inference\Customer\V1\EvaluationView', 'decode'],
        $metadata, $options);
    }

    /**
     * EvaluationsService.Inspect operation.
     * @param \Inference\Customer\V1\InspectEvaluationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Inference\Customer\V1\EvaluationView>
     */
    public function Inspect(\Inference\Customer\V1\InspectEvaluationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/inference.customer.v1.EvaluationsService/Inspect',
        $argument,
        ['\Inference\Customer\V1\EvaluationView', 'decode'],
        $metadata, $options);
    }

}
