<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Inference\Customer\V1;

/**
 * Admits and inspects immutable evaluation results.
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
     * Admits an immutable evaluation specification.
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
     * Reads an immutable evaluation view.
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
