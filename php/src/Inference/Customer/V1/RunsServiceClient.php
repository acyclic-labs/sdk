<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Inference\Customer\V1;

/**
 * Remote operations for the Inference customer v1 contract.
 */
class RunsServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * RunsService.Generate operation.
     * @param \Inference\Customer\V1\GenerateRunRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Inference\Customer\V1\GenerateRunResponse>
     */
    public function Generate(\Inference\Customer\V1\GenerateRunRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/inference.customer.v1.RunsService/Generate',
        $argument,
        ['\Inference\Customer\V1\GenerateRunResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * RunsService.Inspect operation.
     * @param \Inference\Customer\V1\InspectRunRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Inference\Customer\V1\RunView>
     */
    public function Inspect(\Inference\Customer\V1\InspectRunRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/inference.customer.v1.RunsService/Inspect',
        $argument,
        ['\Inference\Customer\V1\RunView', 'decode'],
        $metadata, $options);
    }

    /**
     * RunsService.Watch operation.
     * @param \Inference\Customer\V1\WatchRunRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function Watch(\Inference\Customer\V1\WatchRunRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/inference.customer.v1.RunsService/Watch',
        $argument,
        ['\Inference\Customer\V1\RunEvent', 'decode'],
        $metadata, $options);
    }

    /**
     * RunsService.Cancel operation.
     * @param \Inference\Customer\V1\InspectRunRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Inference\Customer\V1\RunView>
     */
    public function Cancel(\Inference\Customer\V1\InspectRunRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/inference.customer.v1.RunsService/Cancel',
        $argument,
        ['\Inference\Customer\V1\RunView', 'decode'],
        $metadata, $options);
    }

}
