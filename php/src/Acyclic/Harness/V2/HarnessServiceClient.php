<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Acyclic\Harness\V2;

/**
 */
class HarnessServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * @param \Acyclic\Protocol\V1\HandshakeRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Protocol\V1\HandshakeResponse>
     */
    public function Handshake(\Acyclic\Protocol\V1\HandshakeRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.harness.v2.HarnessService/Handshake',
        $argument,
        ['\Acyclic\Protocol\V1\HandshakeResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Harness\V2\CommandEnvelope $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Harness\V2\Admission>
     */
    public function Submit(\Acyclic\Harness\V2\CommandEnvelope $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.harness.v2.HarnessService/Submit',
        $argument,
        ['\Acyclic\Harness\V2\Admission', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Harness\V2\ResumeRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function Replay(\Acyclic\Harness\V2\ResumeRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/acyclic.harness.v2.HarnessService/Replay',
        $argument,
        ['\Acyclic\Harness\V2\Delivery', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Harness\V2\ObserveRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Harness\V2\OperationStatus>
     */
    public function Observe(\Acyclic\Harness\V2\ObserveRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.harness.v2.HarnessService/Observe',
        $argument,
        ['\Acyclic\Harness\V2\OperationStatus', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Harness\V2\CancelRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Harness\V2\CancelResponse>
     */
    public function Cancel(\Acyclic\Harness\V2\CancelRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.harness.v2.HarnessService/Cancel',
        $argument,
        ['\Acyclic\Harness\V2\CancelResponse', 'decode'],
        $metadata, $options);
    }

}
