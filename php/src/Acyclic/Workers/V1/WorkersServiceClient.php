<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Acyclic\Workers\V1;

/**
 * Publishes immutable Worker versions, selects deployments, and executes jobs.
 */
class WorkersServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Publishes an immutable JavaScript module version.
     * @param \Acyclic\Workers\V1\PublishVersionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Workers\V1\PublishVersionResponse>
     */
    public function PublishVersion(\Acyclic\Workers\V1\PublishVersionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.workers.v1.WorkersService/PublishVersion',
        $argument,
        ['\Acyclic\Workers\V1\PublishVersionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Selects a version for a deployment alias with revision compare-and-swap.
     * @param \Acyclic\Workers\V1\SelectDeploymentRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Workers\V1\SelectDeploymentResponse>
     */
    public function SelectDeployment(\Acyclic\Workers\V1\SelectDeploymentRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.workers.v1.WorkersService/SelectDeployment',
        $argument,
        ['\Acyclic\Workers\V1\SelectDeploymentResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Accepts durable input and returns the accepted job.
     * @param \Acyclic\Workers\V1\SubmitJobRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Workers\V1\SubmitJobResponse>
     */
    public function SubmitJob(\Acyclic\Workers\V1\SubmitJobRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.workers.v1.WorkersService/SubmitJob',
        $argument,
        ['\Acyclic\Workers\V1\SubmitJobResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Returns the current durable job observation.
     * @param \Acyclic\Workers\V1\InspectJobRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Workers\V1\InspectJobResponse>
     */
    public function InspectJob(\Acyclic\Workers\V1\InspectJobRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.workers.v1.WorkersService/InspectJob',
        $argument,
        ['\Acyclic\Workers\V1\InspectJobResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Requests cancellation of a durable job.
     * @param \Acyclic\Workers\V1\CancelJobRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Workers\V1\CancelJobResponse>
     */
    public function CancelJob(\Acyclic\Workers\V1\CancelJobRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.workers.v1.WorkersService/CancelJob',
        $argument,
        ['\Acyclic\Workers\V1\CancelJobResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Invokes an immutable Worker version as ordinary HTTP work.
     * @param \Acyclic\Workers\V1\InvokeVersionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Workers\V1\InvokeResponse>
     */
    public function InvokeVersion(\Acyclic\Workers\V1\InvokeVersionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.workers.v1.WorkersService/InvokeVersion',
        $argument,
        ['\Acyclic\Workers\V1\InvokeResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Invokes the version selected by a deployment alias.
     * @param \Acyclic\Workers\V1\InvokeDeploymentRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Workers\V1\InvokeResponse>
     */
    public function InvokeDeployment(\Acyclic\Workers\V1\InvokeDeploymentRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.workers.v1.WorkersService/InvokeDeployment',
        $argument,
        ['\Acyclic\Workers\V1\InvokeResponse', 'decode'],
        $metadata, $options);
    }

}
