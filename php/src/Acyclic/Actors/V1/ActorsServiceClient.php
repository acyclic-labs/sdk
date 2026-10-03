<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Acyclic\Actors\V1;

/**
 * Remote operations for creating, observing, and invoking actors.
 */
class ActorsServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Creates an actor.
     * @param \Acyclic\Actors\V1\CreateActorRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Actors\V1\CreateActorResponse>
     */
    public function CreateActor(\Acyclic\Actors\V1\CreateActorRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.actors.v1.ActorsService/CreateActor',
        $argument,
        ['\Acyclic\Actors\V1\CreateActorResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Replaces actor configuration with compare-and-swap semantics.
     * @param \Acyclic\Actors\V1\UpdateActorRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Actors\V1\UpdateActorResponse>
     */
    public function UpdateActor(\Acyclic\Actors\V1\UpdateActorRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.actors.v1.ActorsService/UpdateActor',
        $argument,
        ['\Acyclic\Actors\V1\UpdateActorResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Returns the current actor observation.
     * @param \Acyclic\Actors\V1\InspectActorRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Actors\V1\InspectActorResponse>
     */
    public function InspectActor(\Acyclic\Actors\V1\InspectActorRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.actors.v1.ActorsService/InspectActor',
        $argument,
        ['\Acyclic\Actors\V1\InspectActorResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Adds a subscription to an actor.
     * @param \Acyclic\Actors\V1\AddSubscriptionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Actors\V1\AddSubscriptionResponse>
     */
    public function AddSubscription(\Acyclic\Actors\V1\AddSubscriptionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.actors.v1.ActorsService/AddSubscription',
        $argument,
        ['\Acyclic\Actors\V1\AddSubscriptionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Removes a subscription from an actor.
     * @param \Acyclic\Actors\V1\RemoveSubscriptionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Actors\V1\RemoveSubscriptionResponse>
     */
    public function RemoveSubscription(\Acyclic\Actors\V1\RemoveSubscriptionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.actors.v1.ActorsService/RemoveSubscription',
        $argument,
        ['\Acyclic\Actors\V1\RemoveSubscriptionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Resumes a paused subscription.
     * @param \Acyclic\Actors\V1\ResumeSubscriptionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Actors\V1\ResumeSubscriptionResponse>
     */
    public function ResumeSubscription(\Acyclic\Actors\V1\ResumeSubscriptionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.actors.v1.ActorsService/ResumeSubscription',
        $argument,
        ['\Acyclic\Actors\V1\ResumeSubscriptionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Requests an actor checkpoint.
     * @param \Acyclic\Actors\V1\CheckpointActorRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Actors\V1\CheckpointActorResponse>
     */
    public function CheckpointActor(\Acyclic\Actors\V1\CheckpointActorRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.actors.v1.ActorsService/CheckpointActor',
        $argument,
        ['\Acyclic\Actors\V1\CheckpointActorResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Invokes an actor method.
     * @param \Acyclic\Actors\V1\InvokeActorRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Actors\V1\InvokeActorResponse>
     */
    public function InvokeActor(\Acyclic\Actors\V1\InvokeActorRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.actors.v1.ActorsService/InvokeActor',
        $argument,
        ['\Acyclic\Actors\V1\InvokeActorResponse', 'decode'],
        $metadata, $options);
    }

}
