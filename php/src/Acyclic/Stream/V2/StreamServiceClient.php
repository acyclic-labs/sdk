<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Acyclic\Stream\V2;

/**
 * Remote operations for appending, reading, and committing streams.
 */
class StreamServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * Returns the admitted outcome for an idempotency key, when present.
     * @param \Acyclic\Stream\V2\InspectIdempotencyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Stream\V2\InspectIdempotencyResponse>
     */
    public function InspectIdempotency(\Acyclic\Stream\V2\InspectIdempotencyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.stream.v2.StreamService/InspectIdempotency',
        $argument,
        ['\Acyclic\Stream\V2\InspectIdempotencyResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Appends records to a stream with an optional expected tail.
     * @param \Acyclic\Stream\V2\AppendRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Stream\V2\AppendResponse>
     */
    public function Append(\Acyclic\Stream\V2\AppendRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.stream.v2.StreamService/Append',
        $argument,
        ['\Acyclic\Stream\V2\AppendResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Returns the current tail sequence for a stream.
     * @param \Acyclic\Stream\V2\TailRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Stream\V2\TailResponse>
     */
    public function Tail(\Acyclic\Stream\V2\TailRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.stream.v2.StreamService/Tail',
        $argument,
        ['\Acyclic\Stream\V2\TailResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Forks a stream path at an admitted sequence.
     * @param \Acyclic\Stream\V2\ForkRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Stream\V2\ForkReceipt>
     */
    public function Fork(\Acyclic\Stream\V2\ForkRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.stream.v2.StreamService/Fork',
        $argument,
        ['\Acyclic\Stream\V2\ForkReceipt', 'decode'],
        $metadata, $options);
    }

    /**
     * Reads records from a stream page by page.
     * @param \Acyclic\Stream\V2\ReadRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function Read(\Acyclic\Stream\V2\ReadRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/acyclic.stream.v2.StreamService/Read',
        $argument,
        ['\Acyclic\Stream\V2\ReadResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Follows a stream and yields records as they become available.
     * @param \Acyclic\Stream\V2\FollowRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function Follow(\Acyclic\Stream\V2\FollowRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/acyclic.stream.v2.StreamService/Follow',
        $argument,
        ['\Acyclic\Stream\V2\ReadResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Lists child stream paths under a parent path.
     * @param \Acyclic\Stream\V2\ChildrenRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function Children(\Acyclic\Stream\V2\ChildrenRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/acyclic.stream.v2.StreamService/Children',
        $argument,
        ['\Acyclic\Stream\V2\ChildrenResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Returns one ordered page of child stream paths.
     * @param \Acyclic\Stream\V2\ChildrenPageRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Stream\V2\ChildrenPageResponse>
     */
    public function ChildrenPage(\Acyclic\Stream\V2\ChildrenPageRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.stream.v2.StreamService/ChildrenPage',
        $argument,
        ['\Acyclic\Stream\V2\ChildrenPageResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Atomically commits stream mutations after checking conditions.
     * @param \Acyclic\Stream\V2\CommitRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Stream\V2\CommitResponse>
     */
    public function Commit(\Acyclic\Stream\V2\CommitRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.stream.v2.StreamService/Commit',
        $argument,
        ['\Acyclic\Stream\V2\CommitResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * Reads a committed envelope by commit identifier.
     * @param \Acyclic\Stream\V2\ReadCommitRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Stream\V2\CommittedEnvelope>
     */
    public function ReadCommit(\Acyclic\Stream\V2\ReadCommitRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.stream.v2.StreamService/ReadCommit',
        $argument,
        ['\Acyclic\Stream\V2\CommittedEnvelope', 'decode'],
        $metadata, $options);
    }

}
