<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Acyclic\Filesystem\V2;

/**
 */
class FilesystemServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * @param \Acyclic\Filesystem\V2\HandshakeRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\HandshakeResponse>
     */
    public function Handshake(\Acyclic\Filesystem\V2\HandshakeRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/Handshake',
        $argument,
        ['\Acyclic\Filesystem\V2\HandshakeResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\CreateWorkspaceRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\WorkspaceResponse>
     */
    public function CreateWorkspace(\Acyclic\Filesystem\V2\CreateWorkspaceRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/CreateWorkspace',
        $argument,
        ['\Acyclic\Filesystem\V2\WorkspaceResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\OpenWorkspaceRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\WorkspaceResponse>
     */
    public function OpenWorkspace(\Acyclic\Filesystem\V2\OpenWorkspaceRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/OpenWorkspace',
        $argument,
        ['\Acyclic\Filesystem\V2\WorkspaceResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\DeleteWorkspaceRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\MutationResponse>
     */
    public function DeleteWorkspace(\Acyclic\Filesystem\V2\DeleteWorkspaceRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/DeleteWorkspace',
        $argument,
        ['\Acyclic\Filesystem\V2\MutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\GetHeadRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\GenerationResponse>
     */
    public function GetHead(\Acyclic\Filesystem\V2\GetHeadRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/GetHead',
        $argument,
        ['\Acyclic\Filesystem\V2\GenerationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\GetGenerationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\GenerationResponse>
     */
    public function GetGeneration(\Acyclic\Filesystem\V2\GetGenerationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/GetGeneration',
        $argument,
        ['\Acyclic\Filesystem\V2\GenerationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\ReadRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\ReadResponse>
     */
    public function Read(\Acyclic\Filesystem\V2\ReadRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/Read',
        $argument,
        ['\Acyclic\Filesystem\V2\ReadResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\StatRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\StatResponse>
     */
    public function Stat(\Acyclic\Filesystem\V2\StatRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/Stat',
        $argument,
        ['\Acyclic\Filesystem\V2\StatResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\ListDirectoryRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\ListDirectoryResponse>
     */
    public function ListDirectory(\Acyclic\Filesystem\V2\ListDirectoryRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/ListDirectory',
        $argument,
        ['\Acyclic\Filesystem\V2\ListDirectoryResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\ReadLinkRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\ReadResponse>
     */
    public function ReadLink(\Acyclic\Filesystem\V2\ReadLinkRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/ReadLink',
        $argument,
        ['\Acyclic\Filesystem\V2\ReadResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\PlanExtentsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\PlanExtentsResponse>
     */
    public function PlanExtents(\Acyclic\Filesystem\V2\PlanExtentsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/PlanExtents',
        $argument,
        ['\Acyclic\Filesystem\V2\PlanExtentsResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\ApplyTransactionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\MutationResponse>
     */
    public function ApplyTransaction(\Acyclic\Filesystem\V2\ApplyTransactionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/ApplyTransaction',
        $argument,
        ['\Acyclic\Filesystem\V2\MutationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\RebaseTransactionRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\RebaseTransactionResponse>
     */
    public function RebaseTransaction(\Acyclic\Filesystem\V2\RebaseTransactionRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/RebaseTransaction',
        $argument,
        ['\Acyclic\Filesystem\V2\RebaseTransactionResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\ForkWorkspaceRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\WorkspaceResponse>
     */
    public function ForkWorkspace(\Acyclic\Filesystem\V2\ForkWorkspaceRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/ForkWorkspace',
        $argument,
        ['\Acyclic\Filesystem\V2\WorkspaceResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\DiffRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\DiffResponse>
     */
    public function Diff(\Acyclic\Filesystem\V2\DiffRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/Diff',
        $argument,
        ['\Acyclic\Filesystem\V2\DiffResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\RebaseRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\RebaseResponse>
     */
    public function Rebase(\Acyclic\Filesystem\V2\RebaseRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/Rebase',
        $argument,
        ['\Acyclic\Filesystem\V2\RebaseResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\PlanJoinRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\JoinPlan>
     */
    public function PlanJoin(\Acyclic\Filesystem\V2\PlanJoinRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/PlanJoin',
        $argument,
        ['\Acyclic\Filesystem\V2\JoinPlan', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\ApplyJoinRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\JoinResponse>
     */
    public function ApplyJoin(\Acyclic\Filesystem\V2\ApplyJoinRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/ApplyJoin',
        $argument,
        ['\Acyclic\Filesystem\V2\JoinResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\RetainGenerationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\RetainGenerationResponse>
     */
    public function Checkpoint(\Acyclic\Filesystem\V2\RetainGenerationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/Checkpoint',
        $argument,
        ['\Acyclic\Filesystem\V2\RetainGenerationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\RetainGenerationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\RetainGenerationResponse>
     */
    public function Pin(\Acyclic\Filesystem\V2\RetainGenerationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/Pin',
        $argument,
        ['\Acyclic\Filesystem\V2\RetainGenerationResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\ExportRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function Export(\Acyclic\Filesystem\V2\ExportRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/acyclic.filesystem.v2.FilesystemService/Export',
        $argument,
        ['\Acyclic\Filesystem\V2\ExportChunk', 'decode'],
        $metadata, $options);
    }

    /**
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ClientStreamingCall
     */
    public function Import($metadata = [], $options = []) {
        return $this->_clientStreamRequest('/acyclic.filesystem.v2.FilesystemService/Import',
        ['\Acyclic\Filesystem\V2\ImportResponse','decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\CredentialRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\CredentialResponse>
     */
    public function IssueMountCredential(\Acyclic\Filesystem\V2\CredentialRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/IssueMountCredential',
        $argument,
        ['\Acyclic\Filesystem\V2\CredentialResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\CredentialRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\CredentialResponse>
     */
    public function IssueS3Credential(\Acyclic\Filesystem\V2\CredentialRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/IssueS3Credential',
        $argument,
        ['\Acyclic\Filesystem\V2\CredentialResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\SourceStateRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\SourceResponse>
     */
    public function GetSourceState(\Acyclic\Filesystem\V2\SourceStateRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/GetSourceState',
        $argument,
        ['\Acyclic\Filesystem\V2\SourceResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\SourceOperationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\SourceResponse>
     */
    public function ReconcileSource(\Acyclic\Filesystem\V2\SourceOperationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/ReconcileSource',
        $argument,
        ['\Acyclic\Filesystem\V2\SourceResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\SourceOperationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\SourceResponse>
     */
    public function RescanSource(\Acyclic\Filesystem\V2\SourceOperationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/RescanSource',
        $argument,
        ['\Acyclic\Filesystem\V2\SourceResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\SourceOperationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\SourceResponse>
     */
    public function SealSource(\Acyclic\Filesystem\V2\SourceOperationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/SealSource',
        $argument,
        ['\Acyclic\Filesystem\V2\SourceResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\ObserveRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\ObserveResponse>
     */
    public function Observe(\Acyclic\Filesystem\V2\ObserveRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/Observe',
        $argument,
        ['\Acyclic\Filesystem\V2\ObserveResponse', 'decode'],
        $metadata, $options);
    }

    /**
     * @param \Acyclic\Filesystem\V2\CancelRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Filesystem\V2\CancelResponse>
     */
    public function Cancel(\Acyclic\Filesystem\V2\CancelRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.filesystem.v2.FilesystemService/Cancel',
        $argument,
        ['\Acyclic\Filesystem\V2\CancelResponse', 'decode'],
        $metadata, $options);
    }

}
