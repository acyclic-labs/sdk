<?php
// GENERATED CODE -- DO NOT EDIT!

namespace Acyclic\Machines\V1;

/**
 * Remote operations for the Machines v1 contract.
 */
class MachinesServiceClient extends \Grpc\BaseStub {

    /**
     * @param string $hostname hostname
     * @param array $opts channel options
     * @param \Grpc\Channel $channel (optional) re-use channel object
     */
    public function __construct($hostname, $opts, $channel = null) {
        parent::__construct($hostname, $opts, $channel);
    }

    /**
     * MachinesService.QualifyImage operation.
     * @param \Acyclic\Machines\V1\QualifyImageRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Machines\V1\ImageQualification>
     */
    public function QualifyImage(\Acyclic\Machines\V1\QualifyImageRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.machines.v1.MachinesService/QualifyImage',
        $argument,
        ['\Acyclic\Machines\V1\ImageQualification', 'decode'],
        $metadata, $options);
    }

    /**
     * MachinesService.Create operation.
     * @param \Acyclic\Machines\V1\CreateMachineRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Machines\V1\MachineAdmission>
     */
    public function Create(\Acyclic\Machines\V1\CreateMachineRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.machines.v1.MachinesService/Create',
        $argument,
        ['\Acyclic\Machines\V1\MachineAdmission', 'decode'],
        $metadata, $options);
    }

    /**
     * MachinesService.Checkpoint operation.
     * @param \Acyclic\Machines\V1\CheckpointMachineRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Machines\V1\CheckpointAdmission>
     */
    public function Checkpoint(\Acyclic\Machines\V1\CheckpointMachineRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.machines.v1.MachinesService/Checkpoint',
        $argument,
        ['\Acyclic\Machines\V1\CheckpointAdmission', 'decode'],
        $metadata, $options);
    }

    /**
     * Forks a checkpoint into fresh machines.
     * @param \Acyclic\Machines\V1\ForkCheckpointRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Machines\V1\ForkAdmission>
     */
    public function Fork(\Acyclic\Machines\V1\ForkCheckpointRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.machines.v1.MachinesService/Fork',
        $argument,
        ['\Acyclic\Machines\V1\ForkAdmission', 'decode'],
        $metadata, $options);
    }

    /**
     * Forks a running machine into fresh children, preserving the declared fidelity.
     * @param \Acyclic\Machines\V1\ForkMachineRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Machines\V1\ForkMachineAdmission>
     */
    public function ForkMachine(\Acyclic\Machines\V1\ForkMachineRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.machines.v1.MachinesService/ForkMachine',
        $argument,
        ['\Acyclic\Machines\V1\ForkMachineAdmission', 'decode'],
        $metadata, $options);
    }

    /**
     * MachinesService.Suspend operation.
     * @param \Acyclic\Machines\V1\MachineMutationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Machines\V1\MutationAdmission>
     */
    public function Suspend(\Acyclic\Machines\V1\MachineMutationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.machines.v1.MachinesService/Suspend',
        $argument,
        ['\Acyclic\Machines\V1\MutationAdmission', 'decode'],
        $metadata, $options);
    }

    /**
     * MachinesService.Wake operation.
     * @param \Acyclic\Machines\V1\MachineMutationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Machines\V1\MutationAdmission>
     */
    public function Wake(\Acyclic\Machines\V1\MachineMutationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.machines.v1.MachinesService/Wake',
        $argument,
        ['\Acyclic\Machines\V1\MutationAdmission', 'decode'],
        $metadata, $options);
    }

    /**
     * MachinesService.SetSuspensionPolicy operation.
     * @param \Acyclic\Machines\V1\SetSuspensionPolicyRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Machines\V1\PolicyAdmission>
     */
    public function SetSuspensionPolicy(\Acyclic\Machines\V1\SetSuspensionPolicyRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.machines.v1.MachinesService/SetSuspensionPolicy',
        $argument,
        ['\Acyclic\Machines\V1\PolicyAdmission', 'decode'],
        $metadata, $options);
    }

    /**
     * MachinesService.DestroyMachine operation.
     * @param \Acyclic\Machines\V1\MachineMutationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Machines\V1\MutationAdmission>
     */
    public function DestroyMachine(\Acyclic\Machines\V1\MachineMutationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.machines.v1.MachinesService/DestroyMachine',
        $argument,
        ['\Acyclic\Machines\V1\MutationAdmission', 'decode'],
        $metadata, $options);
    }

    /**
     * MachinesService.DestroyCheckpoint operation.
     * @param \Acyclic\Machines\V1\CheckpointMutationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Machines\V1\MutationAdmission>
     */
    public function DestroyCheckpoint(\Acyclic\Machines\V1\CheckpointMutationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.machines.v1.MachinesService/DestroyCheckpoint',
        $argument,
        ['\Acyclic\Machines\V1\MutationAdmission', 'decode'],
        $metadata, $options);
    }

    /**
     * MachinesService.Recover operation.
     * @param \Acyclic\Machines\V1\RecoverRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Machines\V1\RecoveredAdmission>
     */
    public function Recover(\Acyclic\Machines\V1\RecoverRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.machines.v1.MachinesService/Recover',
        $argument,
        ['\Acyclic\Machines\V1\RecoveredAdmission', 'decode'],
        $metadata, $options);
    }

    /**
     * MachinesService.InspectMachine operation.
     * @param \Acyclic\Machines\V1\InspectMachineRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Machines\V1\MachineState>
     */
    public function InspectMachine(\Acyclic\Machines\V1\InspectMachineRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.machines.v1.MachinesService/InspectMachine',
        $argument,
        ['\Acyclic\Machines\V1\MachineState', 'decode'],
        $metadata, $options);
    }

    /**
     * MachinesService.InspectCheckpoint operation.
     * @param \Acyclic\Machines\V1\InspectCheckpointRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Machines\V1\CheckpointState>
     */
    public function InspectCheckpoint(\Acyclic\Machines\V1\InspectCheckpointRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.machines.v1.MachinesService/InspectCheckpoint',
        $argument,
        ['\Acyclic\Machines\V1\CheckpointState', 'decode'],
        $metadata, $options);
    }

    /**
     * MachinesService.ListMachines operation.
     * @param \Acyclic\Machines\V1\ListMachinesRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Machines\V1\MachinePage>
     */
    public function ListMachines(\Acyclic\Machines\V1\ListMachinesRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.machines.v1.MachinesService/ListMachines',
        $argument,
        ['\Acyclic\Machines\V1\MachinePage', 'decode'],
        $metadata, $options);
    }

    /**
     * MachinesService.Events operation.
     * @param \Acyclic\Machines\V1\EventsRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Machines\V1\EventPage>
     */
    public function Events(\Acyclic\Machines\V1\EventsRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.machines.v1.MachinesService/Events',
        $argument,
        ['\Acyclic\Machines\V1\EventPage', 'decode'],
        $metadata, $options);
    }

    /**
     * MachinesService.Usage operation.
     * @param \Acyclic\Machines\V1\UsageRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Machines\V1\UsageReceipt>
     */
    public function Usage(\Acyclic\Machines\V1\UsageRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.machines.v1.MachinesService/Usage',
        $argument,
        ['\Acyclic\Machines\V1\UsageReceipt', 'decode'],
        $metadata, $options);
    }

    /**
     * MachinesService.Cancel operation.
     * @param \Acyclic\Machines\V1\OperationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Machines\V1\OperationState>
     */
    public function Cancel(\Acyclic\Machines\V1\OperationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.machines.v1.MachinesService/Cancel',
        $argument,
        ['\Acyclic\Machines\V1\OperationState', 'decode'],
        $metadata, $options);
    }

    /**
     * MachinesService.InspectOperation operation.
     * @param \Acyclic\Machines\V1\OperationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\UnaryCall<\Acyclic\Machines\V1\OperationState>
     */
    public function InspectOperation(\Acyclic\Machines\V1\OperationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_simpleRequest('/acyclic.machines.v1.MachinesService/InspectOperation',
        $argument,
        ['\Acyclic\Machines\V1\OperationState', 'decode'],
        $metadata, $options);
    }

    /**
     * MachinesService.WatchOperation operation.
     * @param \Acyclic\Machines\V1\OperationRequest $argument input argument
     * @param array $metadata metadata
     * @param array $options call options
     * @return \Grpc\ServerStreamingCall
     */
    public function WatchOperation(\Acyclic\Machines\V1\OperationRequest $argument,
      $metadata = [], $options = []) {
        return $this->_serverStreamRequest('/acyclic.machines.v1.MachinesService/WatchOperation',
        $argument,
        ['\Acyclic\Machines\V1\OperationState', 'decode'],
        $metadata, $options);
    }

}
