{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}
module Acyclic.Remote.Api
  ( Rpc
  , ActorsCreateActor
  , ActorsUpdateActor
  , ActorsInspectActor
  , ActorsAddSubscription
  , ActorsRemoveSubscription
  , ActorsResumeSubscription
  , ActorsCheckpointActor
  , ActorsInvokeActor
  , WorkersPublishVersion
  , WorkersSelectDeployment
  , WorkersSubmitJob
  , WorkersInspectJob
  , WorkersCancelJob
  , WorkersInvokeVersion
  , WorkersInvokeDeployment
  , StreamInspectIdempotency
  , StreamAppend
  , StreamTail
  , StreamFork
  , StreamRead
  , StreamFollow
  , StreamChildren
  , StreamChildrenPage
  , StreamCommit
  , StreamReadCommit
  , ObjectsV2BucketsCreateBucket
  , ObjectsV2BucketsHeadBucket
  , ObjectsV2BucketsDeleteBucket
  , ObjectsV2ObjectsPutObject
  , ObjectsV2ObjectsGetObject
  , ObjectsV2ObjectsHeadObject
  , ObjectsV2ObjectsListObjects
  , ObjectsV2ObjectsDeleteObject
  , ObjectsV2MultipartCreateMultipart
  , ObjectsV2MultipartUploadPart
  , ObjectsV2MultipartListParts
  , ObjectsV2MultipartCompleteMultipart
  , ObjectsV2MultipartAbortMultipart
  , InferenceModelsList
  , InferenceContextsCreate
  , InferenceContextsInspect
  , InferenceContextsMutate
  , InferenceWarmContextsRetain
  , InferenceWarmContextsInspect
  , InferenceWarmContextsRenew
  , InferenceWarmContextsRelease
  , InferenceRunsGenerate
  , InferenceRunsInspect
  , InferenceRunsWatch
  , InferenceRunsCancel
  , InferenceEvaluationsCreate
  , InferenceEvaluationsInspect
  , MachinesQualifyImage
  , MachinesCreate
  , MachinesCheckpoint
  , MachinesFork
  , MachinesForkMachine
  , MachinesSuspend
  , MachinesWake
  , MachinesSetSuspensionPolicy
  , MachinesDestroyMachine
  , MachinesDestroyCheckpoint
  , MachinesRecover
  , MachinesInspectMachine
  , MachinesInspectCheckpoint
  , MachinesListMachines
  , MachinesEvents
  , MachinesUsage
  , MachinesCancel
  , MachinesInspectOperation
  , MachinesWatchOperation
  , FilesystemHandshake
  , FilesystemCreateWorkspace
  , FilesystemOpenWorkspace
  , FilesystemGetHead
  , FilesystemGetGeneration
  , FilesystemRead
  , FilesystemStat
  , FilesystemListDirectory
  , FilesystemReadLink
  , FilesystemPlanExtents
  , FilesystemApplyTransaction
  , FilesystemRebaseTransaction
  , FilesystemForkWorkspace
  , FilesystemDiff
  , FilesystemRebase
  , FilesystemPlanJoin
  , FilesystemApplyJoin
  , FilesystemCheckpoint
  , FilesystemPin
  , FilesystemExport
  , FilesystemImport
  , FilesystemIssueMountCredential
  , FilesystemIssueS3Credential
  , FilesystemGetSourceState
  , FilesystemReconcileSource
  , FilesystemRescanSource
  , FilesystemSealSource
  , FilesystemObserve
  , FilesystemCancel
  , FilesystemDeleteWorkspace
  , HarnessHandshake
  , HarnessSubmit
  , HarnessReplay
  , HarnessObserve
  , HarnessCancel
  , actorsCreateActor
  , actorsUpdateActor
  , actorsInspectActor
  , actorsAddSubscription
  , actorsRemoveSubscription
  , actorsResumeSubscription
  , actorsCheckpointActor
  , actorsInvokeActor
  , workersPublishVersion
  , workersSelectDeployment
  , workersSubmitJob
  , workersInspectJob
  , workersCancelJob
  , workersInvokeVersion
  , workersInvokeDeployment
  , streamInspectIdempotency
  , streamAppend
  , streamTail
  , streamFork
  , streamRead
  , streamFollow
  , streamChildren
  , streamChildrenPage
  , streamCommit
  , streamReadCommit
  , objectsV2BucketsCreateBucket
  , objectsV2BucketsHeadBucket
  , objectsV2BucketsDeleteBucket
  , objectsV2ObjectsPutObject
  , objectsV2ObjectsGetObject
  , objectsV2ObjectsHeadObject
  , objectsV2ObjectsListObjects
  , objectsV2ObjectsDeleteObject
  , objectsV2MultipartCreateMultipart
  , objectsV2MultipartUploadPart
  , objectsV2MultipartListParts
  , objectsV2MultipartCompleteMultipart
  , objectsV2MultipartAbortMultipart
  , inferenceModelsList
  , inferenceContextsCreate
  , inferenceContextsInspect
  , inferenceContextsMutate
  , inferenceWarmContextsRetain
  , inferenceWarmContextsInspect
  , inferenceWarmContextsRenew
  , inferenceWarmContextsRelease
  , inferenceRunsGenerate
  , inferenceRunsInspect
  , inferenceRunsWatch
  , inferenceRunsCancel
  , inferenceEvaluationsCreate
  , inferenceEvaluationsInspect
  , machinesQualifyImage
  , machinesCreate
  , machinesCheckpoint
  , machinesFork
  , machinesForkMachine
  , machinesSuspend
  , machinesWake
  , machinesSetSuspensionPolicy
  , machinesDestroyMachine
  , machinesDestroyCheckpoint
  , machinesRecover
  , machinesInspectMachine
  , machinesInspectCheckpoint
  , machinesListMachines
  , machinesEvents
  , machinesUsage
  , machinesCancel
  , machinesInspectOperation
  , machinesWatchOperation
  , filesystemHandshake
  , filesystemCreateWorkspace
  , filesystemOpenWorkspace
  , filesystemGetHead
  , filesystemGetGeneration
  , filesystemRead
  , filesystemStat
  , filesystemListDirectory
  , filesystemReadLink
  , filesystemPlanExtents
  , filesystemApplyTransaction
  , filesystemRebaseTransaction
  , filesystemForkWorkspace
  , filesystemDiff
  , filesystemRebase
  , filesystemPlanJoin
  , filesystemApplyJoin
  , filesystemCheckpoint
  , filesystemPin
  , filesystemExport
  , filesystemImport
  , filesystemIssueMountCredential
  , filesystemIssueS3Credential
  , filesystemGetSourceState
  , filesystemReconcileSource
  , filesystemRescanSource
  , filesystemSealSource
  , filesystemObserve
  , filesystemCancel
  , filesystemDeleteWorkspace
  , harnessHandshake
  , harnessSubmit
  , harnessReplay
  , harnessObserve
  , harnessCancel
  ) where

import qualified Network.GRPC.Client as Client
import qualified Network.GRPC.Client.StreamType.IO as Typed
import Network.GRPC.Common
  ( NoMetadata
  , RequestMetadata
  , ResponseInitialMetadata
  , ResponseTrailingMetadata
  )
import Network.GRPC.Common.Protobuf (Protobuf)
import Acyclic.Stream.Api ()
import qualified Proto.Actors.V1.Actors as Actors
import qualified Proto.Filesystem.V2.Filesystem as Filesystem
import qualified Proto.Harness.V2.Harness as Harness
import qualified Proto.Inference.V1.Inference as Inference
import qualified Proto.Machines.V1.Machines as Machines
import qualified Proto.Objects.V2.Objects as ObjectsV2
import qualified Proto.Stream.V2.Stream as Stream
import qualified Proto.Workers.V1.Workers as Workers

type instance RequestMetadata (Protobuf Actors.ActorsService meth) = NoMetadata
type instance ResponseInitialMetadata (Protobuf Actors.ActorsService meth) = NoMetadata
type instance ResponseTrailingMetadata (Protobuf Actors.ActorsService meth) = NoMetadata
type instance RequestMetadata (Protobuf Filesystem.FilesystemService meth) = NoMetadata
type instance ResponseInitialMetadata (Protobuf Filesystem.FilesystemService meth) = NoMetadata
type instance ResponseTrailingMetadata (Protobuf Filesystem.FilesystemService meth) = NoMetadata
type instance RequestMetadata (Protobuf Harness.HarnessService meth) = NoMetadata
type instance ResponseInitialMetadata (Protobuf Harness.HarnessService meth) = NoMetadata
type instance ResponseTrailingMetadata (Protobuf Harness.HarnessService meth) = NoMetadata
type instance RequestMetadata (Protobuf Inference.ContextsService meth) = NoMetadata
type instance ResponseInitialMetadata (Protobuf Inference.ContextsService meth) = NoMetadata
type instance ResponseTrailingMetadata (Protobuf Inference.ContextsService meth) = NoMetadata
type instance RequestMetadata (Protobuf Inference.EvaluationsService meth) = NoMetadata
type instance ResponseInitialMetadata (Protobuf Inference.EvaluationsService meth) = NoMetadata
type instance ResponseTrailingMetadata (Protobuf Inference.EvaluationsService meth) = NoMetadata
type instance RequestMetadata (Protobuf Inference.ModelsService meth) = NoMetadata
type instance ResponseInitialMetadata (Protobuf Inference.ModelsService meth) = NoMetadata
type instance ResponseTrailingMetadata (Protobuf Inference.ModelsService meth) = NoMetadata
type instance RequestMetadata (Protobuf Inference.RunsService meth) = NoMetadata
type instance ResponseInitialMetadata (Protobuf Inference.RunsService meth) = NoMetadata
type instance ResponseTrailingMetadata (Protobuf Inference.RunsService meth) = NoMetadata
type instance RequestMetadata (Protobuf Inference.WarmContextsService meth) = NoMetadata
type instance ResponseInitialMetadata (Protobuf Inference.WarmContextsService meth) = NoMetadata
type instance ResponseTrailingMetadata (Protobuf Inference.WarmContextsService meth) = NoMetadata
type instance RequestMetadata (Protobuf Machines.MachinesService meth) = NoMetadata
type instance ResponseInitialMetadata (Protobuf Machines.MachinesService meth) = NoMetadata
type instance ResponseTrailingMetadata (Protobuf Machines.MachinesService meth) = NoMetadata
type instance RequestMetadata (Protobuf ObjectsV2.BucketsService meth) = NoMetadata
type instance ResponseInitialMetadata (Protobuf ObjectsV2.BucketsService meth) = NoMetadata
type instance ResponseTrailingMetadata (Protobuf ObjectsV2.BucketsService meth) = NoMetadata
type instance RequestMetadata (Protobuf ObjectsV2.MultipartService meth) = NoMetadata
type instance ResponseInitialMetadata (Protobuf ObjectsV2.MultipartService meth) = NoMetadata
type instance ResponseTrailingMetadata (Protobuf ObjectsV2.MultipartService meth) = NoMetadata
type instance RequestMetadata (Protobuf ObjectsV2.ObjectsService meth) = NoMetadata
type instance ResponseInitialMetadata (Protobuf ObjectsV2.ObjectsService meth) = NoMetadata
type instance ResponseTrailingMetadata (Protobuf ObjectsV2.ObjectsService meth) = NoMetadata
type instance RequestMetadata (Protobuf Workers.WorkersService meth) = NoMetadata
type instance ResponseInitialMetadata (Protobuf Workers.WorkersService meth) = NoMetadata
type instance ResponseTrailingMetadata (Protobuf Workers.WorkersService meth) = NoMetadata

-- Generated from the Rust typed-request manifest; do not edit.
-- Each facade retains the generated request, response, and transport mode.

type Rpc service method = Protobuf service method

type ActorsCreateActor = Protobuf Actors.ActorsService "createActor"
type ActorsUpdateActor = Protobuf Actors.ActorsService "updateActor"
type ActorsInspectActor = Protobuf Actors.ActorsService "inspectActor"
type ActorsAddSubscription = Protobuf Actors.ActorsService "addSubscription"
type ActorsRemoveSubscription = Protobuf Actors.ActorsService "removeSubscription"
type ActorsResumeSubscription = Protobuf Actors.ActorsService "resumeSubscription"
type ActorsCheckpointActor = Protobuf Actors.ActorsService "checkpointActor"
type ActorsInvokeActor = Protobuf Actors.ActorsService "invokeActor"
type WorkersPublishVersion = Protobuf Workers.WorkersService "publishVersion"
type WorkersSelectDeployment = Protobuf Workers.WorkersService "selectDeployment"
type WorkersSubmitJob = Protobuf Workers.WorkersService "submitJob"
type WorkersInspectJob = Protobuf Workers.WorkersService "inspectJob"
type WorkersCancelJob = Protobuf Workers.WorkersService "cancelJob"
type WorkersInvokeVersion = Protobuf Workers.WorkersService "invokeVersion"
type WorkersInvokeDeployment = Protobuf Workers.WorkersService "invokeDeployment"
type StreamInspectIdempotency = Protobuf Stream.StreamService "inspectIdempotency"
type StreamAppend = Protobuf Stream.StreamService "append"
type StreamTail = Protobuf Stream.StreamService "tail"
type StreamFork = Protobuf Stream.StreamService "fork"
type StreamRead = Protobuf Stream.StreamService "read"
type StreamFollow = Protobuf Stream.StreamService "follow"
type StreamChildren = Protobuf Stream.StreamService "children"
type StreamChildrenPage = Protobuf Stream.StreamService "childrenPage"
type StreamCommit = Protobuf Stream.StreamService "commit"
type StreamReadCommit = Protobuf Stream.StreamService "readCommit"
type ObjectsV2BucketsCreateBucket = Protobuf ObjectsV2.BucketsService "createBucket"
type ObjectsV2BucketsHeadBucket = Protobuf ObjectsV2.BucketsService "headBucket"
type ObjectsV2BucketsDeleteBucket = Protobuf ObjectsV2.BucketsService "deleteBucket"
type ObjectsV2ObjectsPutObject = Protobuf ObjectsV2.ObjectsService "putObject"
type ObjectsV2ObjectsGetObject = Protobuf ObjectsV2.ObjectsService "getObject"
type ObjectsV2ObjectsHeadObject = Protobuf ObjectsV2.ObjectsService "headObject"
type ObjectsV2ObjectsListObjects = Protobuf ObjectsV2.ObjectsService "listObjects"
type ObjectsV2ObjectsDeleteObject = Protobuf ObjectsV2.ObjectsService "deleteObject"
type ObjectsV2MultipartCreateMultipart = Protobuf ObjectsV2.MultipartService "createMultipart"
type ObjectsV2MultipartUploadPart = Protobuf ObjectsV2.MultipartService "uploadPart"
type ObjectsV2MultipartListParts = Protobuf ObjectsV2.MultipartService "listParts"
type ObjectsV2MultipartCompleteMultipart = Protobuf ObjectsV2.MultipartService "completeMultipart"
type ObjectsV2MultipartAbortMultipart = Protobuf ObjectsV2.MultipartService "abortMultipart"
type InferenceModelsList = Protobuf Inference.ModelsService "list"
type InferenceContextsCreate = Protobuf Inference.ContextsService "create"
type InferenceContextsInspect = Protobuf Inference.ContextsService "inspect"
type InferenceContextsMutate = Protobuf Inference.ContextsService "mutate"
type InferenceWarmContextsRetain = Protobuf Inference.WarmContextsService "retain"
type InferenceWarmContextsInspect = Protobuf Inference.WarmContextsService "inspect"
type InferenceWarmContextsRenew = Protobuf Inference.WarmContextsService "renew"
type InferenceWarmContextsRelease = Protobuf Inference.WarmContextsService "release"
type InferenceRunsGenerate = Protobuf Inference.RunsService "generate"
type InferenceRunsInspect = Protobuf Inference.RunsService "inspect"
type InferenceRunsWatch = Protobuf Inference.RunsService "watch"
type InferenceRunsCancel = Protobuf Inference.RunsService "cancel"
type InferenceEvaluationsCreate = Protobuf Inference.EvaluationsService "create"
type InferenceEvaluationsInspect = Protobuf Inference.EvaluationsService "inspect"
type MachinesQualifyImage = Protobuf Machines.MachinesService "qualifyImage"
type MachinesCreate = Protobuf Machines.MachinesService "create"
type MachinesCheckpoint = Protobuf Machines.MachinesService "checkpoint"
type MachinesFork = Protobuf Machines.MachinesService "fork"
type MachinesForkMachine = Protobuf Machines.MachinesService "forkMachine"
type MachinesSuspend = Protobuf Machines.MachinesService "suspend"
type MachinesWake = Protobuf Machines.MachinesService "wake"
type MachinesSetSuspensionPolicy = Protobuf Machines.MachinesService "setSuspensionPolicy"
type MachinesDestroyMachine = Protobuf Machines.MachinesService "destroyMachine"
type MachinesDestroyCheckpoint = Protobuf Machines.MachinesService "destroyCheckpoint"
type MachinesRecover = Protobuf Machines.MachinesService "recover"
type MachinesInspectMachine = Protobuf Machines.MachinesService "inspectMachine"
type MachinesInspectCheckpoint = Protobuf Machines.MachinesService "inspectCheckpoint"
type MachinesListMachines = Protobuf Machines.MachinesService "listMachines"
type MachinesEvents = Protobuf Machines.MachinesService "events"
type MachinesUsage = Protobuf Machines.MachinesService "usage"
type MachinesCancel = Protobuf Machines.MachinesService "cancel"
type MachinesInspectOperation = Protobuf Machines.MachinesService "inspectOperation"
type MachinesWatchOperation = Protobuf Machines.MachinesService "watchOperation"
type FilesystemHandshake = Protobuf Filesystem.FilesystemService "handshake"
type FilesystemCreateWorkspace = Protobuf Filesystem.FilesystemService "createWorkspace"
type FilesystemOpenWorkspace = Protobuf Filesystem.FilesystemService "openWorkspace"
type FilesystemGetHead = Protobuf Filesystem.FilesystemService "getHead"
type FilesystemGetGeneration = Protobuf Filesystem.FilesystemService "getGeneration"
type FilesystemRead = Protobuf Filesystem.FilesystemService "read"
type FilesystemStat = Protobuf Filesystem.FilesystemService "stat"
type FilesystemListDirectory = Protobuf Filesystem.FilesystemService "listDirectory"
type FilesystemReadLink = Protobuf Filesystem.FilesystemService "readLink"
type FilesystemPlanExtents = Protobuf Filesystem.FilesystemService "planExtents"
type FilesystemApplyTransaction = Protobuf Filesystem.FilesystemService "applyTransaction"
type FilesystemRebaseTransaction = Protobuf Filesystem.FilesystemService "rebaseTransaction"
type FilesystemForkWorkspace = Protobuf Filesystem.FilesystemService "forkWorkspace"
type FilesystemDiff = Protobuf Filesystem.FilesystemService "diff"
type FilesystemRebase = Protobuf Filesystem.FilesystemService "rebase"
type FilesystemPlanJoin = Protobuf Filesystem.FilesystemService "planJoin"
type FilesystemApplyJoin = Protobuf Filesystem.FilesystemService "applyJoin"
type FilesystemCheckpoint = Protobuf Filesystem.FilesystemService "checkpoint"
type FilesystemPin = Protobuf Filesystem.FilesystemService "pin"
type FilesystemExport = Protobuf Filesystem.FilesystemService "export"
type FilesystemImport = Protobuf Filesystem.FilesystemService "import"
type FilesystemIssueMountCredential = Protobuf Filesystem.FilesystemService "issueMountCredential"
type FilesystemIssueS3Credential = Protobuf Filesystem.FilesystemService "issueS3Credential"
type FilesystemGetSourceState = Protobuf Filesystem.FilesystemService "getSourceState"
type FilesystemReconcileSource = Protobuf Filesystem.FilesystemService "reconcileSource"
type FilesystemRescanSource = Protobuf Filesystem.FilesystemService "rescanSource"
type FilesystemSealSource = Protobuf Filesystem.FilesystemService "sealSource"
type FilesystemObserve = Protobuf Filesystem.FilesystemService "observe"
type FilesystemCancel = Protobuf Filesystem.FilesystemService "cancel"
type FilesystemDeleteWorkspace = Protobuf Filesystem.FilesystemService "deleteWorkspace"
type HarnessHandshake = Protobuf Harness.HarnessService "handshake"
type HarnessSubmit = Protobuf Harness.HarnessService "submit"
type HarnessReplay = Protobuf Harness.HarnessService "replay"
type HarnessObserve = Protobuf Harness.HarnessService "observe"
type HarnessCancel = Protobuf Harness.HarnessService "cancel"

actorsCreateActor conn value = Typed.nonStreaming conn (Client.rpc @ActorsCreateActor) value
actorsUpdateActor conn value = Typed.nonStreaming conn (Client.rpc @ActorsUpdateActor) value
actorsInspectActor conn value = Typed.nonStreaming conn (Client.rpc @ActorsInspectActor) value
actorsAddSubscription conn value = Typed.nonStreaming conn (Client.rpc @ActorsAddSubscription) value
actorsRemoveSubscription conn value = Typed.nonStreaming conn (Client.rpc @ActorsRemoveSubscription) value
actorsResumeSubscription conn value = Typed.nonStreaming conn (Client.rpc @ActorsResumeSubscription) value
actorsCheckpointActor conn value = Typed.nonStreaming conn (Client.rpc @ActorsCheckpointActor) value
actorsInvokeActor conn value = Typed.nonStreaming conn (Client.rpc @ActorsInvokeActor) value
workersPublishVersion conn value = Typed.nonStreaming conn (Client.rpc @WorkersPublishVersion) value
workersSelectDeployment conn value = Typed.nonStreaming conn (Client.rpc @WorkersSelectDeployment) value
workersSubmitJob conn value = Typed.nonStreaming conn (Client.rpc @WorkersSubmitJob) value
workersInspectJob conn value = Typed.nonStreaming conn (Client.rpc @WorkersInspectJob) value
workersCancelJob conn value = Typed.nonStreaming conn (Client.rpc @WorkersCancelJob) value
workersInvokeVersion conn value = Typed.nonStreaming conn (Client.rpc @WorkersInvokeVersion) value
workersInvokeDeployment conn value = Typed.nonStreaming conn (Client.rpc @WorkersInvokeDeployment) value
streamInspectIdempotency conn value = Typed.nonStreaming conn (Client.rpc @StreamInspectIdempotency) value
streamAppend conn value = Typed.nonStreaming conn (Client.rpc @StreamAppend) value
streamTail conn value = Typed.nonStreaming conn (Client.rpc @StreamTail) value
streamFork conn value = Typed.nonStreaming conn (Client.rpc @StreamFork) value
streamRead conn value receive = Typed.serverStreaming conn (Client.rpc @StreamRead) value receive
streamFollow conn value receive = Typed.serverStreaming conn (Client.rpc @StreamFollow) value receive
streamChildren conn value receive = Typed.serverStreaming conn (Client.rpc @StreamChildren) value receive
streamChildrenPage conn value = Typed.nonStreaming conn (Client.rpc @StreamChildrenPage) value
streamCommit conn value = Typed.nonStreaming conn (Client.rpc @StreamCommit) value
streamReadCommit conn value = Typed.nonStreaming conn (Client.rpc @StreamReadCommit) value
objectsV2BucketsCreateBucket conn value = Typed.nonStreaming conn (Client.rpc @ObjectsV2BucketsCreateBucket) value
objectsV2BucketsHeadBucket conn value = Typed.nonStreaming conn (Client.rpc @ObjectsV2BucketsHeadBucket) value
objectsV2BucketsDeleteBucket conn value = Typed.nonStreaming conn (Client.rpc @ObjectsV2BucketsDeleteBucket) value
objectsV2ObjectsPutObject conn send = Typed.clientStreaming conn (Client.rpc @ObjectsV2ObjectsPutObject) send
objectsV2ObjectsGetObject conn value receive = Typed.serverStreaming conn (Client.rpc @ObjectsV2ObjectsGetObject) value receive
objectsV2ObjectsHeadObject conn value = Typed.nonStreaming conn (Client.rpc @ObjectsV2ObjectsHeadObject) value
objectsV2ObjectsListObjects conn value = Typed.nonStreaming conn (Client.rpc @ObjectsV2ObjectsListObjects) value
objectsV2ObjectsDeleteObject conn value = Typed.nonStreaming conn (Client.rpc @ObjectsV2ObjectsDeleteObject) value
objectsV2MultipartCreateMultipart conn value = Typed.nonStreaming conn (Client.rpc @ObjectsV2MultipartCreateMultipart) value
objectsV2MultipartUploadPart conn send = Typed.clientStreaming conn (Client.rpc @ObjectsV2MultipartUploadPart) send
objectsV2MultipartListParts conn value = Typed.nonStreaming conn (Client.rpc @ObjectsV2MultipartListParts) value
objectsV2MultipartCompleteMultipart conn value = Typed.nonStreaming conn (Client.rpc @ObjectsV2MultipartCompleteMultipart) value
objectsV2MultipartAbortMultipart conn value = Typed.nonStreaming conn (Client.rpc @ObjectsV2MultipartAbortMultipart) value
inferenceModelsList conn value = Typed.nonStreaming conn (Client.rpc @InferenceModelsList) value
inferenceContextsCreate conn value = Typed.nonStreaming conn (Client.rpc @InferenceContextsCreate) value
inferenceContextsInspect conn value = Typed.nonStreaming conn (Client.rpc @InferenceContextsInspect) value
inferenceContextsMutate conn value = Typed.nonStreaming conn (Client.rpc @InferenceContextsMutate) value
inferenceWarmContextsRetain conn value = Typed.nonStreaming conn (Client.rpc @InferenceWarmContextsRetain) value
inferenceWarmContextsInspect conn value = Typed.nonStreaming conn (Client.rpc @InferenceWarmContextsInspect) value
inferenceWarmContextsRenew conn value = Typed.nonStreaming conn (Client.rpc @InferenceWarmContextsRenew) value
inferenceWarmContextsRelease conn value = Typed.nonStreaming conn (Client.rpc @InferenceWarmContextsRelease) value
inferenceRunsGenerate conn value = Typed.nonStreaming conn (Client.rpc @InferenceRunsGenerate) value
inferenceRunsInspect conn value = Typed.nonStreaming conn (Client.rpc @InferenceRunsInspect) value
inferenceRunsWatch conn value receive = Typed.serverStreaming conn (Client.rpc @InferenceRunsWatch) value receive
inferenceRunsCancel conn value = Typed.nonStreaming conn (Client.rpc @InferenceRunsCancel) value
inferenceEvaluationsCreate conn value = Typed.nonStreaming conn (Client.rpc @InferenceEvaluationsCreate) value
inferenceEvaluationsInspect conn value = Typed.nonStreaming conn (Client.rpc @InferenceEvaluationsInspect) value
machinesQualifyImage conn value = Typed.nonStreaming conn (Client.rpc @MachinesQualifyImage) value
machinesCreate conn value = Typed.nonStreaming conn (Client.rpc @MachinesCreate) value
machinesCheckpoint conn value = Typed.nonStreaming conn (Client.rpc @MachinesCheckpoint) value
machinesFork conn value = Typed.nonStreaming conn (Client.rpc @MachinesFork) value
machinesForkMachine conn value = Typed.nonStreaming conn (Client.rpc @MachinesForkMachine) value
machinesSuspend conn value = Typed.nonStreaming conn (Client.rpc @MachinesSuspend) value
machinesWake conn value = Typed.nonStreaming conn (Client.rpc @MachinesWake) value
machinesSetSuspensionPolicy conn value = Typed.nonStreaming conn (Client.rpc @MachinesSetSuspensionPolicy) value
machinesDestroyMachine conn value = Typed.nonStreaming conn (Client.rpc @MachinesDestroyMachine) value
machinesDestroyCheckpoint conn value = Typed.nonStreaming conn (Client.rpc @MachinesDestroyCheckpoint) value
machinesRecover conn value = Typed.nonStreaming conn (Client.rpc @MachinesRecover) value
machinesInspectMachine conn value = Typed.nonStreaming conn (Client.rpc @MachinesInspectMachine) value
machinesInspectCheckpoint conn value = Typed.nonStreaming conn (Client.rpc @MachinesInspectCheckpoint) value
machinesListMachines conn value = Typed.nonStreaming conn (Client.rpc @MachinesListMachines) value
machinesEvents conn value = Typed.nonStreaming conn (Client.rpc @MachinesEvents) value
machinesUsage conn value = Typed.nonStreaming conn (Client.rpc @MachinesUsage) value
machinesCancel conn value = Typed.nonStreaming conn (Client.rpc @MachinesCancel) value
machinesInspectOperation conn value = Typed.nonStreaming conn (Client.rpc @MachinesInspectOperation) value
machinesWatchOperation conn value receive = Typed.serverStreaming conn (Client.rpc @MachinesWatchOperation) value receive
filesystemHandshake conn value = Typed.nonStreaming conn (Client.rpc @FilesystemHandshake) value
filesystemCreateWorkspace conn value = Typed.nonStreaming conn (Client.rpc @FilesystemCreateWorkspace) value
filesystemOpenWorkspace conn value = Typed.nonStreaming conn (Client.rpc @FilesystemOpenWorkspace) value
filesystemGetHead conn value = Typed.nonStreaming conn (Client.rpc @FilesystemGetHead) value
filesystemGetGeneration conn value = Typed.nonStreaming conn (Client.rpc @FilesystemGetGeneration) value
filesystemRead conn value = Typed.nonStreaming conn (Client.rpc @FilesystemRead) value
filesystemStat conn value = Typed.nonStreaming conn (Client.rpc @FilesystemStat) value
filesystemListDirectory conn value = Typed.nonStreaming conn (Client.rpc @FilesystemListDirectory) value
filesystemReadLink conn value = Typed.nonStreaming conn (Client.rpc @FilesystemReadLink) value
filesystemPlanExtents conn value = Typed.nonStreaming conn (Client.rpc @FilesystemPlanExtents) value
filesystemApplyTransaction conn value = Typed.nonStreaming conn (Client.rpc @FilesystemApplyTransaction) value
filesystemRebaseTransaction conn value = Typed.nonStreaming conn (Client.rpc @FilesystemRebaseTransaction) value
filesystemForkWorkspace conn value = Typed.nonStreaming conn (Client.rpc @FilesystemForkWorkspace) value
filesystemDiff conn value = Typed.nonStreaming conn (Client.rpc @FilesystemDiff) value
filesystemRebase conn value = Typed.nonStreaming conn (Client.rpc @FilesystemRebase) value
filesystemPlanJoin conn value = Typed.nonStreaming conn (Client.rpc @FilesystemPlanJoin) value
filesystemApplyJoin conn value = Typed.nonStreaming conn (Client.rpc @FilesystemApplyJoin) value
filesystemCheckpoint conn value = Typed.nonStreaming conn (Client.rpc @FilesystemCheckpoint) value
filesystemPin conn value = Typed.nonStreaming conn (Client.rpc @FilesystemPin) value
filesystemExport conn value receive = Typed.serverStreaming conn (Client.rpc @FilesystemExport) value receive
filesystemImport conn send = Typed.clientStreaming conn (Client.rpc @FilesystemImport) send
filesystemIssueMountCredential conn value = Typed.nonStreaming conn (Client.rpc @FilesystemIssueMountCredential) value
filesystemIssueS3Credential conn value = Typed.nonStreaming conn (Client.rpc @FilesystemIssueS3Credential) value
filesystemGetSourceState conn value = Typed.nonStreaming conn (Client.rpc @FilesystemGetSourceState) value
filesystemReconcileSource conn value = Typed.nonStreaming conn (Client.rpc @FilesystemReconcileSource) value
filesystemRescanSource conn value = Typed.nonStreaming conn (Client.rpc @FilesystemRescanSource) value
filesystemSealSource conn value = Typed.nonStreaming conn (Client.rpc @FilesystemSealSource) value
filesystemObserve conn value = Typed.nonStreaming conn (Client.rpc @FilesystemObserve) value
filesystemCancel conn value = Typed.nonStreaming conn (Client.rpc @FilesystemCancel) value
filesystemDeleteWorkspace conn value = Typed.nonStreaming conn (Client.rpc @FilesystemDeleteWorkspace) value
harnessHandshake conn value = Typed.nonStreaming conn (Client.rpc @HarnessHandshake) value
harnessSubmit conn value = Typed.nonStreaming conn (Client.rpc @HarnessSubmit) value
harnessReplay conn value receive = Typed.serverStreaming conn (Client.rpc @HarnessReplay) value receive
harnessObserve conn value = Typed.nonStreaming conn (Client.rpc @HarnessObserve) value
harnessCancel conn value = Typed.nonStreaming conn (Client.rpc @HarnessCancel) value
