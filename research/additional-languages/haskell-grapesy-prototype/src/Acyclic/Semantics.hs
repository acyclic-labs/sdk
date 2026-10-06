{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GADTs #-}
module Acyclic.Semantics
  ( WireChoice(..)
  , KnownOneofPayload(..)
  , ActorId, mkActorId, actorIdValue
  , MethodName, mkMethodName, methodNameValue
  , ResourcePath, mkResourcePath, resourcePathValue
  , SourceName, mkSourceName, sourceNameValue
  , DestinationName, mkDestinationName, destinationNameValue
  , BucketName, mkBucketName, bucketNameValue
  , ObjectKey, mkObjectKey, objectKeyValue
  , VersionAlias, mkVersionAlias, versionAliasValue
  , JobId, mkJobId, jobIdValue
  , MachineId, mkMachineId, machineIdValue
  , OperationId, mkOperationId, operationIdValue
  , WorkspaceId, mkWorkspaceId, workspaceIdValue
  , CheckpointId, mkCheckpointId, checkpointIdValue
  , IdempotencyKeyBytes, mkIdempotencyKeyBytes, idempotencyKeyBytesValue
  , IdempotencyKeyText, mkIdempotencyKeyText, idempotencyKeyTextValue
  , IdempotencyKey, mkIdempotencyKey, idempotencyKeyValue
  , OpaqueText, mkOpaqueText, opaqueTextValue
  , UploadId, mkUploadId, uploadIdValue
  , Sha256Digest, mkSha256Digest, sha256DigestValue
  , RevisionDigest, mkRevisionDigest, revisionDigestValue
  , Image, mkImage, imageValue
  , Revision, mkRevision, revisionValue
  , RunId, mkRunId, runIdValue
  , EvaluationId, mkEvaluationId, evaluationIdValue
  , PageLimit, mkPageLimit, pageLimitValue
  , StreamPageLimit, mkStreamPageLimit, streamPageLimitValue
  , MachinePageLimit, mkMachinePageLimit, machinePageLimitValue
  , MachineEventPageLimit, mkMachineEventPageLimit, machineEventPageLimitValue
  , CommitId, mkCommitId, commitIdValue
  , OpenEnumValue, mkOpenEnumValue, openEnumValueValue
  , OpaqueBytes, mkOpaqueBytes, opaqueBytesValue
  , SequenceNumber, mkSequenceNumber, sequenceNumberValue
  , NonNegativeCount, mkNonNegativeCount, nonNegativeCountValue
  , PositiveCount, mkPositiveCount, positiveCountValue
  , UnixTimestampMillis, mkUnixTimestampMillis, unixTimestampMillisValue
  , ActorsActorStateEnum, actorsActorStateEnumFromWire, actorsActorStateEnumValue
  , ActorsSubscriptionStateEnum, actorsSubscriptionStateEnumFromWire, actorsSubscriptionStateEnumValue
  , FilesystemConflictUseEnum, filesystemConflictUseEnumFromWire, filesystemConflictUseEnumValue
  , FilesystemExtentKindEnum, filesystemExtentKindEnumFromWire, filesystemExtentKindEnumValue
  , FilesystemFileKindEnum, filesystemFileKindEnumFromWire, filesystemFileKindEnumValue
  , FilesystemFilesystemProfileEnum, filesystemFilesystemProfileEnumFromWire, filesystemFilesystemProfileEnumValue
  , FilesystemJoinHistoryEnum, filesystemJoinHistoryEnumFromWire, filesystemJoinHistoryEnumValue
  , FilesystemJoinStatusEnum, filesystemJoinStatusEnumFromWire, filesystemJoinStatusEnumValue
  , FilesystemMutationStatusEnum, filesystemMutationStatusEnumFromWire, filesystemMutationStatusEnumValue
  , FilesystemNameEncodingEnum, filesystemNameEncodingEnumFromWire, filesystemNameEncodingEnumValue
  , FilesystemRebaseStatusEnum, filesystemRebaseStatusEnumFromWire, filesystemRebaseStatusEnumValue
  , FilesystemSourceInvalidationReasonEnum, filesystemSourceInvalidationReasonEnumFromWire, filesystemSourceInvalidationReasonEnumValue
  , FilesystemSourceStateEnum, filesystemSourceStateEnumFromWire, filesystemSourceStateEnumValue
  , FilesystemSparseTargetEnum, filesystemSparseTargetEnumFromWire, filesystemSparseTargetEnumValue
  , HarnessAdmissionStateEnum, harnessAdmissionStateEnumFromWire, harnessAdmissionStateEnumValue
  , HarnessAggregateKindEnum, harnessAggregateKindEnumFromWire, harnessAggregateKindEnumValue
  , HarnessCompletionStateEnum, harnessCompletionStateEnumFromWire, harnessCompletionStateEnumValue
  , HarnessErrorCodeEnum, harnessErrorCodeEnumFromWire, harnessErrorCodeEnumValue
  , InferenceCustomerEvaluationAggregationEnum, inferenceCustomerEvaluationAggregationEnumFromWire, inferenceCustomerEvaluationAggregationEnumValue
  , InferenceCustomerEvaluationCaseOutcomeEnum, inferenceCustomerEvaluationCaseOutcomeEnumFromWire, inferenceCustomerEvaluationCaseOutcomeEnumValue
  , InferenceCustomerEvaluationStateEnum, inferenceCustomerEvaluationStateEnumFromWire, inferenceCustomerEvaluationStateEnumValue
  , InferenceCustomerItemKindEnum, inferenceCustomerItemKindEnumFromWire, inferenceCustomerItemKindEnumValue
  , InferenceCustomerRunTerminalEnum, inferenceCustomerRunTerminalEnumFromWire, inferenceCustomerRunTerminalEnumValue
  , InferenceCustomerWarmStateEnum, inferenceCustomerWarmStateEnumFromWire, inferenceCustomerWarmStateEnumValue
  , MachinesCapabilityEnum, machinesCapabilityEnumFromWire, machinesCapabilityEnumValue
  , MachinesCompatibilityModeEnum, machinesCompatibilityModeEnumFromWire, machinesCompatibilityModeEnumValue
  , MachinesEventKindEnum, machinesEventKindEnumFromWire, machinesEventKindEnumValue
  , MachinesExpirationKindEnum, machinesExpirationKindEnumFromWire, machinesExpirationKindEnumValue
  , MachinesForkFidelityEnum, machinesForkFidelityEnumFromWire, machinesForkFidelityEnumValue
  , MachinesImageKindEnum, machinesImageKindEnumFromWire, machinesImageKindEnumValue
  , MachinesMachineStatusEnum, machinesMachineStatusEnumFromWire, machinesMachineStatusEnumValue
  , MachinesOperationStatusEnum, machinesOperationStatusEnumFromWire, machinesOperationStatusEnumValue
  , MachinesPressureKindEnum, machinesPressureKindEnumFromWire, machinesPressureKindEnumValue
  , ObjectsErrorCodeEnum, objectsErrorCodeEnumFromWire, objectsErrorCodeEnumValue
  , WorkersJobStateEnum, workersJobStateEnumFromWire, workersJobStateEnumValue
  ) where

-- Generated by the Rust semantic type policy; do not edit.
import qualified Data.ByteString as BS
import Data.Int (Int32, Int64)
import Data.Word (Word32, Word64)

import qualified Proto.Filesystem.V2.Filesystem as FilesystemV2
import qualified Proto.Inference.V1.Inference as InferenceV1
import qualified Proto.Machines.V1.Machines as MachinesV1
import qualified Proto.Objects.V2.Objects as ObjectsV2
import qualified Proto.Stream.V2.Stream as StreamV2
import qualified Proto.Workers.V1.Workers as WorkersV1

-- Every known arm is a descriptor-bound DTO or scalar; only future unknown arms are raw bytes.
data KnownOneofPayload where
  KnownAcyclicActorsV1SubscriptionStartCursorN1 :: Word64 -> KnownOneofPayload
  KnownAcyclicActorsV1SubscriptionStartCurrentHeadN2 :: Bool -> KnownOneofPayload
  KnownAcyclicFilesystemV2ConflictFileRecordN1 :: FilesystemV2.FileConflict -> KnownOneofPayload
  KnownAcyclicFilesystemV2ConflictMetadataN2 :: FilesystemV2.FileConflict -> KnownOneofPayload
  KnownAcyclicFilesystemV2ConflictFileLengthN3 :: FilesystemV2.FileConflict -> KnownOneofPayload
  KnownAcyclicFilesystemV2ConflictContentRangeN4 :: FilesystemV2.ContentConflict -> KnownOneofPayload
  KnownAcyclicFilesystemV2ConflictSparseSeekN5 :: FilesystemV2.SparseConflict -> KnownOneofPayload
  KnownAcyclicFilesystemV2ConflictDirectoryNameN6 :: FilesystemV2.DirectoryNameConflict -> KnownOneofPayload
  KnownAcyclicFilesystemV2ConflictDirectoryRangeN7 :: FilesystemV2.DirectoryRangeConflict -> KnownOneofPayload
  KnownAcyclicFilesystemV2OptionalU32PresentN1 :: Word32 -> KnownOneofPayload
  KnownAcyclicFilesystemV2OptionalU32UnavailableN2 :: Bool -> KnownOneofPayload
  KnownAcyclicFilesystemV2OptionalU64PresentN1 :: Word64 -> KnownOneofPayload
  KnownAcyclicFilesystemV2OptionalU64UnavailableN2 :: Bool -> KnownOneofPayload
  KnownAcyclicFilesystemV2MutationCreateFileN1 :: FilesystemV2.CreateFile -> KnownOneofPayload
  KnownAcyclicFilesystemV2MutationCreateDirectoryN2 :: FilesystemV2.CreateDirectory -> KnownOneofPayload
  KnownAcyclicFilesystemV2MutationCreateSymbolicLinkN3 :: FilesystemV2.CreateSymbolicLink -> KnownOneofPayload
  KnownAcyclicFilesystemV2MutationRemoveN4 :: FilesystemV2.Remove -> KnownOneofPayload
  KnownAcyclicFilesystemV2MutationRenameN5 :: FilesystemV2.Rename -> KnownOneofPayload
  KnownAcyclicFilesystemV2MutationHardLinkN6 :: FilesystemV2.HardLink -> KnownOneofPayload
  KnownAcyclicFilesystemV2MutationWriteN7 :: FilesystemV2.Write -> KnownOneofPayload
  KnownAcyclicFilesystemV2MutationResizeN8 :: FilesystemV2.Resize -> KnownOneofPayload
  KnownAcyclicFilesystemV2MutationZeroRangeN9 :: FilesystemV2.ZeroRange -> KnownOneofPayload
  KnownAcyclicFilesystemV2MutationPreallocateN10 :: FilesystemV2.Preallocate -> KnownOneofPayload
  KnownAcyclicFilesystemV2MutationCloneRangeN11 :: FilesystemV2.CloneRange -> KnownOneofPayload
  KnownAcyclicFilesystemV2MutationSetMetadataN12 :: FilesystemV2.SetMetadata -> KnownOneofPayload
  KnownAcyclicFilesystemV2MutationCreateDirectoriesN13 :: FilesystemV2.CreateDirectories -> KnownOneofPayload
  KnownAcyclicFilesystemV2MutationPutFileN14 :: FilesystemV2.PutFile -> KnownOneofPayload
  KnownAcyclicFilesystemV2MutationCopyFileN15 :: FilesystemV2.CopyFile -> KnownOneofPayload
  KnownAcyclicFilesystemV2OptionalI64PresentN1 :: Int64 -> KnownOneofPayload
  KnownAcyclicFilesystemV2OptionalI64UnavailableN2 :: Bool -> KnownOneofPayload
  KnownAcyclicFilesystemV2CredentialResponseBearerTokenN3 :: String -> KnownOneofPayload
  KnownAcyclicFilesystemV2CredentialResponseS3N4 :: FilesystemV2.S3Credential -> KnownOneofPayload
  KnownAcyclicFilesystemV2OpenWorkspaceRequestWorkspaceN1 :: FilesystemV2.WorkspaceRef -> KnownOneofPayload
  KnownAcyclicFilesystemV2OpenWorkspaceRequestNameN2 :: String -> KnownOneofPayload
  KnownInferenceCustomerV1ContextProvenanceCreatedN1 :: InferenceV1.Empty -> KnownOneofPayload
  KnownInferenceCustomerV1ContextProvenanceDerivedN2 :: InferenceV1.ProvenanceSource -> KnownOneofPayload
  KnownInferenceCustomerV1ContextProvenanceForkedN3 :: InferenceV1.ProvenanceSource -> KnownOneofPayload
  KnownInferenceCustomerV1ContextProvenanceTransferredN4 :: InferenceV1.TransferProvenance -> KnownOneofPayload
  KnownInferenceCustomerV1ContextProvenanceGeneratedN5 :: InferenceV1.GenerationProvenance -> KnownOneofPayload
  KnownInferenceCustomerV1ContextProvenanceRunInputN6 :: InferenceV1.RunInputProvenance -> KnownOneofPayload
  KnownInferenceCustomerV1EditAppendN1 :: InferenceV1.Item -> KnownOneofPayload
  KnownInferenceCustomerV1EditInsertBeforeN2 :: InferenceV1.Insert -> KnownOneofPayload
  KnownInferenceCustomerV1EditInsertAfterN3 :: InferenceV1.Insert -> KnownOneofPayload
  KnownInferenceCustomerV1EditReplaceN4 :: InferenceV1.Replace -> KnownOneofPayload
  KnownInferenceCustomerV1EditDeleteN5 :: BS.ByteString -> KnownOneofPayload
  KnownInferenceCustomerV1MutateContextRequestEditN3 :: InferenceV1.Edits -> KnownOneofPayload
  KnownInferenceCustomerV1MutateContextRequestForkN4 :: InferenceV1.Empty -> KnownOneofPayload
  KnownInferenceCustomerV1MutateContextRequestTruncateN5 :: InferenceV1.Truncate -> KnownOneofPayload
  KnownInferenceCustomerV1MutateContextRequestCompactN6 :: InferenceV1.Compact -> KnownOneofPayload
  KnownInferenceCustomerV1MutateContextRequestReleaseN7 :: InferenceV1.Empty -> KnownOneofPayload
  KnownInferenceCustomerV1MutateContextRequestTransferN8 :: InferenceV1.Transfer -> KnownOneofPayload
  KnownInferenceCustomerV1RunEventOutputN2 :: BS.ByteString -> KnownOneofPayload
  KnownInferenceCustomerV1RunEventUsageN3 :: InferenceV1.LogicalUsage -> KnownOneofPayload
  KnownInferenceCustomerV1RunEventTerminalN4 :: InferenceV1.RunTerminal -> KnownOneofPayload
  KnownInferenceCustomerV1RunEventProgressN5 :: InferenceV1.RunProgress -> KnownOneofPayload
  KnownAcyclicMachinesV1ImageManagedDigestN2 :: BS.ByteString -> KnownOneofPayload
  KnownAcyclicMachinesV1ImageCustomDigestN3 :: BS.ByteString -> KnownOneofPayload
  KnownAcyclicMachinesV1ImageCheckpointN4 :: MachinesV1.CheckpointId -> KnownOneofPayload
  KnownAcyclicMachinesV1SuspensionPolicyManualN1 :: Bool -> KnownOneofPayload
  KnownAcyclicMachinesV1SuspensionPolicyAfterIdleMsN2 :: Word64 -> KnownOneofPayload
  KnownAcyclicMachinesV1RecoveredAdmissionCreateN2 :: MachinesV1.MachineAdmission -> KnownOneofPayload
  KnownAcyclicMachinesV1RecoveredAdmissionCheckpointN3 :: MachinesV1.CheckpointAdmission -> KnownOneofPayload
  KnownAcyclicMachinesV1RecoveredAdmissionForkN4 :: MachinesV1.ForkAdmission -> KnownOneofPayload
  KnownAcyclicMachinesV1RecoveredAdmissionSuspendN5 :: MachinesV1.MutationAdmission -> KnownOneofPayload
  KnownAcyclicMachinesV1RecoveredAdmissionWakeN6 :: MachinesV1.MutationAdmission -> KnownOneofPayload
  KnownAcyclicMachinesV1RecoveredAdmissionDestroyMachineN7 :: MachinesV1.MutationAdmission -> KnownOneofPayload
  KnownAcyclicMachinesV1RecoveredAdmissionSetSuspensionPolicyN8 :: MachinesV1.PolicyAdmission -> KnownOneofPayload
  KnownAcyclicMachinesV1RecoveredAdmissionDestroyCheckpointN9 :: MachinesV1.MutationAdmission -> KnownOneofPayload
  KnownAcyclicMachinesV1RecoveredAdmissionForkMachineN10 :: MachinesV1.ForkMachineAdmission -> KnownOneofPayload
  KnownAcyclicObjectsV2PreconditionsIfAbsentN1 :: Bool -> KnownOneofPayload
  KnownAcyclicObjectsV2PreconditionsIfMatchN2 :: String -> KnownOneofPayload
  KnownAcyclicObjectsV2UploadPartRequestHeaderN1 :: ObjectsV2.UploadPartHeader -> KnownOneofPayload
  KnownAcyclicObjectsV2UploadPartRequestBodyN2 :: BS.ByteString -> KnownOneofPayload
  KnownAcyclicObjectsV2UploadPartRequestCompleteN3 :: Bool -> KnownOneofPayload
  KnownAcyclicObjectsV2ByteRangeBytesN1 :: ObjectsV2.InclusiveRange -> KnownOneofPayload
  KnownAcyclicObjectsV2ByteRangeSuffixLengthN2 :: Word64 -> KnownOneofPayload
  KnownAcyclicObjectsV2GetObjectResponseHeaderN1 :: ObjectsV2.GetObjectHeader -> KnownOneofPayload
  KnownAcyclicObjectsV2GetObjectResponseBodyN2 :: BS.ByteString -> KnownOneofPayload
  KnownAcyclicObjectsV2GetObjectResponseErrorN3 :: ObjectsV2.ErrorDetail -> KnownOneofPayload
  KnownAcyclicObjectsV2PutObjectRequestHeaderN1 :: ObjectsV2.PutObjectHeader -> KnownOneofPayload
  KnownAcyclicObjectsV2PutObjectRequestBodyN2 :: BS.ByteString -> KnownOneofPayload
  KnownAcyclicObjectsV2PutObjectRequestCompleteN3 :: Bool -> KnownOneofPayload
  KnownAcyclicStreamV2AppendResponseCommittedN1 :: StreamV2.AppendReceipt -> KnownOneofPayload
  KnownAcyclicStreamV2AppendResponseConflictN2 :: StreamV2.TailConflict -> KnownOneofPayload
  KnownAcyclicStreamV2CommitConditionTailN1 :: StreamV2.TailCondition -> KnownOneofPayload
  KnownAcyclicStreamV2CommitConditionAbsentN2 :: StreamV2.AbsentCondition -> KnownOneofPayload
  KnownAcyclicStreamV2CommitConflictTailN1 :: StreamV2.TailCommitConflict -> KnownOneofPayload
  KnownAcyclicStreamV2CommitConflictExistsN2 :: StreamV2.ExistsCommitConflict -> KnownOneofPayload
  KnownAcyclicStreamV2CommitMutationAppendN1 :: StreamV2.AppendMutation -> KnownOneofPayload
  KnownAcyclicStreamV2CommitMutationForkN2 :: StreamV2.ForkMutation -> KnownOneofPayload
  KnownAcyclicStreamV2CommitResponseCommittedN1 :: StreamV2.CommittedEnvelope -> KnownOneofPayload
  KnownAcyclicStreamV2CommitResponseConflictN2 :: StreamV2.CommitConflicts -> KnownOneofPayload
  KnownAcyclicStreamV2CommittedMutationAppendN1 :: StreamV2.CommittedAppend -> KnownOneofPayload
  KnownAcyclicStreamV2CommittedMutationForkN2 :: StreamV2.CommittedFork -> KnownOneofPayload
  KnownAcyclicStreamV2IdempotencyObservationAppendN3 :: StreamV2.AppendResponse -> KnownOneofPayload
  KnownAcyclicStreamV2IdempotencyObservationForkN4 :: StreamV2.ForkReceipt -> KnownOneofPayload
  KnownAcyclicStreamV2IdempotencyObservationCommitN7 :: StreamV2.CommitResponse -> KnownOneofPayload
  KnownAcyclicWorkersV1JobTargetDeploymentAliasN1 :: String -> KnownOneofPayload
  KnownAcyclicWorkersV1JobTargetVersionSha256N2 :: BS.ByteString -> KnownOneofPayload
  KnownAcyclicWorkersV1PayloadInlineBytesN1 :: BS.ByteString -> KnownOneofPayload
  KnownAcyclicWorkersV1PayloadObjectN2 :: WorkersV1.ObjectRef -> KnownOneofPayload
data WireChoice
  = KnownOneof KnownOneofPayload
  | UnknownOneof BS.ByteString

data ActorsActorStateEnum
  = ActorsActorStateACTORSTATEUNSPECIFIED
  | ActorsActorStateACTORSTATEACTIVE
  | ActorsActorStateACTORSTATEHIBERNATED
  | ActorsActorStateACTORSTATEPAUSED
  | ActorsActorStateEnumUnknown Int
  deriving (Eq, Ord, Show)

actorsActorStateEnumFromWire :: Int -> ActorsActorStateEnum
actorsActorStateEnumFromWire value = case value of
  0 -> ActorsActorStateACTORSTATEUNSPECIFIED
  1 -> ActorsActorStateACTORSTATEACTIVE
  2 -> ActorsActorStateACTORSTATEHIBERNATED
  3 -> ActorsActorStateACTORSTATEPAUSED
  _ -> ActorsActorStateEnumUnknown value
actorsActorStateEnumValue :: ActorsActorStateEnum -> Int
actorsActorStateEnumValue value = case value of
  ActorsActorStateACTORSTATEUNSPECIFIED -> 0
  ActorsActorStateACTORSTATEACTIVE -> 1
  ActorsActorStateACTORSTATEHIBERNATED -> 2
  ActorsActorStateACTORSTATEPAUSED -> 3
  ActorsActorStateEnumUnknown number -> number

data ActorsSubscriptionStateEnum
  = ActorsSubscriptionStateSUBSCRIPTIONSTATEUNSPECIFIED
  | ActorsSubscriptionStateSUBSCRIPTIONSTATEACTIVE
  | ActorsSubscriptionStateSUBSCRIPTIONSTATEPAUSED
  | ActorsSubscriptionStateEnumUnknown Int
  deriving (Eq, Ord, Show)

actorsSubscriptionStateEnumFromWire :: Int -> ActorsSubscriptionStateEnum
actorsSubscriptionStateEnumFromWire value = case value of
  0 -> ActorsSubscriptionStateSUBSCRIPTIONSTATEUNSPECIFIED
  1 -> ActorsSubscriptionStateSUBSCRIPTIONSTATEACTIVE
  2 -> ActorsSubscriptionStateSUBSCRIPTIONSTATEPAUSED
  _ -> ActorsSubscriptionStateEnumUnknown value
actorsSubscriptionStateEnumValue :: ActorsSubscriptionStateEnum -> Int
actorsSubscriptionStateEnumValue value = case value of
  ActorsSubscriptionStateSUBSCRIPTIONSTATEUNSPECIFIED -> 0
  ActorsSubscriptionStateSUBSCRIPTIONSTATEACTIVE -> 1
  ActorsSubscriptionStateSUBSCRIPTIONSTATEPAUSED -> 2
  ActorsSubscriptionStateEnumUnknown number -> number

data FilesystemConflictUseEnum
  = FilesystemConflictUseCONFLICTUSEUNSPECIFIED
  | FilesystemConflictUseCONFLICTUSEOBSERVATION
  | FilesystemConflictUseCONFLICTUSEMUTATION
  | FilesystemConflictUseCONFLICTUSEOBSERVATIONANDMUTATION
  | FilesystemConflictUseEnumUnknown Int
  deriving (Eq, Ord, Show)

filesystemConflictUseEnumFromWire :: Int -> FilesystemConflictUseEnum
filesystemConflictUseEnumFromWire value = case value of
  0 -> FilesystemConflictUseCONFLICTUSEUNSPECIFIED
  1 -> FilesystemConflictUseCONFLICTUSEOBSERVATION
  2 -> FilesystemConflictUseCONFLICTUSEMUTATION
  3 -> FilesystemConflictUseCONFLICTUSEOBSERVATIONANDMUTATION
  _ -> FilesystemConflictUseEnumUnknown value
filesystemConflictUseEnumValue :: FilesystemConflictUseEnum -> Int
filesystemConflictUseEnumValue value = case value of
  FilesystemConflictUseCONFLICTUSEUNSPECIFIED -> 0
  FilesystemConflictUseCONFLICTUSEOBSERVATION -> 1
  FilesystemConflictUseCONFLICTUSEMUTATION -> 2
  FilesystemConflictUseCONFLICTUSEOBSERVATIONANDMUTATION -> 3
  FilesystemConflictUseEnumUnknown number -> number

data FilesystemExtentKindEnum
  = FilesystemExtentKindEXTENTKINDUNSPECIFIED
  | FilesystemExtentKindEXTENTKINDHOLE
  | FilesystemExtentKindEXTENTKINDALLOCATEDZERO
  | FilesystemExtentKindEXTENTKINDCONTENT
  | FilesystemExtentKindEnumUnknown Int
  deriving (Eq, Ord, Show)

filesystemExtentKindEnumFromWire :: Int -> FilesystemExtentKindEnum
filesystemExtentKindEnumFromWire value = case value of
  0 -> FilesystemExtentKindEXTENTKINDUNSPECIFIED
  1 -> FilesystemExtentKindEXTENTKINDHOLE
  2 -> FilesystemExtentKindEXTENTKINDALLOCATEDZERO
  3 -> FilesystemExtentKindEXTENTKINDCONTENT
  _ -> FilesystemExtentKindEnumUnknown value
filesystemExtentKindEnumValue :: FilesystemExtentKindEnum -> Int
filesystemExtentKindEnumValue value = case value of
  FilesystemExtentKindEXTENTKINDUNSPECIFIED -> 0
  FilesystemExtentKindEXTENTKINDHOLE -> 1
  FilesystemExtentKindEXTENTKINDALLOCATEDZERO -> 2
  FilesystemExtentKindEXTENTKINDCONTENT -> 3
  FilesystemExtentKindEnumUnknown number -> number

data FilesystemFileKindEnum
  = FilesystemFileKindFILEKINDUNSPECIFIED
  | FilesystemFileKindFILEKINDREGULAR
  | FilesystemFileKindFILEKINDDIRECTORY
  | FilesystemFileKindFILEKINDSYMBOLICLINK
  | FilesystemFileKindFILEKINDFIFO
  | FilesystemFileKindFILEKINDSOCKET
  | FilesystemFileKindFILEKINDCHARACTERDEVICE
  | FilesystemFileKindFILEKINDBLOCKDEVICE
  | FilesystemFileKindFILEKINDREPARSEPOINT
  | FilesystemFileKindFILEKINDMOUNTBOUNDARY
  | FilesystemFileKindEnumUnknown Int
  deriving (Eq, Ord, Show)

filesystemFileKindEnumFromWire :: Int -> FilesystemFileKindEnum
filesystemFileKindEnumFromWire value = case value of
  0 -> FilesystemFileKindFILEKINDUNSPECIFIED
  1 -> FilesystemFileKindFILEKINDREGULAR
  2 -> FilesystemFileKindFILEKINDDIRECTORY
  3 -> FilesystemFileKindFILEKINDSYMBOLICLINK
  4 -> FilesystemFileKindFILEKINDFIFO
  5 -> FilesystemFileKindFILEKINDSOCKET
  6 -> FilesystemFileKindFILEKINDCHARACTERDEVICE
  7 -> FilesystemFileKindFILEKINDBLOCKDEVICE
  8 -> FilesystemFileKindFILEKINDREPARSEPOINT
  9 -> FilesystemFileKindFILEKINDMOUNTBOUNDARY
  _ -> FilesystemFileKindEnumUnknown value
filesystemFileKindEnumValue :: FilesystemFileKindEnum -> Int
filesystemFileKindEnumValue value = case value of
  FilesystemFileKindFILEKINDUNSPECIFIED -> 0
  FilesystemFileKindFILEKINDREGULAR -> 1
  FilesystemFileKindFILEKINDDIRECTORY -> 2
  FilesystemFileKindFILEKINDSYMBOLICLINK -> 3
  FilesystemFileKindFILEKINDFIFO -> 4
  FilesystemFileKindFILEKINDSOCKET -> 5
  FilesystemFileKindFILEKINDCHARACTERDEVICE -> 6
  FilesystemFileKindFILEKINDBLOCKDEVICE -> 7
  FilesystemFileKindFILEKINDREPARSEPOINT -> 8
  FilesystemFileKindFILEKINDMOUNTBOUNDARY -> 9
  FilesystemFileKindEnumUnknown number -> number

data FilesystemFilesystemProfileEnum
  = FilesystemFilesystemProfileFILESYSTEMPROFILEUNSPECIFIED
  | FilesystemFilesystemProfileFILESYSTEMPROFILEPORTABLE
  | FilesystemFilesystemProfileFILESYSTEMPROFILEPOSIX
  | FilesystemFilesystemProfileFILESYSTEMPROFILEWINDOWS
  | FilesystemFilesystemProfileFILESYSTEMPROFILEBROWSER
  | FilesystemFilesystemProfileEnumUnknown Int
  deriving (Eq, Ord, Show)

filesystemFilesystemProfileEnumFromWire :: Int -> FilesystemFilesystemProfileEnum
filesystemFilesystemProfileEnumFromWire value = case value of
  0 -> FilesystemFilesystemProfileFILESYSTEMPROFILEUNSPECIFIED
  1 -> FilesystemFilesystemProfileFILESYSTEMPROFILEPORTABLE
  2 -> FilesystemFilesystemProfileFILESYSTEMPROFILEPOSIX
  3 -> FilesystemFilesystemProfileFILESYSTEMPROFILEWINDOWS
  4 -> FilesystemFilesystemProfileFILESYSTEMPROFILEBROWSER
  _ -> FilesystemFilesystemProfileEnumUnknown value
filesystemFilesystemProfileEnumValue :: FilesystemFilesystemProfileEnum -> Int
filesystemFilesystemProfileEnumValue value = case value of
  FilesystemFilesystemProfileFILESYSTEMPROFILEUNSPECIFIED -> 0
  FilesystemFilesystemProfileFILESYSTEMPROFILEPORTABLE -> 1
  FilesystemFilesystemProfileFILESYSTEMPROFILEPOSIX -> 2
  FilesystemFilesystemProfileFILESYSTEMPROFILEWINDOWS -> 3
  FilesystemFilesystemProfileFILESYSTEMPROFILEBROWSER -> 4
  FilesystemFilesystemProfileEnumUnknown number -> number

data FilesystemJoinHistoryEnum
  = FilesystemJoinHistoryJOINHISTORYUNSPECIFIED
  | FilesystemJoinHistoryJOINHISTORYMERGE
  | FilesystemJoinHistoryJOINHISTORYREBASE
  | FilesystemJoinHistoryJOINHISTORYSQUASH
  | FilesystemJoinHistoryJOINHISTORYCHERRYPICK
  | FilesystemJoinHistoryEnumUnknown Int
  deriving (Eq, Ord, Show)

filesystemJoinHistoryEnumFromWire :: Int -> FilesystemJoinHistoryEnum
filesystemJoinHistoryEnumFromWire value = case value of
  0 -> FilesystemJoinHistoryJOINHISTORYUNSPECIFIED
  1 -> FilesystemJoinHistoryJOINHISTORYMERGE
  2 -> FilesystemJoinHistoryJOINHISTORYREBASE
  3 -> FilesystemJoinHistoryJOINHISTORYSQUASH
  4 -> FilesystemJoinHistoryJOINHISTORYCHERRYPICK
  _ -> FilesystemJoinHistoryEnumUnknown value
filesystemJoinHistoryEnumValue :: FilesystemJoinHistoryEnum -> Int
filesystemJoinHistoryEnumValue value = case value of
  FilesystemJoinHistoryJOINHISTORYUNSPECIFIED -> 0
  FilesystemJoinHistoryJOINHISTORYMERGE -> 1
  FilesystemJoinHistoryJOINHISTORYREBASE -> 2
  FilesystemJoinHistoryJOINHISTORYSQUASH -> 3
  FilesystemJoinHistoryJOINHISTORYCHERRYPICK -> 4
  FilesystemJoinHistoryEnumUnknown number -> number

data FilesystemJoinStatusEnum
  = FilesystemJoinStatusJOINSTATUSUNSPECIFIED
  | FilesystemJoinStatusJOINSTATUSAPPLIED
  | FilesystemJoinStatusJOINSTATUSALREADYAPPLIED
  | FilesystemJoinStatusJOINSTATUSNOCHANGES
  | FilesystemJoinStatusJOINSTATUSSTALETARGET
  | FilesystemJoinStatusJOINSTATUSCONFLICTED
  | FilesystemJoinStatusJOINSTATUSFENCED
  | FilesystemJoinStatusJOINSTATUSIDEMPOTENCYCONFLICT
  | FilesystemJoinStatusEnumUnknown Int
  deriving (Eq, Ord, Show)

filesystemJoinStatusEnumFromWire :: Int -> FilesystemJoinStatusEnum
filesystemJoinStatusEnumFromWire value = case value of
  0 -> FilesystemJoinStatusJOINSTATUSUNSPECIFIED
  1 -> FilesystemJoinStatusJOINSTATUSAPPLIED
  2 -> FilesystemJoinStatusJOINSTATUSALREADYAPPLIED
  3 -> FilesystemJoinStatusJOINSTATUSNOCHANGES
  4 -> FilesystemJoinStatusJOINSTATUSSTALETARGET
  5 -> FilesystemJoinStatusJOINSTATUSCONFLICTED
  6 -> FilesystemJoinStatusJOINSTATUSFENCED
  7 -> FilesystemJoinStatusJOINSTATUSIDEMPOTENCYCONFLICT
  _ -> FilesystemJoinStatusEnumUnknown value
filesystemJoinStatusEnumValue :: FilesystemJoinStatusEnum -> Int
filesystemJoinStatusEnumValue value = case value of
  FilesystemJoinStatusJOINSTATUSUNSPECIFIED -> 0
  FilesystemJoinStatusJOINSTATUSAPPLIED -> 1
  FilesystemJoinStatusJOINSTATUSALREADYAPPLIED -> 2
  FilesystemJoinStatusJOINSTATUSNOCHANGES -> 3
  FilesystemJoinStatusJOINSTATUSSTALETARGET -> 4
  FilesystemJoinStatusJOINSTATUSCONFLICTED -> 5
  FilesystemJoinStatusJOINSTATUSFENCED -> 6
  FilesystemJoinStatusJOINSTATUSIDEMPOTENCYCONFLICT -> 7
  FilesystemJoinStatusEnumUnknown number -> number

data FilesystemMutationStatusEnum
  = FilesystemMutationStatusMUTATIONSTATUSUNSPECIFIED
  | FilesystemMutationStatusMUTATIONSTATUSCOMMITTED
  | FilesystemMutationStatusMUTATIONSTATUSALREADYCOMMITTED
  | FilesystemMutationStatusMUTATIONSTATUSCONFLICT
  | FilesystemMutationStatusMUTATIONSTATUSFENCED
  | FilesystemMutationStatusMUTATIONSTATUSIDEMPOTENCYCONFLICT
  | FilesystemMutationStatusMUTATIONSTATUSINDETERMINATE
  | FilesystemMutationStatusEnumUnknown Int
  deriving (Eq, Ord, Show)

filesystemMutationStatusEnumFromWire :: Int -> FilesystemMutationStatusEnum
filesystemMutationStatusEnumFromWire value = case value of
  0 -> FilesystemMutationStatusMUTATIONSTATUSUNSPECIFIED
  1 -> FilesystemMutationStatusMUTATIONSTATUSCOMMITTED
  2 -> FilesystemMutationStatusMUTATIONSTATUSALREADYCOMMITTED
  3 -> FilesystemMutationStatusMUTATIONSTATUSCONFLICT
  4 -> FilesystemMutationStatusMUTATIONSTATUSFENCED
  5 -> FilesystemMutationStatusMUTATIONSTATUSIDEMPOTENCYCONFLICT
  6 -> FilesystemMutationStatusMUTATIONSTATUSINDETERMINATE
  _ -> FilesystemMutationStatusEnumUnknown value
filesystemMutationStatusEnumValue :: FilesystemMutationStatusEnum -> Int
filesystemMutationStatusEnumValue value = case value of
  FilesystemMutationStatusMUTATIONSTATUSUNSPECIFIED -> 0
  FilesystemMutationStatusMUTATIONSTATUSCOMMITTED -> 1
  FilesystemMutationStatusMUTATIONSTATUSALREADYCOMMITTED -> 2
  FilesystemMutationStatusMUTATIONSTATUSCONFLICT -> 3
  FilesystemMutationStatusMUTATIONSTATUSFENCED -> 4
  FilesystemMutationStatusMUTATIONSTATUSIDEMPOTENCYCONFLICT -> 5
  FilesystemMutationStatusMUTATIONSTATUSINDETERMINATE -> 6
  FilesystemMutationStatusEnumUnknown number -> number

data FilesystemNameEncodingEnum
  = FilesystemNameEncodingNAMEENCODINGUNSPECIFIED
  | FilesystemNameEncodingNAMEENCODINGUTF8
  | FilesystemNameEncodingNAMEENCODINGPOSIXBYTES
  | FilesystemNameEncodingNAMEENCODINGWINDOWSUTF16LE
  | FilesystemNameEncodingEnumUnknown Int
  deriving (Eq, Ord, Show)

filesystemNameEncodingEnumFromWire :: Int -> FilesystemNameEncodingEnum
filesystemNameEncodingEnumFromWire value = case value of
  0 -> FilesystemNameEncodingNAMEENCODINGUNSPECIFIED
  1 -> FilesystemNameEncodingNAMEENCODINGUTF8
  2 -> FilesystemNameEncodingNAMEENCODINGPOSIXBYTES
  3 -> FilesystemNameEncodingNAMEENCODINGWINDOWSUTF16LE
  _ -> FilesystemNameEncodingEnumUnknown value
filesystemNameEncodingEnumValue :: FilesystemNameEncodingEnum -> Int
filesystemNameEncodingEnumValue value = case value of
  FilesystemNameEncodingNAMEENCODINGUNSPECIFIED -> 0
  FilesystemNameEncodingNAMEENCODINGUTF8 -> 1
  FilesystemNameEncodingNAMEENCODINGPOSIXBYTES -> 2
  FilesystemNameEncodingNAMEENCODINGWINDOWSUTF16LE -> 3
  FilesystemNameEncodingEnumUnknown number -> number

data FilesystemRebaseStatusEnum
  = FilesystemRebaseStatusREBASESTATUSUNSPECIFIED
  | FilesystemRebaseStatusREBASESTATUSREBASED
  | FilesystemRebaseStatusREBASESTATUSALREADYREBASED
  | FilesystemRebaseStatusREBASESTATUSCURRENT
  | FilesystemRebaseStatusREBASESTATUSSTALE
  | FilesystemRebaseStatusREBASESTATUSCONFLICTED
  | FilesystemRebaseStatusREBASESTATUSFENCED
  | FilesystemRebaseStatusREBASESTATUSIDEMPOTENCYCONFLICT
  | FilesystemRebaseStatusEnumUnknown Int
  deriving (Eq, Ord, Show)

filesystemRebaseStatusEnumFromWire :: Int -> FilesystemRebaseStatusEnum
filesystemRebaseStatusEnumFromWire value = case value of
  0 -> FilesystemRebaseStatusREBASESTATUSUNSPECIFIED
  1 -> FilesystemRebaseStatusREBASESTATUSREBASED
  2 -> FilesystemRebaseStatusREBASESTATUSALREADYREBASED
  3 -> FilesystemRebaseStatusREBASESTATUSCURRENT
  4 -> FilesystemRebaseStatusREBASESTATUSSTALE
  5 -> FilesystemRebaseStatusREBASESTATUSCONFLICTED
  6 -> FilesystemRebaseStatusREBASESTATUSFENCED
  7 -> FilesystemRebaseStatusREBASESTATUSIDEMPOTENCYCONFLICT
  _ -> FilesystemRebaseStatusEnumUnknown value
filesystemRebaseStatusEnumValue :: FilesystemRebaseStatusEnum -> Int
filesystemRebaseStatusEnumValue value = case value of
  FilesystemRebaseStatusREBASESTATUSUNSPECIFIED -> 0
  FilesystemRebaseStatusREBASESTATUSREBASED -> 1
  FilesystemRebaseStatusREBASESTATUSALREADYREBASED -> 2
  FilesystemRebaseStatusREBASESTATUSCURRENT -> 3
  FilesystemRebaseStatusREBASESTATUSSTALE -> 4
  FilesystemRebaseStatusREBASESTATUSCONFLICTED -> 5
  FilesystemRebaseStatusREBASESTATUSFENCED -> 6
  FilesystemRebaseStatusREBASESTATUSIDEMPOTENCYCONFLICT -> 7
  FilesystemRebaseStatusEnumUnknown number -> number

data FilesystemSourceInvalidationReasonEnum
  = FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONUNSPECIFIED
  | FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONINITIALSNAPSHOTREQUIRED
  | FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONQUEUEOVERFLOW
  | FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONNATIVERESCANREQUIRED
  | FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONBACKENDERROR
  | FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONUNREPRESENTABLEPATH
  | FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONAMBIGUOUSRENAME
  | FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONROOTCHANGED
  | FilesystemSourceInvalidationReasonEnumUnknown Int
  deriving (Eq, Ord, Show)

filesystemSourceInvalidationReasonEnumFromWire :: Int -> FilesystemSourceInvalidationReasonEnum
filesystemSourceInvalidationReasonEnumFromWire value = case value of
  0 -> FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONUNSPECIFIED
  1 -> FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONINITIALSNAPSHOTREQUIRED
  2 -> FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONQUEUEOVERFLOW
  3 -> FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONNATIVERESCANREQUIRED
  4 -> FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONBACKENDERROR
  5 -> FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONUNREPRESENTABLEPATH
  6 -> FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONAMBIGUOUSRENAME
  7 -> FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONROOTCHANGED
  _ -> FilesystemSourceInvalidationReasonEnumUnknown value
filesystemSourceInvalidationReasonEnumValue :: FilesystemSourceInvalidationReasonEnum -> Int
filesystemSourceInvalidationReasonEnumValue value = case value of
  FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONUNSPECIFIED -> 0
  FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONINITIALSNAPSHOTREQUIRED -> 1
  FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONQUEUEOVERFLOW -> 2
  FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONNATIVERESCANREQUIRED -> 3
  FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONBACKENDERROR -> 4
  FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONUNREPRESENTABLEPATH -> 5
  FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONAMBIGUOUSRENAME -> 6
  FilesystemSourceInvalidationReasonSOURCEINVALIDATIONREASONROOTCHANGED -> 7
  FilesystemSourceInvalidationReasonEnumUnknown number -> number

data FilesystemSourceStateEnum
  = FilesystemSourceStateSOURCESTATEUNSPECIFIED
  | FilesystemSourceStateSOURCESTATECLEAN
  | FilesystemSourceStateSOURCESTATEPENDINGCAPTURE
  | FilesystemSourceStateSOURCESTATENEEDSRESCAN
  | FilesystemSourceStateSOURCESTATECONFLICT
  | FilesystemSourceStateSOURCESTATESEALED
  | FilesystemSourceStateEnumUnknown Int
  deriving (Eq, Ord, Show)

filesystemSourceStateEnumFromWire :: Int -> FilesystemSourceStateEnum
filesystemSourceStateEnumFromWire value = case value of
  0 -> FilesystemSourceStateSOURCESTATEUNSPECIFIED
  1 -> FilesystemSourceStateSOURCESTATECLEAN
  2 -> FilesystemSourceStateSOURCESTATEPENDINGCAPTURE
  3 -> FilesystemSourceStateSOURCESTATENEEDSRESCAN
  4 -> FilesystemSourceStateSOURCESTATECONFLICT
  5 -> FilesystemSourceStateSOURCESTATESEALED
  _ -> FilesystemSourceStateEnumUnknown value
filesystemSourceStateEnumValue :: FilesystemSourceStateEnum -> Int
filesystemSourceStateEnumValue value = case value of
  FilesystemSourceStateSOURCESTATEUNSPECIFIED -> 0
  FilesystemSourceStateSOURCESTATECLEAN -> 1
  FilesystemSourceStateSOURCESTATEPENDINGCAPTURE -> 2
  FilesystemSourceStateSOURCESTATENEEDSRESCAN -> 3
  FilesystemSourceStateSOURCESTATECONFLICT -> 4
  FilesystemSourceStateSOURCESTATESEALED -> 5
  FilesystemSourceStateEnumUnknown number -> number

data FilesystemSparseTargetEnum
  = FilesystemSparseTargetSPARSETARGETUNSPECIFIED
  | FilesystemSparseTargetSPARSETARGETDATA
  | FilesystemSparseTargetSPARSETARGETHOLE
  | FilesystemSparseTargetEnumUnknown Int
  deriving (Eq, Ord, Show)

filesystemSparseTargetEnumFromWire :: Int -> FilesystemSparseTargetEnum
filesystemSparseTargetEnumFromWire value = case value of
  0 -> FilesystemSparseTargetSPARSETARGETUNSPECIFIED
  1 -> FilesystemSparseTargetSPARSETARGETDATA
  2 -> FilesystemSparseTargetSPARSETARGETHOLE
  _ -> FilesystemSparseTargetEnumUnknown value
filesystemSparseTargetEnumValue :: FilesystemSparseTargetEnum -> Int
filesystemSparseTargetEnumValue value = case value of
  FilesystemSparseTargetSPARSETARGETUNSPECIFIED -> 0
  FilesystemSparseTargetSPARSETARGETDATA -> 1
  FilesystemSparseTargetSPARSETARGETHOLE -> 2
  FilesystemSparseTargetEnumUnknown number -> number

data HarnessAdmissionStateEnum
  = HarnessAdmissionStateADMISSIONSTATEUNSPECIFIED
  | HarnessAdmissionStateADMISSIONSTATEACCEPTED
  | HarnessAdmissionStateADMISSIONSTATEREJECTED
  | HarnessAdmissionStateADMISSIONSTATEINDETERMINATE
  | HarnessAdmissionStateEnumUnknown Int
  deriving (Eq, Ord, Show)

harnessAdmissionStateEnumFromWire :: Int -> HarnessAdmissionStateEnum
harnessAdmissionStateEnumFromWire value = case value of
  0 -> HarnessAdmissionStateADMISSIONSTATEUNSPECIFIED
  1 -> HarnessAdmissionStateADMISSIONSTATEACCEPTED
  2 -> HarnessAdmissionStateADMISSIONSTATEREJECTED
  3 -> HarnessAdmissionStateADMISSIONSTATEINDETERMINATE
  _ -> HarnessAdmissionStateEnumUnknown value
harnessAdmissionStateEnumValue :: HarnessAdmissionStateEnum -> Int
harnessAdmissionStateEnumValue value = case value of
  HarnessAdmissionStateADMISSIONSTATEUNSPECIFIED -> 0
  HarnessAdmissionStateADMISSIONSTATEACCEPTED -> 1
  HarnessAdmissionStateADMISSIONSTATEREJECTED -> 2
  HarnessAdmissionStateADMISSIONSTATEINDETERMINATE -> 3
  HarnessAdmissionStateEnumUnknown number -> number

data HarnessAggregateKindEnum
  = HarnessAggregateKindAGGREGATEKINDUNSPECIFIED
  | HarnessAggregateKindAGGREGATEKINDAGENT
  | HarnessAggregateKindAGGREGATEKINDCONVERSATION
  | HarnessAggregateKindAGGREGATEKINDSESSION
  | HarnessAggregateKindAGGREGATEKINDTURN
  | HarnessAggregateKindAGGREGATEKINDTASK
  | HarnessAggregateKindEnumUnknown Int
  deriving (Eq, Ord, Show)

harnessAggregateKindEnumFromWire :: Int -> HarnessAggregateKindEnum
harnessAggregateKindEnumFromWire value = case value of
  0 -> HarnessAggregateKindAGGREGATEKINDUNSPECIFIED
  1 -> HarnessAggregateKindAGGREGATEKINDAGENT
  2 -> HarnessAggregateKindAGGREGATEKINDCONVERSATION
  3 -> HarnessAggregateKindAGGREGATEKINDSESSION
  4 -> HarnessAggregateKindAGGREGATEKINDTURN
  5 -> HarnessAggregateKindAGGREGATEKINDTASK
  _ -> HarnessAggregateKindEnumUnknown value
harnessAggregateKindEnumValue :: HarnessAggregateKindEnum -> Int
harnessAggregateKindEnumValue value = case value of
  HarnessAggregateKindAGGREGATEKINDUNSPECIFIED -> 0
  HarnessAggregateKindAGGREGATEKINDAGENT -> 1
  HarnessAggregateKindAGGREGATEKINDCONVERSATION -> 2
  HarnessAggregateKindAGGREGATEKINDSESSION -> 3
  HarnessAggregateKindAGGREGATEKINDTURN -> 4
  HarnessAggregateKindAGGREGATEKINDTASK -> 5
  HarnessAggregateKindEnumUnknown number -> number

data HarnessCompletionStateEnum
  = HarnessCompletionStateCOMPLETIONSTATEUNSPECIFIED
  | HarnessCompletionStateCOMPLETIONSTATERUNNING
  | HarnessCompletionStateCOMPLETIONSTATESUCCEEDED
  | HarnessCompletionStateCOMPLETIONSTATEFAILED
  | HarnessCompletionStateCOMPLETIONSTATECANCELLED
  | HarnessCompletionStateCOMPLETIONSTATEINDETERMINATE
  | HarnessCompletionStateEnumUnknown Int
  deriving (Eq, Ord, Show)

harnessCompletionStateEnumFromWire :: Int -> HarnessCompletionStateEnum
harnessCompletionStateEnumFromWire value = case value of
  0 -> HarnessCompletionStateCOMPLETIONSTATEUNSPECIFIED
  1 -> HarnessCompletionStateCOMPLETIONSTATERUNNING
  2 -> HarnessCompletionStateCOMPLETIONSTATESUCCEEDED
  3 -> HarnessCompletionStateCOMPLETIONSTATEFAILED
  4 -> HarnessCompletionStateCOMPLETIONSTATECANCELLED
  5 -> HarnessCompletionStateCOMPLETIONSTATEINDETERMINATE
  _ -> HarnessCompletionStateEnumUnknown value
harnessCompletionStateEnumValue :: HarnessCompletionStateEnum -> Int
harnessCompletionStateEnumValue value = case value of
  HarnessCompletionStateCOMPLETIONSTATEUNSPECIFIED -> 0
  HarnessCompletionStateCOMPLETIONSTATERUNNING -> 1
  HarnessCompletionStateCOMPLETIONSTATESUCCEEDED -> 2
  HarnessCompletionStateCOMPLETIONSTATEFAILED -> 3
  HarnessCompletionStateCOMPLETIONSTATECANCELLED -> 4
  HarnessCompletionStateCOMPLETIONSTATEINDETERMINATE -> 5
  HarnessCompletionStateEnumUnknown number -> number

data HarnessErrorCodeEnum
  = HarnessErrorCodeERRORCODEUNSPECIFIED
  | HarnessErrorCodeERRORCODENOTFOUND
  | HarnessErrorCodeERRORCODECONFLICT
  | HarnessErrorCodeERRORCODEUNSUPPORTED
  | HarnessErrorCodeERRORCODEINVALID
  | HarnessErrorCodeERRORCODEUNAUTHORIZED
  | HarnessErrorCodeERRORCODESTORAGE
  | HarnessErrorCodeERRORCODEINDETERMINATE
  | HarnessErrorCodeERRORCODEINTERACTIONDECLINED
  | HarnessErrorCodeERRORCODEINTERACTIONCANCELLED
  | HarnessErrorCodeERRORCODEINTERACTIONEXPIRED
  | HarnessErrorCodeERRORCODEINTERACTIONDENIED
  | HarnessErrorCodeEnumUnknown Int
  deriving (Eq, Ord, Show)

harnessErrorCodeEnumFromWire :: Int -> HarnessErrorCodeEnum
harnessErrorCodeEnumFromWire value = case value of
  0 -> HarnessErrorCodeERRORCODEUNSPECIFIED
  1 -> HarnessErrorCodeERRORCODENOTFOUND
  2 -> HarnessErrorCodeERRORCODECONFLICT
  3 -> HarnessErrorCodeERRORCODEUNSUPPORTED
  4 -> HarnessErrorCodeERRORCODEINVALID
  5 -> HarnessErrorCodeERRORCODEUNAUTHORIZED
  6 -> HarnessErrorCodeERRORCODESTORAGE
  7 -> HarnessErrorCodeERRORCODEINDETERMINATE
  8 -> HarnessErrorCodeERRORCODEINTERACTIONDECLINED
  9 -> HarnessErrorCodeERRORCODEINTERACTIONCANCELLED
  10 -> HarnessErrorCodeERRORCODEINTERACTIONEXPIRED
  11 -> HarnessErrorCodeERRORCODEINTERACTIONDENIED
  _ -> HarnessErrorCodeEnumUnknown value
harnessErrorCodeEnumValue :: HarnessErrorCodeEnum -> Int
harnessErrorCodeEnumValue value = case value of
  HarnessErrorCodeERRORCODEUNSPECIFIED -> 0
  HarnessErrorCodeERRORCODENOTFOUND -> 1
  HarnessErrorCodeERRORCODECONFLICT -> 2
  HarnessErrorCodeERRORCODEUNSUPPORTED -> 3
  HarnessErrorCodeERRORCODEINVALID -> 4
  HarnessErrorCodeERRORCODEUNAUTHORIZED -> 5
  HarnessErrorCodeERRORCODESTORAGE -> 6
  HarnessErrorCodeERRORCODEINDETERMINATE -> 7
  HarnessErrorCodeERRORCODEINTERACTIONDECLINED -> 8
  HarnessErrorCodeERRORCODEINTERACTIONCANCELLED -> 9
  HarnessErrorCodeERRORCODEINTERACTIONEXPIRED -> 10
  HarnessErrorCodeERRORCODEINTERACTIONDENIED -> 11
  HarnessErrorCodeEnumUnknown number -> number

data InferenceCustomerEvaluationAggregationEnum
  = InferenceCustomerEvaluationAggregationEVALUATIONAGGREGATIONUNSPECIFIED
  | InferenceCustomerEvaluationAggregationEVALUATIONAGGREGATIONMEAN
  | InferenceCustomerEvaluationAggregationEVALUATIONAGGREGATIONSUM
  | InferenceCustomerEvaluationAggregationEVALUATIONAGGREGATIONMINIMUM
  | InferenceCustomerEvaluationAggregationEVALUATIONAGGREGATIONMAXIMUM
  | InferenceCustomerEvaluationAggregationEnumUnknown Int
  deriving (Eq, Ord, Show)

inferenceCustomerEvaluationAggregationEnumFromWire :: Int -> InferenceCustomerEvaluationAggregationEnum
inferenceCustomerEvaluationAggregationEnumFromWire value = case value of
  0 -> InferenceCustomerEvaluationAggregationEVALUATIONAGGREGATIONUNSPECIFIED
  1 -> InferenceCustomerEvaluationAggregationEVALUATIONAGGREGATIONMEAN
  2 -> InferenceCustomerEvaluationAggregationEVALUATIONAGGREGATIONSUM
  3 -> InferenceCustomerEvaluationAggregationEVALUATIONAGGREGATIONMINIMUM
  4 -> InferenceCustomerEvaluationAggregationEVALUATIONAGGREGATIONMAXIMUM
  _ -> InferenceCustomerEvaluationAggregationEnumUnknown value
inferenceCustomerEvaluationAggregationEnumValue :: InferenceCustomerEvaluationAggregationEnum -> Int
inferenceCustomerEvaluationAggregationEnumValue value = case value of
  InferenceCustomerEvaluationAggregationEVALUATIONAGGREGATIONUNSPECIFIED -> 0
  InferenceCustomerEvaluationAggregationEVALUATIONAGGREGATIONMEAN -> 1
  InferenceCustomerEvaluationAggregationEVALUATIONAGGREGATIONSUM -> 2
  InferenceCustomerEvaluationAggregationEVALUATIONAGGREGATIONMINIMUM -> 3
  InferenceCustomerEvaluationAggregationEVALUATIONAGGREGATIONMAXIMUM -> 4
  InferenceCustomerEvaluationAggregationEnumUnknown number -> number

data InferenceCustomerEvaluationCaseOutcomeEnum
  = InferenceCustomerEvaluationCaseOutcomeEVALUATIONCASEOUTCOMEUNSPECIFIED
  | InferenceCustomerEvaluationCaseOutcomeEVALUATIONCASEOUTCOMESCORED
  | InferenceCustomerEvaluationCaseOutcomeEVALUATIONCASEOUTCOMECANDIDATEFAILED
  | InferenceCustomerEvaluationCaseOutcomeEVALUATIONCASEOUTCOMEGRADERFAILED
  | InferenceCustomerEvaluationCaseOutcomeEnumUnknown Int
  deriving (Eq, Ord, Show)

inferenceCustomerEvaluationCaseOutcomeEnumFromWire :: Int -> InferenceCustomerEvaluationCaseOutcomeEnum
inferenceCustomerEvaluationCaseOutcomeEnumFromWire value = case value of
  0 -> InferenceCustomerEvaluationCaseOutcomeEVALUATIONCASEOUTCOMEUNSPECIFIED
  1 -> InferenceCustomerEvaluationCaseOutcomeEVALUATIONCASEOUTCOMESCORED
  2 -> InferenceCustomerEvaluationCaseOutcomeEVALUATIONCASEOUTCOMECANDIDATEFAILED
  3 -> InferenceCustomerEvaluationCaseOutcomeEVALUATIONCASEOUTCOMEGRADERFAILED
  _ -> InferenceCustomerEvaluationCaseOutcomeEnumUnknown value
inferenceCustomerEvaluationCaseOutcomeEnumValue :: InferenceCustomerEvaluationCaseOutcomeEnum -> Int
inferenceCustomerEvaluationCaseOutcomeEnumValue value = case value of
  InferenceCustomerEvaluationCaseOutcomeEVALUATIONCASEOUTCOMEUNSPECIFIED -> 0
  InferenceCustomerEvaluationCaseOutcomeEVALUATIONCASEOUTCOMESCORED -> 1
  InferenceCustomerEvaluationCaseOutcomeEVALUATIONCASEOUTCOMECANDIDATEFAILED -> 2
  InferenceCustomerEvaluationCaseOutcomeEVALUATIONCASEOUTCOMEGRADERFAILED -> 3
  InferenceCustomerEvaluationCaseOutcomeEnumUnknown number -> number

data InferenceCustomerEvaluationStateEnum
  = InferenceCustomerEvaluationStateEVALUATIONSTATEUNSPECIFIED
  | InferenceCustomerEvaluationStateEVALUATIONSTATEADMITTED
  | InferenceCustomerEvaluationStateEVALUATIONSTATERUNNING
  | InferenceCustomerEvaluationStateEVALUATIONSTATECOMPLETED
  | InferenceCustomerEvaluationStateEVALUATIONSTATEFAILED
  | InferenceCustomerEvaluationStateEVALUATIONSTATECANCELLED
  | InferenceCustomerEvaluationStateEnumUnknown Int
  deriving (Eq, Ord, Show)

inferenceCustomerEvaluationStateEnumFromWire :: Int -> InferenceCustomerEvaluationStateEnum
inferenceCustomerEvaluationStateEnumFromWire value = case value of
  0 -> InferenceCustomerEvaluationStateEVALUATIONSTATEUNSPECIFIED
  1 -> InferenceCustomerEvaluationStateEVALUATIONSTATEADMITTED
  2 -> InferenceCustomerEvaluationStateEVALUATIONSTATERUNNING
  3 -> InferenceCustomerEvaluationStateEVALUATIONSTATECOMPLETED
  4 -> InferenceCustomerEvaluationStateEVALUATIONSTATEFAILED
  5 -> InferenceCustomerEvaluationStateEVALUATIONSTATECANCELLED
  _ -> InferenceCustomerEvaluationStateEnumUnknown value
inferenceCustomerEvaluationStateEnumValue :: InferenceCustomerEvaluationStateEnum -> Int
inferenceCustomerEvaluationStateEnumValue value = case value of
  InferenceCustomerEvaluationStateEVALUATIONSTATEUNSPECIFIED -> 0
  InferenceCustomerEvaluationStateEVALUATIONSTATEADMITTED -> 1
  InferenceCustomerEvaluationStateEVALUATIONSTATERUNNING -> 2
  InferenceCustomerEvaluationStateEVALUATIONSTATECOMPLETED -> 3
  InferenceCustomerEvaluationStateEVALUATIONSTATEFAILED -> 4
  InferenceCustomerEvaluationStateEVALUATIONSTATECANCELLED -> 5
  InferenceCustomerEvaluationStateEnumUnknown number -> number

data InferenceCustomerItemKindEnum
  = InferenceCustomerItemKindITEMKINDUNSPECIFIED
  | InferenceCustomerItemKindITEMKINDINSTRUCTION
  | InferenceCustomerItemKindITEMKINDSYSTEM
  | InferenceCustomerItemKindITEMKINDDEVELOPER
  | InferenceCustomerItemKindITEMKINDUSER
  | InferenceCustomerItemKindITEMKINDASSISTANT
  | InferenceCustomerItemKindITEMKINDTOOLDEFINITION
  | InferenceCustomerItemKindITEMKINDTOOLCALL
  | InferenceCustomerItemKindITEMKINDTOOLRESULT
  | InferenceCustomerItemKindITEMKINDIMAGE
  | InferenceCustomerItemKindITEMKINDAUDIO
  | InferenceCustomerItemKindITEMKINDFILE
  | InferenceCustomerItemKindITEMKINDCONTINUATION
  | InferenceCustomerItemKindEnumUnknown Int
  deriving (Eq, Ord, Show)

inferenceCustomerItemKindEnumFromWire :: Int -> InferenceCustomerItemKindEnum
inferenceCustomerItemKindEnumFromWire value = case value of
  0 -> InferenceCustomerItemKindITEMKINDUNSPECIFIED
  1 -> InferenceCustomerItemKindITEMKINDINSTRUCTION
  2 -> InferenceCustomerItemKindITEMKINDSYSTEM
  3 -> InferenceCustomerItemKindITEMKINDDEVELOPER
  4 -> InferenceCustomerItemKindITEMKINDUSER
  5 -> InferenceCustomerItemKindITEMKINDASSISTANT
  6 -> InferenceCustomerItemKindITEMKINDTOOLDEFINITION
  7 -> InferenceCustomerItemKindITEMKINDTOOLCALL
  8 -> InferenceCustomerItemKindITEMKINDTOOLRESULT
  9 -> InferenceCustomerItemKindITEMKINDIMAGE
  10 -> InferenceCustomerItemKindITEMKINDAUDIO
  11 -> InferenceCustomerItemKindITEMKINDFILE
  12 -> InferenceCustomerItemKindITEMKINDCONTINUATION
  _ -> InferenceCustomerItemKindEnumUnknown value
inferenceCustomerItemKindEnumValue :: InferenceCustomerItemKindEnum -> Int
inferenceCustomerItemKindEnumValue value = case value of
  InferenceCustomerItemKindITEMKINDUNSPECIFIED -> 0
  InferenceCustomerItemKindITEMKINDINSTRUCTION -> 1
  InferenceCustomerItemKindITEMKINDSYSTEM -> 2
  InferenceCustomerItemKindITEMKINDDEVELOPER -> 3
  InferenceCustomerItemKindITEMKINDUSER -> 4
  InferenceCustomerItemKindITEMKINDASSISTANT -> 5
  InferenceCustomerItemKindITEMKINDTOOLDEFINITION -> 6
  InferenceCustomerItemKindITEMKINDTOOLCALL -> 7
  InferenceCustomerItemKindITEMKINDTOOLRESULT -> 8
  InferenceCustomerItemKindITEMKINDIMAGE -> 9
  InferenceCustomerItemKindITEMKINDAUDIO -> 10
  InferenceCustomerItemKindITEMKINDFILE -> 11
  InferenceCustomerItemKindITEMKINDCONTINUATION -> 12
  InferenceCustomerItemKindEnumUnknown number -> number

data InferenceCustomerRunTerminalEnum
  = InferenceCustomerRunTerminalRUNTERMINALUNSPECIFIED
  | InferenceCustomerRunTerminalRUNTERMINALCOMPLETED
  | InferenceCustomerRunTerminalRUNTERMINALOUTPUTLIMITED
  | InferenceCustomerRunTerminalRUNTERMINALTOOLCALL
  | InferenceCustomerRunTerminalRUNTERMINALREFUSAL
  | InferenceCustomerRunTerminalRUNTERMINALCANCELLED
  | InferenceCustomerRunTerminalRUNTERMINALFAILED
  | InferenceCustomerRunTerminalRUNTERMINALINDETERMINATE
  | InferenceCustomerRunTerminalEnumUnknown Int
  deriving (Eq, Ord, Show)

inferenceCustomerRunTerminalEnumFromWire :: Int -> InferenceCustomerRunTerminalEnum
inferenceCustomerRunTerminalEnumFromWire value = case value of
  0 -> InferenceCustomerRunTerminalRUNTERMINALUNSPECIFIED
  1 -> InferenceCustomerRunTerminalRUNTERMINALCOMPLETED
  2 -> InferenceCustomerRunTerminalRUNTERMINALOUTPUTLIMITED
  3 -> InferenceCustomerRunTerminalRUNTERMINALTOOLCALL
  4 -> InferenceCustomerRunTerminalRUNTERMINALREFUSAL
  5 -> InferenceCustomerRunTerminalRUNTERMINALCANCELLED
  6 -> InferenceCustomerRunTerminalRUNTERMINALFAILED
  7 -> InferenceCustomerRunTerminalRUNTERMINALINDETERMINATE
  _ -> InferenceCustomerRunTerminalEnumUnknown value
inferenceCustomerRunTerminalEnumValue :: InferenceCustomerRunTerminalEnum -> Int
inferenceCustomerRunTerminalEnumValue value = case value of
  InferenceCustomerRunTerminalRUNTERMINALUNSPECIFIED -> 0
  InferenceCustomerRunTerminalRUNTERMINALCOMPLETED -> 1
  InferenceCustomerRunTerminalRUNTERMINALOUTPUTLIMITED -> 2
  InferenceCustomerRunTerminalRUNTERMINALTOOLCALL -> 3
  InferenceCustomerRunTerminalRUNTERMINALREFUSAL -> 4
  InferenceCustomerRunTerminalRUNTERMINALCANCELLED -> 5
  InferenceCustomerRunTerminalRUNTERMINALFAILED -> 6
  InferenceCustomerRunTerminalRUNTERMINALINDETERMINATE -> 7
  InferenceCustomerRunTerminalEnumUnknown number -> number

data InferenceCustomerWarmStateEnum
  = InferenceCustomerWarmStateWARMSTATEUNSPECIFIED
  | InferenceCustomerWarmStateWARMSTATEACTIVE
  | InferenceCustomerWarmStateWARMSTATEEXPIRED
  | InferenceCustomerWarmStateWARMSTATEBREACHED
  | InferenceCustomerWarmStateWARMSTATERELEASED
  | InferenceCustomerWarmStateEnumUnknown Int
  deriving (Eq, Ord, Show)

inferenceCustomerWarmStateEnumFromWire :: Int -> InferenceCustomerWarmStateEnum
inferenceCustomerWarmStateEnumFromWire value = case value of
  0 -> InferenceCustomerWarmStateWARMSTATEUNSPECIFIED
  1 -> InferenceCustomerWarmStateWARMSTATEACTIVE
  2 -> InferenceCustomerWarmStateWARMSTATEEXPIRED
  3 -> InferenceCustomerWarmStateWARMSTATEBREACHED
  4 -> InferenceCustomerWarmStateWARMSTATERELEASED
  _ -> InferenceCustomerWarmStateEnumUnknown value
inferenceCustomerWarmStateEnumValue :: InferenceCustomerWarmStateEnum -> Int
inferenceCustomerWarmStateEnumValue value = case value of
  InferenceCustomerWarmStateWARMSTATEUNSPECIFIED -> 0
  InferenceCustomerWarmStateWARMSTATEACTIVE -> 1
  InferenceCustomerWarmStateWARMSTATEEXPIRED -> 2
  InferenceCustomerWarmStateWARMSTATEBREACHED -> 3
  InferenceCustomerWarmStateWARMSTATERELEASED -> 4
  InferenceCustomerWarmStateEnumUnknown number -> number

data MachinesCapabilityEnum
  = MachinesCapabilityCAPABILITYUNSPECIFIED
  | MachinesCapabilityCAPABILITYELASTICCPU
  | MachinesCapabilityCAPABILITYELASTICMEMORY
  | MachinesCapabilityCAPABILITYLIVECHECKPOINT
  | MachinesCapabilityCAPABILITYLIVEFORK
  | MachinesCapabilityCAPABILITYSUSPENDRESUME
  | MachinesCapabilityCAPABILITYLIVEMOVEMENT
  | MachinesCapabilityCAPABILITYDISKFORK
  | MachinesCapabilityEnumUnknown Int
  deriving (Eq, Ord, Show)

machinesCapabilityEnumFromWire :: Int -> MachinesCapabilityEnum
machinesCapabilityEnumFromWire value = case value of
  0 -> MachinesCapabilityCAPABILITYUNSPECIFIED
  1 -> MachinesCapabilityCAPABILITYELASTICCPU
  2 -> MachinesCapabilityCAPABILITYELASTICMEMORY
  3 -> MachinesCapabilityCAPABILITYLIVECHECKPOINT
  4 -> MachinesCapabilityCAPABILITYLIVEFORK
  5 -> MachinesCapabilityCAPABILITYSUSPENDRESUME
  6 -> MachinesCapabilityCAPABILITYLIVEMOVEMENT
  7 -> MachinesCapabilityCAPABILITYDISKFORK
  _ -> MachinesCapabilityEnumUnknown value
machinesCapabilityEnumValue :: MachinesCapabilityEnum -> Int
machinesCapabilityEnumValue value = case value of
  MachinesCapabilityCAPABILITYUNSPECIFIED -> 0
  MachinesCapabilityCAPABILITYELASTICCPU -> 1
  MachinesCapabilityCAPABILITYELASTICMEMORY -> 2
  MachinesCapabilityCAPABILITYLIVECHECKPOINT -> 3
  MachinesCapabilityCAPABILITYLIVEFORK -> 4
  MachinesCapabilityCAPABILITYSUSPENDRESUME -> 5
  MachinesCapabilityCAPABILITYLIVEMOVEMENT -> 6
  MachinesCapabilityCAPABILITYDISKFORK -> 7
  MachinesCapabilityEnumUnknown number -> number

data MachinesCompatibilityModeEnum
  = MachinesCompatibilityModeCOMPATIBILITYMODEUNSPECIFIED
  | MachinesCompatibilityModeCOMPATIBILITYMODEBESTEFFORT
  | MachinesCompatibilityModeCOMPATIBILITYMODEREQUIRE
  | MachinesCompatibilityModeEnumUnknown Int
  deriving (Eq, Ord, Show)

machinesCompatibilityModeEnumFromWire :: Int -> MachinesCompatibilityModeEnum
machinesCompatibilityModeEnumFromWire value = case value of
  0 -> MachinesCompatibilityModeCOMPATIBILITYMODEUNSPECIFIED
  1 -> MachinesCompatibilityModeCOMPATIBILITYMODEBESTEFFORT
  2 -> MachinesCompatibilityModeCOMPATIBILITYMODEREQUIRE
  _ -> MachinesCompatibilityModeEnumUnknown value
machinesCompatibilityModeEnumValue :: MachinesCompatibilityModeEnum -> Int
machinesCompatibilityModeEnumValue value = case value of
  MachinesCompatibilityModeCOMPATIBILITYMODEUNSPECIFIED -> 0
  MachinesCompatibilityModeCOMPATIBILITYMODEBESTEFFORT -> 1
  MachinesCompatibilityModeCOMPATIBILITYMODEREQUIRE -> 2
  MachinesCompatibilityModeEnumUnknown number -> number

data MachinesEventKindEnum
  = MachinesEventKindEVENTKINDUNSPECIFIED
  | MachinesEventKindEVENTKINDSTATE
  | MachinesEventKindEVENTKINDPRESSURE
  | MachinesEventKindEVENTKINDCAPACITY
  | MachinesEventKindEnumUnknown Int
  deriving (Eq, Ord, Show)

machinesEventKindEnumFromWire :: Int -> MachinesEventKindEnum
machinesEventKindEnumFromWire value = case value of
  0 -> MachinesEventKindEVENTKINDUNSPECIFIED
  1 -> MachinesEventKindEVENTKINDSTATE
  2 -> MachinesEventKindEVENTKINDPRESSURE
  3 -> MachinesEventKindEVENTKINDCAPACITY
  _ -> MachinesEventKindEnumUnknown value
machinesEventKindEnumValue :: MachinesEventKindEnum -> Int
machinesEventKindEnumValue value = case value of
  MachinesEventKindEVENTKINDUNSPECIFIED -> 0
  MachinesEventKindEVENTKINDSTATE -> 1
  MachinesEventKindEVENTKINDPRESSURE -> 2
  MachinesEventKindEVENTKINDCAPACITY -> 3
  MachinesEventKindEnumUnknown number -> number

data MachinesExpirationKindEnum
  = MachinesExpirationKindEXPIRATIONKINDUNSPECIFIED
  | MachinesExpirationKindEXPIRATIONKINDNEVER
  | MachinesExpirationKindEXPIRATIONKINDMAXAGE
  | MachinesExpirationKindEXPIRATIONKINDAT
  | MachinesExpirationKindEXPIRATIONKINDIDLE
  | MachinesExpirationKindEnumUnknown Int
  deriving (Eq, Ord, Show)

machinesExpirationKindEnumFromWire :: Int -> MachinesExpirationKindEnum
machinesExpirationKindEnumFromWire value = case value of
  0 -> MachinesExpirationKindEXPIRATIONKINDUNSPECIFIED
  1 -> MachinesExpirationKindEXPIRATIONKINDNEVER
  2 -> MachinesExpirationKindEXPIRATIONKINDMAXAGE
  3 -> MachinesExpirationKindEXPIRATIONKINDAT
  4 -> MachinesExpirationKindEXPIRATIONKINDIDLE
  _ -> MachinesExpirationKindEnumUnknown value
machinesExpirationKindEnumValue :: MachinesExpirationKindEnum -> Int
machinesExpirationKindEnumValue value = case value of
  MachinesExpirationKindEXPIRATIONKINDUNSPECIFIED -> 0
  MachinesExpirationKindEXPIRATIONKINDNEVER -> 1
  MachinesExpirationKindEXPIRATIONKINDMAXAGE -> 2
  MachinesExpirationKindEXPIRATIONKINDAT -> 3
  MachinesExpirationKindEXPIRATIONKINDIDLE -> 4
  MachinesExpirationKindEnumUnknown number -> number

data MachinesForkFidelityEnum
  = MachinesForkFidelityFORKFIDELITYUNSPECIFIED
  | MachinesForkFidelityFORKFIDELITYMEMORYANDDISK
  | MachinesForkFidelityFORKFIDELITYDISKONLY
  | MachinesForkFidelityEnumUnknown Int
  deriving (Eq, Ord, Show)

machinesForkFidelityEnumFromWire :: Int -> MachinesForkFidelityEnum
machinesForkFidelityEnumFromWire value = case value of
  0 -> MachinesForkFidelityFORKFIDELITYUNSPECIFIED
  1 -> MachinesForkFidelityFORKFIDELITYMEMORYANDDISK
  2 -> MachinesForkFidelityFORKFIDELITYDISKONLY
  _ -> MachinesForkFidelityEnumUnknown value
machinesForkFidelityEnumValue :: MachinesForkFidelityEnum -> Int
machinesForkFidelityEnumValue value = case value of
  MachinesForkFidelityFORKFIDELITYUNSPECIFIED -> 0
  MachinesForkFidelityFORKFIDELITYMEMORYANDDISK -> 1
  MachinesForkFidelityFORKFIDELITYDISKONLY -> 2
  MachinesForkFidelityEnumUnknown number -> number

data MachinesImageKindEnum
  = MachinesImageKindIMAGEKINDUNSPECIFIED
  | MachinesImageKindIMAGEKINDMANAGEDOCI
  | MachinesImageKindIMAGEKINDCUSTOM
  | MachinesImageKindIMAGEKINDCHECKPOINT
  | MachinesImageKindEnumUnknown Int
  deriving (Eq, Ord, Show)

machinesImageKindEnumFromWire :: Int -> MachinesImageKindEnum
machinesImageKindEnumFromWire value = case value of
  0 -> MachinesImageKindIMAGEKINDUNSPECIFIED
  1 -> MachinesImageKindIMAGEKINDMANAGEDOCI
  2 -> MachinesImageKindIMAGEKINDCUSTOM
  3 -> MachinesImageKindIMAGEKINDCHECKPOINT
  _ -> MachinesImageKindEnumUnknown value
machinesImageKindEnumValue :: MachinesImageKindEnum -> Int
machinesImageKindEnumValue value = case value of
  MachinesImageKindIMAGEKINDUNSPECIFIED -> 0
  MachinesImageKindIMAGEKINDMANAGEDOCI -> 1
  MachinesImageKindIMAGEKINDCUSTOM -> 2
  MachinesImageKindIMAGEKINDCHECKPOINT -> 3
  MachinesImageKindEnumUnknown number -> number

data MachinesMachineStatusEnum
  = MachinesMachineStatusMACHINESTATUSUNSPECIFIED
  | MachinesMachineStatusMACHINESTATUSSTARTING
  | MachinesMachineStatusMACHINESTATUSRUNNING
  | MachinesMachineStatusMACHINESTATUSSUSPENDING
  | MachinesMachineStatusMACHINESTATUSSUSPENDED
  | MachinesMachineStatusMACHINESTATUSWAKING
  | MachinesMachineStatusMACHINESTATUSDESTROYING
  | MachinesMachineStatusMACHINESTATUSDESTROYED
  | MachinesMachineStatusMACHINESTATUSFAILED
  | MachinesMachineStatusMACHINESTATUSINDETERMINATE
  | MachinesMachineStatusEnumUnknown Int
  deriving (Eq, Ord, Show)

machinesMachineStatusEnumFromWire :: Int -> MachinesMachineStatusEnum
machinesMachineStatusEnumFromWire value = case value of
  0 -> MachinesMachineStatusMACHINESTATUSUNSPECIFIED
  1 -> MachinesMachineStatusMACHINESTATUSSTARTING
  2 -> MachinesMachineStatusMACHINESTATUSRUNNING
  3 -> MachinesMachineStatusMACHINESTATUSSUSPENDING
  4 -> MachinesMachineStatusMACHINESTATUSSUSPENDED
  5 -> MachinesMachineStatusMACHINESTATUSWAKING
  6 -> MachinesMachineStatusMACHINESTATUSDESTROYING
  7 -> MachinesMachineStatusMACHINESTATUSDESTROYED
  8 -> MachinesMachineStatusMACHINESTATUSFAILED
  9 -> MachinesMachineStatusMACHINESTATUSINDETERMINATE
  _ -> MachinesMachineStatusEnumUnknown value
machinesMachineStatusEnumValue :: MachinesMachineStatusEnum -> Int
machinesMachineStatusEnumValue value = case value of
  MachinesMachineStatusMACHINESTATUSUNSPECIFIED -> 0
  MachinesMachineStatusMACHINESTATUSSTARTING -> 1
  MachinesMachineStatusMACHINESTATUSRUNNING -> 2
  MachinesMachineStatusMACHINESTATUSSUSPENDING -> 3
  MachinesMachineStatusMACHINESTATUSSUSPENDED -> 4
  MachinesMachineStatusMACHINESTATUSWAKING -> 5
  MachinesMachineStatusMACHINESTATUSDESTROYING -> 6
  MachinesMachineStatusMACHINESTATUSDESTROYED -> 7
  MachinesMachineStatusMACHINESTATUSFAILED -> 8
  MachinesMachineStatusMACHINESTATUSINDETERMINATE -> 9
  MachinesMachineStatusEnumUnknown number -> number

data MachinesOperationStatusEnum
  = MachinesOperationStatusOPERATIONSTATUSUNSPECIFIED
  | MachinesOperationStatusOPERATIONSTATUSPENDING
  | MachinesOperationStatusOPERATIONSTATUSSUCCEEDED
  | MachinesOperationStatusOPERATIONSTATUSCANCELLED
  | MachinesOperationStatusOPERATIONSTATUSINDETERMINATE
  | MachinesOperationStatusOPERATIONSTATUSFAILED
  | MachinesOperationStatusEnumUnknown Int
  deriving (Eq, Ord, Show)

machinesOperationStatusEnumFromWire :: Int -> MachinesOperationStatusEnum
machinesOperationStatusEnumFromWire value = case value of
  0 -> MachinesOperationStatusOPERATIONSTATUSUNSPECIFIED
  1 -> MachinesOperationStatusOPERATIONSTATUSPENDING
  2 -> MachinesOperationStatusOPERATIONSTATUSSUCCEEDED
  3 -> MachinesOperationStatusOPERATIONSTATUSCANCELLED
  4 -> MachinesOperationStatusOPERATIONSTATUSINDETERMINATE
  5 -> MachinesOperationStatusOPERATIONSTATUSFAILED
  _ -> MachinesOperationStatusEnumUnknown value
machinesOperationStatusEnumValue :: MachinesOperationStatusEnum -> Int
machinesOperationStatusEnumValue value = case value of
  MachinesOperationStatusOPERATIONSTATUSUNSPECIFIED -> 0
  MachinesOperationStatusOPERATIONSTATUSPENDING -> 1
  MachinesOperationStatusOPERATIONSTATUSSUCCEEDED -> 2
  MachinesOperationStatusOPERATIONSTATUSCANCELLED -> 3
  MachinesOperationStatusOPERATIONSTATUSINDETERMINATE -> 4
  MachinesOperationStatusOPERATIONSTATUSFAILED -> 5
  MachinesOperationStatusEnumUnknown number -> number

data MachinesPressureKindEnum
  = MachinesPressureKindPRESSUREKINDUNSPECIFIED
  | MachinesPressureKindPRESSUREKINDCUSTOMERBUDGET
  | MachinesPressureKindPRESSUREKINDMACHINELIMIT
  | MachinesPressureKindPRESSUREKINDSERVICESATURATION
  | MachinesPressureKindEnumUnknown Int
  deriving (Eq, Ord, Show)

machinesPressureKindEnumFromWire :: Int -> MachinesPressureKindEnum
machinesPressureKindEnumFromWire value = case value of
  0 -> MachinesPressureKindPRESSUREKINDUNSPECIFIED
  1 -> MachinesPressureKindPRESSUREKINDCUSTOMERBUDGET
  2 -> MachinesPressureKindPRESSUREKINDMACHINELIMIT
  3 -> MachinesPressureKindPRESSUREKINDSERVICESATURATION
  _ -> MachinesPressureKindEnumUnknown value
machinesPressureKindEnumValue :: MachinesPressureKindEnum -> Int
machinesPressureKindEnumValue value = case value of
  MachinesPressureKindPRESSUREKINDUNSPECIFIED -> 0
  MachinesPressureKindPRESSUREKINDCUSTOMERBUDGET -> 1
  MachinesPressureKindPRESSUREKINDMACHINELIMIT -> 2
  MachinesPressureKindPRESSUREKINDSERVICESATURATION -> 3
  MachinesPressureKindEnumUnknown number -> number

data ObjectsErrorCodeEnum
  = ObjectsErrorCodeERRORCODEUNSPECIFIED
  | ObjectsErrorCodeERRORCODEINVALIDARGUMENT
  | ObjectsErrorCodeERRORCODENOTFOUND
  | ObjectsErrorCodeERRORCODEALREADYEXISTS
  | ObjectsErrorCodeERRORCODEPRECONDITIONFAILED
  | ObjectsErrorCodeERRORCODEIDEMPOTENCYMISMATCH
  | ObjectsErrorCodeERRORCODEQUOTAEXCEEDED
  | ObjectsErrorCodeERRORCODEUNSUPPORTED
  | ObjectsErrorCodeERRORCODEUNAVAILABLE
  | ObjectsErrorCodeERRORCODEACCESSDENIED
  | ObjectsErrorCodeERRORCODERANGENOTSATISFIABLE
  | ObjectsErrorCodeERRORCODENOTMODIFIED
  | ObjectsErrorCodeEnumUnknown Int
  deriving (Eq, Ord, Show)

objectsErrorCodeEnumFromWire :: Int -> ObjectsErrorCodeEnum
objectsErrorCodeEnumFromWire value = case value of
  0 -> ObjectsErrorCodeERRORCODEUNSPECIFIED
  1 -> ObjectsErrorCodeERRORCODEINVALIDARGUMENT
  2 -> ObjectsErrorCodeERRORCODENOTFOUND
  3 -> ObjectsErrorCodeERRORCODEALREADYEXISTS
  4 -> ObjectsErrorCodeERRORCODEPRECONDITIONFAILED
  5 -> ObjectsErrorCodeERRORCODEIDEMPOTENCYMISMATCH
  6 -> ObjectsErrorCodeERRORCODEQUOTAEXCEEDED
  7 -> ObjectsErrorCodeERRORCODEUNSUPPORTED
  8 -> ObjectsErrorCodeERRORCODEUNAVAILABLE
  9 -> ObjectsErrorCodeERRORCODEACCESSDENIED
  10 -> ObjectsErrorCodeERRORCODERANGENOTSATISFIABLE
  11 -> ObjectsErrorCodeERRORCODENOTMODIFIED
  _ -> ObjectsErrorCodeEnumUnknown value
objectsErrorCodeEnumValue :: ObjectsErrorCodeEnum -> Int
objectsErrorCodeEnumValue value = case value of
  ObjectsErrorCodeERRORCODEUNSPECIFIED -> 0
  ObjectsErrorCodeERRORCODEINVALIDARGUMENT -> 1
  ObjectsErrorCodeERRORCODENOTFOUND -> 2
  ObjectsErrorCodeERRORCODEALREADYEXISTS -> 3
  ObjectsErrorCodeERRORCODEPRECONDITIONFAILED -> 4
  ObjectsErrorCodeERRORCODEIDEMPOTENCYMISMATCH -> 5
  ObjectsErrorCodeERRORCODEQUOTAEXCEEDED -> 6
  ObjectsErrorCodeERRORCODEUNSUPPORTED -> 7
  ObjectsErrorCodeERRORCODEUNAVAILABLE -> 8
  ObjectsErrorCodeERRORCODEACCESSDENIED -> 9
  ObjectsErrorCodeERRORCODERANGENOTSATISFIABLE -> 10
  ObjectsErrorCodeERRORCODENOTMODIFIED -> 11
  ObjectsErrorCodeEnumUnknown number -> number

data WorkersJobStateEnum
  = WorkersJobStateJOBSTATEUNSPECIFIED
  | WorkersJobStateJOBSTATEACCEPTED
  | WorkersJobStateJOBSTATERUNNING
  | WorkersJobStateJOBSTATESUCCEEDED
  | WorkersJobStateJOBSTATEFAILED
  | WorkersJobStateJOBSTATECANCELLED
  | WorkersJobStateEnumUnknown Int
  deriving (Eq, Ord, Show)

workersJobStateEnumFromWire :: Int -> WorkersJobStateEnum
workersJobStateEnumFromWire value = case value of
  0 -> WorkersJobStateJOBSTATEUNSPECIFIED
  1 -> WorkersJobStateJOBSTATEACCEPTED
  2 -> WorkersJobStateJOBSTATERUNNING
  3 -> WorkersJobStateJOBSTATESUCCEEDED
  4 -> WorkersJobStateJOBSTATEFAILED
  5 -> WorkersJobStateJOBSTATECANCELLED
  _ -> WorkersJobStateEnumUnknown value
workersJobStateEnumValue :: WorkersJobStateEnum -> Int
workersJobStateEnumValue value = case value of
  WorkersJobStateJOBSTATEUNSPECIFIED -> 0
  WorkersJobStateJOBSTATEACCEPTED -> 1
  WorkersJobStateJOBSTATERUNNING -> 2
  WorkersJobStateJOBSTATESUCCEEDED -> 3
  WorkersJobStateJOBSTATEFAILED -> 4
  WorkersJobStateJOBSTATECANCELLED -> 5
  WorkersJobStateEnumUnknown number -> number

newtype ActorId = ActorId { unActorId :: String } deriving (Eq, Ord, Show)
mkActorId :: String -> Either String ActorId
mkActorId value | null value = Left "ActorId failed Rust validation" | otherwise = Right (ActorId value)
actorIdValue :: ActorId -> String
actorIdValue = unActorId

newtype MethodName = MethodName { unMethodName :: String } deriving (Eq, Ord, Show)
mkMethodName :: String -> Either String MethodName
mkMethodName value | null value = Left "MethodName failed Rust validation" | otherwise = Right (MethodName value)
methodNameValue :: MethodName -> String
methodNameValue = unMethodName

newtype ResourcePath = ResourcePath { unResourcePath :: String } deriving (Eq, Ord, Show)
mkResourcePath :: String -> Either String ResourcePath
mkResourcePath value | null value = Left "ResourcePath failed Rust validation" | otherwise = Right (ResourcePath value)
resourcePathValue :: ResourcePath -> String
resourcePathValue = unResourcePath

newtype SourceName = SourceName { unSourceName :: String } deriving (Eq, Ord, Show)
mkSourceName :: String -> Either String SourceName
mkSourceName value | null value = Left "SourceName failed Rust validation" | otherwise = Right (SourceName value)
sourceNameValue :: SourceName -> String
sourceNameValue = unSourceName

newtype DestinationName = DestinationName { unDestinationName :: String } deriving (Eq, Ord, Show)
mkDestinationName :: String -> Either String DestinationName
mkDestinationName value | null value = Left "DestinationName failed Rust validation" | otherwise = Right (DestinationName value)
destinationNameValue :: DestinationName -> String
destinationNameValue = unDestinationName

newtype BucketName = BucketName { unBucketName :: String } deriving (Eq, Ord, Show)
mkBucketName :: String -> Either String BucketName
mkBucketName value | null value = Left "BucketName failed Rust validation" | otherwise = Right (BucketName value)
bucketNameValue :: BucketName -> String
bucketNameValue = unBucketName

newtype ObjectKey = ObjectKey { unObjectKey :: String } deriving (Eq, Ord, Show)
mkObjectKey :: String -> Either String ObjectKey
mkObjectKey value | null value = Left "ObjectKey failed Rust validation" | otherwise = Right (ObjectKey value)
objectKeyValue :: ObjectKey -> String
objectKeyValue = unObjectKey

newtype VersionAlias = VersionAlias { unVersionAlias :: String } deriving (Eq, Ord, Show)
mkVersionAlias :: String -> Either String VersionAlias
mkVersionAlias value | null value = Left "VersionAlias failed Rust validation" | otherwise = Right (VersionAlias value)
versionAliasValue :: VersionAlias -> String
versionAliasValue = unVersionAlias

newtype JobId = JobId { unJobId :: String } deriving (Eq, Ord, Show)
mkJobId :: String -> Either String JobId
mkJobId value | null value = Left "JobId failed Rust validation" | otherwise = Right (JobId value)
jobIdValue :: JobId -> String
jobIdValue = unJobId

newtype MachineId = MachineId { unMachineId :: BS.ByteString } deriving (Eq, Ord, Show)
mkMachineId :: BS.ByteString -> Either String MachineId
mkMachineId value | BS.null value || BS.length value /= 16 = Left "MachineId failed Rust validation" | otherwise = Right (MachineId value)
machineIdValue :: MachineId -> BS.ByteString
machineIdValue = unMachineId

newtype OperationId = OperationId { unOperationId :: BS.ByteString } deriving (Eq, Ord, Show)
mkOperationId :: BS.ByteString -> Either String OperationId
mkOperationId value | BS.null value || BS.length value /= 16 = Left "OperationId failed Rust validation" | otherwise = Right (OperationId value)
operationIdValue :: OperationId -> BS.ByteString
operationIdValue = unOperationId

newtype WorkspaceId = WorkspaceId { unWorkspaceId :: BS.ByteString } deriving (Eq, Ord, Show)
mkWorkspaceId :: BS.ByteString -> Either String WorkspaceId
mkWorkspaceId value | BS.null value || BS.length value /= 16 = Left "WorkspaceId failed Rust validation" | otherwise = Right (WorkspaceId value)
workspaceIdValue :: WorkspaceId -> BS.ByteString
workspaceIdValue = unWorkspaceId

newtype CheckpointId = CheckpointId { unCheckpointId :: BS.ByteString } deriving (Eq, Ord, Show)
mkCheckpointId :: BS.ByteString -> Either String CheckpointId
mkCheckpointId value | BS.null value || BS.length value /= 16 = Left "CheckpointId failed Rust validation" | otherwise = Right (CheckpointId value)
checkpointIdValue :: CheckpointId -> BS.ByteString
checkpointIdValue = unCheckpointId

newtype IdempotencyKeyBytes = IdempotencyKeyBytes { unIdempotencyKeyBytes :: BS.ByteString } deriving (Eq, Ord, Show)
mkIdempotencyKeyBytes :: BS.ByteString -> Either String IdempotencyKeyBytes
mkIdempotencyKeyBytes value | BS.null value = Left "IdempotencyKeyBytes failed Rust validation" | otherwise = Right (IdempotencyKeyBytes value)
idempotencyKeyBytesValue :: IdempotencyKeyBytes -> BS.ByteString
idempotencyKeyBytesValue = unIdempotencyKeyBytes

newtype IdempotencyKeyText = IdempotencyKeyText { unIdempotencyKeyText :: String } deriving (Eq, Ord, Show)
mkIdempotencyKeyText :: String -> Either String IdempotencyKeyText
mkIdempotencyKeyText value | null value = Left "IdempotencyKeyText failed Rust validation" | otherwise = Right (IdempotencyKeyText value)
idempotencyKeyTextValue :: IdempotencyKeyText -> String
idempotencyKeyTextValue = unIdempotencyKeyText

newtype IdempotencyKey = IdempotencyKey { unIdempotencyKey :: BS.ByteString } deriving (Eq, Ord, Show)
mkIdempotencyKey :: BS.ByteString -> Either String IdempotencyKey
mkIdempotencyKey value | BS.null value || BS.length value /= 16 = Left "IdempotencyKey failed Rust validation" | otherwise = Right (IdempotencyKey value)
idempotencyKeyValue :: IdempotencyKey -> BS.ByteString
idempotencyKeyValue = unIdempotencyKey

newtype OpaqueText = OpaqueText { unOpaqueText :: String } deriving (Eq, Ord, Show)
mkOpaqueText :: String -> Either String OpaqueText
mkOpaqueText value | null value = Left "OpaqueText failed Rust validation" | otherwise = Right (OpaqueText value)
opaqueTextValue :: OpaqueText -> String
opaqueTextValue = unOpaqueText

newtype UploadId = UploadId { unUploadId :: String } deriving (Eq, Ord, Show)
mkUploadId :: String -> Either String UploadId
mkUploadId value | null value = Left "UploadId failed Rust validation" | otherwise = Right (UploadId value)
uploadIdValue :: UploadId -> String
uploadIdValue = unUploadId

newtype Sha256Digest = Sha256Digest { unSha256Digest :: BS.ByteString } deriving (Eq, Ord, Show)
mkSha256Digest :: BS.ByteString -> Either String Sha256Digest
mkSha256Digest value | BS.length value /= 32 = Left "Sha256Digest failed Rust validation" | otherwise = Right (Sha256Digest value)
sha256DigestValue :: Sha256Digest -> BS.ByteString
sha256DigestValue = unSha256Digest

newtype RevisionDigest = RevisionDigest { unRevisionDigest :: BS.ByteString } deriving (Eq, Ord, Show)
mkRevisionDigest :: BS.ByteString -> Either String RevisionDigest
mkRevisionDigest value | BS.length value /= 32 = Left "RevisionDigest failed Rust validation" | otherwise = Right (RevisionDigest value)
revisionDigestValue :: RevisionDigest -> BS.ByteString
revisionDigestValue = unRevisionDigest

newtype Image = Image { unImage :: BS.ByteString } deriving (Eq, Ord, Show)
mkImage :: BS.ByteString -> Either String Image
mkImage value = Right (Image value)
imageValue :: Image -> BS.ByteString
imageValue = unImage

newtype Revision = Revision { unRevision :: Integer } deriving (Eq, Ord, Show)
mkRevision :: Integer -> Either String Revision
mkRevision value | value < 0 = Left "Revision failed Rust validation" | otherwise = Right (Revision value)
revisionValue :: Revision -> Integer
revisionValue = unRevision

newtype RunId = RunId { unRunId :: BS.ByteString } deriving (Eq, Ord, Show)
mkRunId :: BS.ByteString -> Either String RunId
mkRunId value | BS.length value /= 16 = Left "RunId failed Rust validation" | otherwise = Right (RunId value)
runIdValue :: RunId -> BS.ByteString
runIdValue = unRunId

newtype EvaluationId = EvaluationId { unEvaluationId :: BS.ByteString } deriving (Eq, Ord, Show)
mkEvaluationId :: BS.ByteString -> Either String EvaluationId
mkEvaluationId value | BS.length value /= 16 = Left "EvaluationId failed Rust validation" | otherwise = Right (EvaluationId value)
evaluationIdValue :: EvaluationId -> BS.ByteString
evaluationIdValue = unEvaluationId

newtype PageLimit = PageLimit { unPageLimit :: Integer } deriving (Eq, Ord, Show)
mkPageLimit :: Integer -> Either String PageLimit
mkPageLimit value | value <= 0 || value > 1000 = Left "PageLimit failed Rust validation" | otherwise = Right (PageLimit value)
pageLimitValue :: PageLimit -> Integer
pageLimitValue = unPageLimit

newtype StreamPageLimit = StreamPageLimit { unStreamPageLimit :: Integer } deriving (Eq, Ord, Show)
mkStreamPageLimit :: Integer -> Either String StreamPageLimit
mkStreamPageLimit value | value <= 0 || value > 1024 = Left "StreamPageLimit failed Rust validation" | otherwise = Right (StreamPageLimit value)
streamPageLimitValue :: StreamPageLimit -> Integer
streamPageLimitValue = unStreamPageLimit

newtype MachinePageLimit = MachinePageLimit { unMachinePageLimit :: Integer } deriving (Eq, Ord, Show)
mkMachinePageLimit :: Integer -> Either String MachinePageLimit
mkMachinePageLimit value | value <= 0 || value > 256 = Left "MachinePageLimit failed Rust validation" | otherwise = Right (MachinePageLimit value)
machinePageLimitValue :: MachinePageLimit -> Integer
machinePageLimitValue = unMachinePageLimit

newtype MachineEventPageLimit = MachineEventPageLimit { unMachineEventPageLimit :: Integer } deriving (Eq, Ord, Show)
mkMachineEventPageLimit :: Integer -> Either String MachineEventPageLimit
mkMachineEventPageLimit value | value <= 0 || value > 1024 = Left "MachineEventPageLimit failed Rust validation" | otherwise = Right (MachineEventPageLimit value)
machineEventPageLimitValue :: MachineEventPageLimit -> Integer
machineEventPageLimitValue = unMachineEventPageLimit

newtype CommitId = CommitId { unCommitId :: BS.ByteString } deriving (Eq, Ord, Show)
mkCommitId :: BS.ByteString -> Either String CommitId
mkCommitId value | BS.null value = Left "CommitId failed Rust validation" | otherwise = Right (CommitId value)
commitIdValue :: CommitId -> BS.ByteString
commitIdValue = unCommitId

newtype OpenEnumValue = OpenEnumValue { unOpenEnumValue :: Int } deriving (Eq, Ord, Show)
mkOpenEnumValue :: Int -> Either String OpenEnumValue
mkOpenEnumValue value = Right (OpenEnumValue value)
openEnumValueValue :: OpenEnumValue -> Int
openEnumValueValue = unOpenEnumValue

newtype OpaqueBytes = OpaqueBytes { unOpaqueBytes :: BS.ByteString } deriving (Eq, Ord, Show)
mkOpaqueBytes :: BS.ByteString -> Either String OpaqueBytes
mkOpaqueBytes value = Right (OpaqueBytes value)
opaqueBytesValue :: OpaqueBytes -> BS.ByteString
opaqueBytesValue = unOpaqueBytes

newtype SequenceNumber = SequenceNumber { unSequenceNumber :: Integer } deriving (Eq, Ord, Show)
mkSequenceNumber :: Integer -> Either String SequenceNumber
mkSequenceNumber value | value < 0 = Left "SequenceNumber failed Rust validation" | otherwise = Right (SequenceNumber value)
sequenceNumberValue :: SequenceNumber -> Integer
sequenceNumberValue = unSequenceNumber

newtype NonNegativeCount = NonNegativeCount { unNonNegativeCount :: Integer } deriving (Eq, Ord, Show)
mkNonNegativeCount :: Integer -> Either String NonNegativeCount
mkNonNegativeCount value | value < 0 = Left "NonNegativeCount failed Rust validation" | otherwise = Right (NonNegativeCount value)
nonNegativeCountValue :: NonNegativeCount -> Integer
nonNegativeCountValue = unNonNegativeCount

newtype PositiveCount = PositiveCount { unPositiveCount :: Integer } deriving (Eq, Ord, Show)
mkPositiveCount :: Integer -> Either String PositiveCount
mkPositiveCount value | value <= 0 = Left "PositiveCount failed Rust validation" | otherwise = Right (PositiveCount value)
positiveCountValue :: PositiveCount -> Integer
positiveCountValue = unPositiveCount

newtype UnixTimestampMillis = UnixTimestampMillis { unUnixTimestampMillis :: Integer } deriving (Eq, Ord, Show)
mkUnixTimestampMillis :: Integer -> Either String UnixTimestampMillis
mkUnixTimestampMillis value | value < 0 = Left "UnixTimestampMillis failed Rust validation" | otherwise = Right (UnixTimestampMillis value)
unixTimestampMillisValue :: UnixTimestampMillis -> Integer
unixTimestampMillisValue = unUnixTimestampMillis
