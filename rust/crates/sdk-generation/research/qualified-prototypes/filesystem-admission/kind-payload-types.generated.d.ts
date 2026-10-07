/* Generated from canonical Rust enums; do not hand-edit. */
export type FilesystemFileKind = "regular" | "directory" | "symbolic-link" | "fifo" | "socket" | "character-device" | "block-device" | "reparse-point" | "mount-boundary";
export type FilesystemFilePayloadKind = "inline-regular" | "regular" | "directory" | "symbolic-link" | "empty" | "device" | "reparse-point";

export interface FilesystemKindPayloadTypes {
  readonly fileKind: FilesystemFileKind;
  readonly payloadKind: FilesystemFilePayloadKind;
}
