"""Directories of the repositories the Windows C toolchain runs from.

A repository's directory under the execution root is its canonical name, which only Bazel knows: these
resolve it from each repository's label.
"""

WINDOWS_LLVM = Label("@llvm_toolchain_llvm//:bin").workspace_root
WINDOWS_XWIN = Label("@xwin//:libs").workspace_root
