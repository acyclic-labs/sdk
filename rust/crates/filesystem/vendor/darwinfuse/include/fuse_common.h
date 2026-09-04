/*
 * DarwinFUSE — fuse_common.h compatibility shim
 *
 * In libfuse, fuse_common.h contains shared types (fuse_file_info,
 * fuse_conn_info, capability flags). DarwinFUSE puts everything in
 * fuse.h, so this header simply includes it for source compatibility
 * with programs that do #include <fuse/fuse_common.h>.
 *
 * Copyright (c) 2026 Marcel Cotta. All rights reserved.
 * Licensed under the MIT License.
 */

#ifndef DARWINFUSE_FUSE_COMMON_H
#define DARWINFUSE_FUSE_COMMON_H

#include "fuse.h"

#endif /* DARWINFUSE_FUSE_COMMON_H */
