/*
 * DarwinFUSE — internal fuse context management
 *
 * Copyright (c) 2026 Marcel Cotta. All rights reserved.
 * Licensed under the MIT License.
 */

#ifndef DARWINFUSE_FUSE_CONTEXT_H
#define DARWINFUSE_FUSE_CONTEXT_H

#include <sys/types.h>

/*
 * Set the thread-local FUSE context identity: uid, gid, and supplementary
 * groups. Called by nfs4_server.c before invoking any FUSE callback, from
 * the ONC RPC AUTH_SYS credentials; fuse_getgroups() returns the groups.
 */
void darwinfuse_set_context(uid_t uid, gid_t gid,
                            unsigned ngroups, const gid_t *groups);

/*
 * Set the thread-local FUSE context private_data.
 * Must be called with user_data before init(), and with init()'s
 * return value after init(). Programs like sshfs depend on this.
 */
void darwinfuse_set_private_data(void *private_data);

#endif /* DARWINFUSE_FUSE_CONTEXT_H */
