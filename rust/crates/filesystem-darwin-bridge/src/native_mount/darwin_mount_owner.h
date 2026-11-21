#ifndef ACYCLIC_DARWIN_MOUNT_OWNER_H
#define ACYCLIC_DARWIN_MOUNT_OWNER_H

#include <stddef.h>
#include <stdatomic.h>
#include <sys/types.h>

struct acyclic_mount_owner {
    _Atomic pid_t pid;
    int control;
    int peer;
};

int acyclic_mount_owner_init(struct acyclic_mount_owner *owner);
void acyclic_mount_owner_destroy(struct acyclic_mount_owner *owner);
void acyclic_mount_owner_release(const struct acyclic_mount_owner *owner);
int acyclic_mount_owner_start(struct acyclic_mount_owner *owner,
                              const unsigned char *image, size_t image_len,
                              int fence, const int *holds, size_t hold_count,
                              const char *options, const char *source,
                              const char *destination, const char *socket_path);
int acyclic_mount_owner_wait(struct acyclic_mount_owner *owner);

#endif
