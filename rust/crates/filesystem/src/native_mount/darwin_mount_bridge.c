/* Darwin high-level mount bridge. Filesystem truth remains in Rust. */

#include <fuse.h>

#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

struct acyclic_fs_native_stat {
  uint64_t inode;
  uint64_t logical_bytes;
  uint64_t blocks;
  int64_t accessed_seconds;
  uint32_t accessed_nanoseconds;
  int64_t modified_seconds;
  uint32_t modified_nanoseconds;
  int64_t changed_seconds;
  uint32_t changed_nanoseconds;
  int64_t created_seconds;
  uint32_t created_nanoseconds;
  uint32_t mode;
  uint32_t link_count;
  uint32_t uid;
  uint32_t gid;
  uint64_t device;
  uint32_t block_size;
  uint32_t flags;
};

struct acyclic_fs_native_times {
  int64_t accessed_seconds;
  int64_t accessed_nanoseconds;
  int64_t modified_seconds;
  int64_t modified_nanoseconds;
};

extern int acyclic_fs_darwin_mount_getattr(uintptr_t context, const char *path, uint64_t handle,
                                     struct acyclic_fs_native_stat *result);
extern int acyclic_fs_darwin_mount_access(uintptr_t context, const char *path, int mask);
extern int acyclic_fs_darwin_mount_open(uintptr_t context, const char *path, int flags,
                                  uint64_t *handle);
extern int acyclic_fs_darwin_mount_create(uintptr_t context, const char *path, uint32_t mode,
                                    uint32_t uid, uint32_t gid, int flags, uint64_t *handle);
extern int acyclic_fs_darwin_mount_release(uintptr_t context, const char *path, uint64_t handle);
extern int acyclic_fs_darwin_mount_read(uintptr_t context, const char *path, uint64_t handle,
                                  char *buffer, size_t length, int64_t offset);
extern int acyclic_fs_darwin_mount_write(uintptr_t context, const char *path, uint64_t handle,
                                   const char *buffer, size_t length, int64_t offset);
extern int acyclic_fs_darwin_mount_truncate(uintptr_t context, const char *path, uint64_t handle,
                                      int64_t length);
extern int acyclic_fs_darwin_mount_flush(uintptr_t context, uint64_t handle);
extern int acyclic_fs_darwin_mount_opendir(uintptr_t context, const char *path, uint64_t *handle);
extern int acyclic_fs_darwin_mount_readdir(uintptr_t context, const char *path, void *buffer,
                                     fuse_fill_dir_t filler, int64_t offset, uint64_t handle);
extern int acyclic_fs_darwin_mount_releasedir(uintptr_t context, uint64_t handle);
extern int acyclic_fs_darwin_mount_mkdir(uintptr_t context, const char *path, uint32_t mode,
                                   uint32_t uid, uint32_t gid);
extern int acyclic_fs_darwin_mount_remove(uintptr_t context, const char *path, int directory);
extern int acyclic_fs_darwin_mount_rename(uintptr_t context, const char *source,
                                    const char *destination, uint32_t flags);
extern int acyclic_fs_darwin_mount_link(uintptr_t context, const char *source,
                                  const char *destination);
extern int acyclic_fs_darwin_mount_symlink(uintptr_t context, const char *target,
                                     const char *destination, uint32_t uid, uint32_t gid);
extern int acyclic_fs_darwin_mount_readlink(uintptr_t context, const char *path, char *buffer,
                                      size_t length);
extern int acyclic_fs_darwin_mount_mknod(uintptr_t context, const char *path, uint32_t mode,
                                   uint64_t device, uint32_t uid, uint32_t gid);
extern int acyclic_fs_darwin_mount_chmod(uintptr_t context, const char *path, uint32_t mode,
                                   uint64_t handle);
extern int acyclic_fs_darwin_mount_chown(uintptr_t context, const char *path, uint32_t uid,
                                   uint32_t gid, uint64_t handle);
extern int acyclic_fs_darwin_mount_utimens(uintptr_t context, const char *path,
                                     const struct acyclic_fs_native_times *times,
                                     uint64_t handle);
extern int acyclic_fs_darwin_mount_getxattr(uintptr_t context, const char *path, const char *name,
                                      char *value, size_t length);
extern int acyclic_fs_darwin_mount_setxattr(uintptr_t context, const char *path, const char *name,
                                      const char *value, size_t length, int flags);
extern int acyclic_fs_darwin_mount_listxattr(uintptr_t context, const char *path, char *list,
                                       size_t length);
extern int acyclic_fs_darwin_mount_removexattr(uintptr_t context, const char *path,
                                         const char *name);
extern int64_t acyclic_fs_darwin_mount_lseek(uintptr_t context, const char *path, uint64_t handle,
                                       int64_t offset, int whence);
extern int acyclic_fs_darwin_mount_fallocate(uintptr_t context, const char *path, uint64_t handle,
                                       int mode, int64_t offset, int64_t length);
extern int64_t acyclic_fs_darwin_mount_copy_file_range(
    uintptr_t context, const char *source, uint64_t source_handle, int64_t source_offset,
    const char *destination, uint64_t destination_handle, int64_t destination_offset,
    size_t length, int flags);

static uintptr_t current_context(void) {
  struct fuse_context *context = fuse_get_context();
  return context == NULL ? 0 : (uintptr_t)context->private_data;
}

static uint64_t file_handle(const struct fuse_file_info *info) {
  return info == NULL ? 0 : info->fh;
}

static void apply_stat(struct stat *target, const struct acyclic_fs_native_stat *source) {
  memset(target, 0, sizeof(*target));
  target->st_ino = (ino_t)source->inode;
  target->st_size = (off_t)source->logical_bytes;
  target->st_blocks = (blkcnt_t)source->blocks;
  target->st_atimespec.tv_sec = (time_t)source->accessed_seconds;
  target->st_atimespec.tv_nsec = (long)source->accessed_nanoseconds;
  target->st_mtimespec.tv_sec = (time_t)source->modified_seconds;
  target->st_mtimespec.tv_nsec = (long)source->modified_nanoseconds;
  target->st_ctimespec.tv_sec = (time_t)source->changed_seconds;
  target->st_ctimespec.tv_nsec = (long)source->changed_nanoseconds;
  target->st_birthtimespec.tv_sec = (time_t)source->created_seconds;
  target->st_birthtimespec.tv_nsec = (long)source->created_nanoseconds;
  target->st_mode = (mode_t)source->mode;
  target->st_nlink = (nlink_t)source->link_count;
  target->st_uid = (uid_t)source->uid;
  target->st_gid = (gid_t)source->gid;
  target->st_rdev = (dev_t)source->device;
  target->st_blksize = (blksize_t)source->block_size;
  target->st_flags = source->flags;
}

int acyclic_fs_darwin_mount_fill_directory(void *buffer, fuse_fill_dir_t filler, const char *name,
                                     const struct acyclic_fs_native_stat *attributes,
                                     int64_t next_offset) {
  struct stat native;
  struct stat *native_pointer = NULL;
  if (attributes != NULL) {
    apply_stat(&native, attributes);
    native_pointer = &native;
  }
  return filler(buffer, name, native_pointer, (off_t)next_offset);
}

static int bridge_getattr(const char *path, struct stat *result) {
  struct acyclic_fs_native_stat portable;
  int status = acyclic_fs_darwin_mount_getattr(current_context(), path, 0, &portable);
  if (status == 0) {
    apply_stat(result, &portable);
  }
  return status;
}

static int bridge_fgetattr(const char *path, struct stat *result,
                           struct fuse_file_info *info) {
  struct acyclic_fs_native_stat portable;
  int status =
      acyclic_fs_darwin_mount_getattr(current_context(), path, file_handle(info), &portable);
  if (status == 0) {
    apply_stat(result, &portable);
  }
  return status;
}

static int bridge_access(const char *path, int mask) {
  return acyclic_fs_darwin_mount_access(current_context(), path, mask);
}

static int bridge_open(const char *path, struct fuse_file_info *info) {
  return acyclic_fs_darwin_mount_open(current_context(), path, info->flags, &info->fh);
}

static int bridge_create(const char *path, mode_t mode, struct fuse_file_info *info) {
  struct fuse_context *context = fuse_get_context();
  return acyclic_fs_darwin_mount_create(current_context(), path, (uint32_t)mode, context->uid,
                                  context->gid, info->flags, &info->fh);
}

static int bridge_release(const char *path, struct fuse_file_info *info) {
  return acyclic_fs_darwin_mount_release(current_context(), path, info->fh);
}

static int bridge_read(const char *path, char *buffer, size_t length, off_t offset,
                       struct fuse_file_info *info) {
  return acyclic_fs_darwin_mount_read(current_context(), path, info->fh, buffer, length, offset);
}

static int bridge_write(const char *path, const char *buffer, size_t length, off_t offset,
                        struct fuse_file_info *info) {
  return acyclic_fs_darwin_mount_write(current_context(), path, info->fh, buffer, length, offset);
}

static int bridge_truncate(const char *path, off_t length) {
  return acyclic_fs_darwin_mount_truncate(current_context(), path, 0, length);
}

static int bridge_ftruncate(const char *path, off_t length, struct fuse_file_info *info) {
  return acyclic_fs_darwin_mount_truncate(current_context(), path, file_handle(info), length);
}

static int bridge_flush(const char *path, struct fuse_file_info *info) {
  (void)path;
  return acyclic_fs_darwin_mount_flush(current_context(), info->fh);
}

static int bridge_fsync(const char *path, int data_only, struct fuse_file_info *info) {
  (void)path;
  (void)data_only;
  return acyclic_fs_darwin_mount_flush(current_context(), info->fh);
}

static int bridge_opendir(const char *path, struct fuse_file_info *info) {
  return acyclic_fs_darwin_mount_opendir(current_context(), path, &info->fh);
}

static int bridge_readdir(const char *path, void *buffer, fuse_fill_dir_t filler, off_t offset,
                          struct fuse_file_info *info) {
  return acyclic_fs_darwin_mount_readdir(current_context(), path, buffer, filler, offset, info->fh);
}

static int bridge_releasedir(const char *path, struct fuse_file_info *info) {
  (void)path;
  return acyclic_fs_darwin_mount_releasedir(current_context(), info->fh);
}

static int bridge_mkdir(const char *path, mode_t mode) {
  struct fuse_context *context = fuse_get_context();
  return acyclic_fs_darwin_mount_mkdir(current_context(), path, (uint32_t)mode, context->uid,
                                 context->gid);
}

static int bridge_unlink(const char *path) {
  return acyclic_fs_darwin_mount_remove(current_context(), path, 0);
}

static int bridge_rmdir(const char *path) {
  return acyclic_fs_darwin_mount_remove(current_context(), path, 1);
}

static int bridge_rename(const char *source, const char *destination) {
  return acyclic_fs_darwin_mount_rename(current_context(), source, destination, 0);
}

static int bridge_renamex(const char *source, const char *destination, unsigned int flags) {
  return acyclic_fs_darwin_mount_rename(current_context(), source, destination, flags);
}

static int bridge_link(const char *source, const char *destination) {
  return acyclic_fs_darwin_mount_link(current_context(), source, destination);
}

static int bridge_symlink(const char *target, const char *destination) {
  struct fuse_context *context = fuse_get_context();
  return acyclic_fs_darwin_mount_symlink(current_context(), target, destination, context->uid,
                                   context->gid);
}

static int bridge_readlink(const char *path, char *buffer, size_t length) {
  return acyclic_fs_darwin_mount_readlink(current_context(), path, buffer, length);
}

static int bridge_mknod(const char *path, mode_t mode, dev_t device) {
  struct fuse_context *context = fuse_get_context();
  return acyclic_fs_darwin_mount_mknod(current_context(), path, (uint32_t)mode, (uint64_t)device,
                                 context->uid, context->gid);
}

static int bridge_chmod(const char *path, mode_t mode) {
  return acyclic_fs_darwin_mount_chmod(current_context(), path, (uint32_t)mode, 0);
}

static int bridge_chown(const char *path, uid_t uid, gid_t gid) {
  return acyclic_fs_darwin_mount_chown(current_context(), path, uid, gid, 0);
}

static int bridge_utimens(const char *path, const struct timespec times[2]) {
  struct acyclic_fs_native_times portable = {
      .accessed_seconds = times[0].tv_sec,
      .accessed_nanoseconds = times[0].tv_nsec,
      .modified_seconds = times[1].tv_sec,
      .modified_nanoseconds = times[1].tv_nsec,
  };
  return acyclic_fs_darwin_mount_utimens(current_context(), path, &portable, 0);
}

static int bridge_getxattr(const char *path, const char *name, char *value, size_t length,
                           uint32_t position) {
  (void)position;
  return acyclic_fs_darwin_mount_getxattr(current_context(), path, name, value, length);
}

static int bridge_setxattr(const char *path, const char *name, const char *value, size_t length,
                           int flags, uint32_t position) {
  (void)position;
  return acyclic_fs_darwin_mount_setxattr(current_context(), path, name, value, length, flags);
}

static int bridge_listxattr(const char *path, char *list, size_t length) {
  return acyclic_fs_darwin_mount_listxattr(current_context(), path, list, length);
}

static int bridge_removexattr(const char *path, const char *name) {
  return acyclic_fs_darwin_mount_removexattr(current_context(), path, name);
}

static int bridge_fallocate(const char *path, int mode, off_t offset, off_t length,
                            struct fuse_file_info *info) {
  return acyclic_fs_darwin_mount_fallocate(current_context(), path, info->fh, mode, offset, length);
}

static int bridge_statfs(const char *path, struct statvfs *result) {
  (void)path;
  if (statvfs("/", result) != 0) {
    return -errno;
  }
  return 0;
}

static void *bridge_init(struct fuse_conn_info *connection) {
  /* NFS file handles are path-independent. Without export support libfuse may
     forget the inode/path mapping that Darwin mount's NFS bridge must resolve. */
  if ((connection->capable & FUSE_CAP_EXPORT_SUPPORT) != 0) {
    connection->want |= FUSE_CAP_EXPORT_SUPPORT;
  }
  return fuse_get_context()->private_data;
}

static const struct fuse_operations bridge_operations = {
    .getattr = bridge_getattr,
    .access = bridge_access,
    .open = bridge_open,
    .create = bridge_create,
    .release = bridge_release,
    .read = bridge_read,
    .write = bridge_write,
    .truncate = bridge_truncate,
    .ftruncate = bridge_ftruncate,
    .fgetattr = bridge_fgetattr,
    .flush = bridge_flush,
    .fsync = bridge_fsync,
    .opendir = bridge_opendir,
    .readdir = bridge_readdir,
    .releasedir = bridge_releasedir,
    .mkdir = bridge_mkdir,
    .unlink = bridge_unlink,
    .rmdir = bridge_rmdir,
    .rename = bridge_rename,
    .renamex = bridge_renamex,
    .link = bridge_link,
    .symlink = bridge_symlink,
    .readlink = bridge_readlink,
    .mknod = bridge_mknod,
    .chmod = bridge_chmod,
    .chown = bridge_chown,
    .utimens = bridge_utimens,
    .getxattr = bridge_getxattr,
    .setxattr = bridge_setxattr,
    .listxattr = bridge_listxattr,
    .removexattr = bridge_removexattr,
    .fallocate = bridge_fallocate,
    .statfs = bridge_statfs,
    .init = bridge_init,
};

struct acyclic_fs_darwin_mount_session {
  pthread_mutex_t mutex;
  struct fuse *instance;
};

struct acyclic_fs_darwin_mount_session *acyclic_fs_darwin_mount_session_new(void) {
  struct acyclic_fs_darwin_mount_session *session = calloc(1, sizeof(*session));
  if (session == NULL) {
    return NULL;
  }
  if (pthread_mutex_init(&session->mutex, NULL) != 0) {
    free(session);
    return NULL;
  }
  return session;
}

void acyclic_fs_darwin_mount_session_free(struct acyclic_fs_darwin_mount_session *session) {
  if (session == NULL) {
    return;
  }
  pthread_mutex_destroy(&session->mutex);
  free(session);
}

int acyclic_fs_darwin_mount_run(struct acyclic_fs_darwin_mount_session *session, int argc, char **argv,
                          const char *mountpoint, uintptr_t context) {
  if (session == NULL) {
    return 4;
  }
  struct fuse_args arguments = FUSE_ARGS_INIT(argc, argv);
  struct fuse_chan *channel = fuse_mount(mountpoint, &arguments);
  if (channel == NULL) {
    return 2;
  }
  struct fuse *instance =
      fuse_new(channel, &arguments, &bridge_operations, sizeof(bridge_operations),
               (void *)context);
  if (instance == NULL) {
    fuse_unmount(mountpoint, channel);
    return 1;
  }
  pthread_mutex_lock(&session->mutex);
  if (session->instance != NULL) {
    pthread_mutex_unlock(&session->mutex);
    fuse_unmount(mountpoint, channel);
    fuse_destroy(instance);
    return 3;
  }
  session->instance = instance;
  pthread_mutex_unlock(&session->mutex);
  int status = fuse_loop_mt(instance);
  pthread_mutex_lock(&session->mutex);
  session->instance = NULL;
  pthread_mutex_unlock(&session->mutex);
  /* Rust owns bounded unmount and verifies that the kernel detached before
     interrupting this loop. Repeating umount here races concurrent teardown. */
  fuse_destroy(instance);
  return status;
}

int acyclic_fs_darwin_mount_invalidate(struct acyclic_fs_darwin_mount_session *session, const char *path) {
  if (session == NULL) {
    return -ESTALE;
  }
  pthread_mutex_lock(&session->mutex);
  struct fuse *instance = session->instance;
  (void)path;
  /* DarwinFUSE mounts with `noac`; there is no kernel-side entry cache to
     invalidate, so a live session is already coherent. */
  int status = instance == NULL ? -ESTALE : 0;
  pthread_mutex_unlock(&session->mutex);
  return status;
}

void acyclic_fs_darwin_mount_interrupt(struct acyclic_fs_darwin_mount_session *session) {
  if (session == NULL) {
    return;
  }
  pthread_mutex_lock(&session->mutex);
  struct fuse *instance = session->instance;
  if (instance != NULL) {
    fuse_exit(instance);
  }
  pthread_mutex_unlock(&session->mutex);
}
