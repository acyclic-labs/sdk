#include "darwin_mount_owner.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <spawn.h>
#include <stdio.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

static int write_image(const char *path, const unsigned char *image, size_t length) {
    int fd = open(path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0700);
    if (fd < 0) return errno;
    int error = 0;
    while (length) {
        ssize_t written = write(fd, image, length);
        if (written < 0 && errno == EINTR) continue;
        if (written <= 0) {
            error = written < 0 ? errno : EIO;
            break;
        }
        image += written;
        length -= (size_t)written;
    }
    if (close(fd) < 0 && !error) error = errno;
    if (error) (void)unlink(path);
    return error;
}

static int spawn_owner(pid_t *pid, const char *path, int control, int fence,
                       const int *holds, size_t hold_count,
                       const char *options, const char *source,
                       const char *destination, const char *socket_path) {
    char process[32], descriptor[32];
    snprintf(process, sizeof(process), "%d", getpid());
    snprintf(descriptor, sizeof(descriptor), "%d", control);
    char *const args[] = {(char *)path, process, descriptor, (char *)options,
                          (char *)source, (char *)destination, (char *)socket_path, NULL};
    char *const environment[] = {NULL};
    posix_spawn_file_actions_t actions;
    posix_spawnattr_t attributes;
    int error = posix_spawn_file_actions_init(&actions);
    if (error) return error;
    error = posix_spawnattr_init(&attributes);
    if (!error) {
        /* The custodian survives the owner's process group. It inherits only
         * the original control endpoint, fence, the enclosing process trees'
         * custody holds, which keep those trees unretired until it exits, and
         * optional diagnostics. */
        error = posix_spawnattr_setflags(&attributes,
                    POSIX_SPAWN_CLOEXEC_DEFAULT | POSIX_SPAWN_SETPGROUP);
        if (!error) error = posix_spawnattr_setpgroup(&attributes, 0);
        if (!error) error = posix_spawn_file_actions_addinherit_np(&actions, control);
        if (!error && fence >= 0)
            error = posix_spawn_file_actions_addinherit_np(&actions, fence);
        for (size_t i = 0; !error && i < hold_count; i++)
            error = posix_spawn_file_actions_addinherit_np(&actions, holds[i]);
        if (!error && fcntl(STDERR_FILENO, F_GETFD) >= 0)
            error = posix_spawn_file_actions_addinherit_np(&actions, STDERR_FILENO);
        if (!error)
            error = posix_spawn(pid, path, &actions, &attributes, args, environment);
        posix_spawnattr_destroy(&attributes);
    }
    posix_spawn_file_actions_destroy(&actions);
    return error;
}

int acyclic_mount_owner_init(struct acyclic_mount_owner *owner) {
    atomic_init(&owner->pid, 0);
    int pair[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, pair) < 0) return errno;
    int enabled = 1;
    if (fcntl(pair[0], F_SETFD, FD_CLOEXEC) < 0 ||
        fcntl(pair[1], F_SETFD, FD_CLOEXEC) < 0 ||
        setsockopt(pair[1], SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled)) < 0) {
        int error = errno;
        (void)close(pair[0]);
        (void)close(pair[1]);
        return error;
    }
    owner->control = pair[0];
    owner->peer = pair[1];
    return 0;
}

void acyclic_mount_owner_destroy(struct acyclic_mount_owner *owner) {
    (void)close(owner->control);
    if (owner->peer >= 0) (void)close(owner->peer);
}

void acyclic_mount_owner_release(const struct acyclic_mount_owner *owner) {
    /* Wake the custodian even if a concurrent fork duplicated this endpoint. */
    (void)shutdown(owner->control, SHUT_WR);
}

int acyclic_mount_owner_wait(struct acyclic_mount_owner *owner) {
    pid_t pid = owner->pid;
    if (!pid) return 0;
    int status;
    pid_t reaped;
    do {
        reaped = waitpid(pid, &status, 0);
    } while (reaped < 0 && errno == EINTR);
    owner->pid = 0;
    if (reaped < 0) return errno;
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) return 0;
    if (WIFSIGNALED(status))
        fprintf(stderr, "Mount custodian terminated by signal %d\n", WTERMSIG(status));
    else if (WIFEXITED(status))
        fprintf(stderr, "Mount custodian exited with status %d\n", WEXITSTATUS(status));
    return EIO;
}

int acyclic_mount_owner_start(struct acyclic_mount_owner *owner,
                              const unsigned char *image, size_t image_len,
                              int fence, const int *holds, size_t hold_count,
                              const char *options, const char *source,
                              const char *destination, const char *socket_path) {
    if (owner->pid || !image || !image_len) return EINVAL;
    /* Only this server has access to its mkdtemp directory. */
    char path[PATH_MAX];
    int length = snprintf(path, sizeof(path), "%s-owner", socket_path);
    if (length < 0 || (size_t)length >= sizeof(path)) return ENAMETOOLONG;
    int error = write_image(path, image, image_len);
    if (error) return error;
    pid_t pid;
    error = spawn_owner(&pid, path, owner->peer, fence, holds, hold_count,
                        options, source, destination, socket_path);
    (void)close(owner->peer);
    owner->peer = -1;
    if (error) {
        (void)unlink(path);
        return error;
    }
    owner->pid = pid;
    ssize_t received;
    do {
        received = recv(owner->control, &error, sizeof(error), MSG_WAITALL);
    } while (received < 0 && errno == EINTR);
    if (received != (ssize_t)sizeof(error)) error = received < 0 ? errno : EIO;
    if (error) {
        acyclic_mount_owner_release(owner);
        (void)acyclic_mount_owner_wait(owner);
        (void)unlink(path);
        return error;
    }
    return 0;
}
