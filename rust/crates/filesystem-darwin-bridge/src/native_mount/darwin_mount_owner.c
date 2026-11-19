/* The NFS callback server lives in the SDK process. This separate process
 * owns the physical mount and retains its original destination fence. */
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <signal.h>
#include <spawn.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/event.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

int acyclic_fs_darwin_retire_mount(const char *destination, int *dissent);

static int number(const char *text) {
    char *end;
    errno = 0;
    long value = strtol(text, &end, 10);
    return errno || end == text || *end || value < 0 || value > INT_MAX ? -1 : (int)value;
}

/* A cold kernel-table lookup never opens a root whose NFS server is gone.
 * The source socket and destination must both identify this mount. */
static int find_mount(const char *source, const char *destination, bool *present) {
    struct statfs *mounts = NULL;
    int count = getmntinfo(&mounts, MNT_NOWAIT);
    if (count <= 0 || mounts == NULL) return errno ? errno : EIO;
    *present = false;
    for (int i = 0; i < count; i++) {
        if (mounts[i].f_owner == getuid() &&
            strcmp(mounts[i].f_fstypename, "nfs") == 0 &&
            strcmp(mounts[i].f_mntfromname, source) == 0 &&
            strcmp(mounts[i].f_mntonname, destination) == 0) {
            *present = true;
            break;
        }
    }
    return 0;
}

static void remove_private_socket(const char *socket_path) {
    /* This private socket and its parent were created for this one server. */
    (void)unlink(socket_path);
    char directory[PATH_MAX];
    size_t length = strlen(socket_path);
    if (length < sizeof(directory)) {
        memcpy(directory, socket_path, length + 1);
        char *leaf = strrchr(directory, '/');
        if (leaf != NULL) {
            *leaf = '\0';
            (void)rmdir(directory);
        }
    }
}

static int retire(const char *source, const char *destination, const char *socket_path) {
    bool present;
    int error = find_mount(source, destination, &present);
    if (error) return error;
    if (present) {
        int dissent = 0;
        int status = acyclic_fs_darwin_retire_mount(destination, &dissent);
        error = find_mount(source, destination, &present);
        if (error) return error;
        if (present) {
            fprintf(stderr, "NFS mount retirement failed: %s (Disk Arbitration status %d)\n",
                    strerror(status ? status : EBUSY), dissent);
            return status ? status : EBUSY;
        }
    }
    remove_private_socket(socket_path);
    return 0;
}

static int child_status(pid_t child) {
    int status;
    pid_t reaped;
    do {
        reaped = waitpid(child, &status, 0);
    } while (reaped < 0 && errno == EINTR);
    if (reaped < 0) return errno;
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) return 0;
    if (WIFSIGNALED(status))
        fprintf(stderr, "mount_nfs terminated by signal %d\n", WTERMSIG(status));
    else if (WIFEXITED(status))
        fprintf(stderr, "mount_nfs exited with status %d\n", WEXITSTATUS(status));
    return EIO;
}

static int next_event(int queue, struct kevent *event) {
    int received;
    do {
        received = kevent(queue, NULL, 0, event, 1, NULL);
    } while (received < 0 && errno == EINTR);
    return received == 1 ? 0 : received < 0 ? errno : EIO;
}

static int mount_nfs(int queue, const char *options, const char *source,
                     const char *destination) {
    posix_spawnattr_t attributes;
    int error = posix_spawnattr_init(&attributes);
    if (error) return error;
    char *const args[] = {"mount_nfs", "-o", (char *)options, (char *)source,
                          (char *)destination, NULL};
    char *const environment[] = {NULL};
    pid_t child;
    posix_spawn_file_actions_t actions;
    error = posix_spawn_file_actions_init(&actions);
    if (!error) {
        error = posix_spawnattr_setflags(&attributes, POSIX_SPAWN_CLOEXEC_DEFAULT);
        if (!error && fcntl(STDERR_FILENO, F_GETFD) >= 0)
            error = posix_spawn_file_actions_addinherit_np(&actions, STDERR_FILENO);
        if (!error)
            error = posix_spawn(&child, "/sbin/mount_nfs", &actions, &attributes, args,
                                environment);
        posix_spawn_file_actions_destroy(&actions);
    }
    posix_spawnattr_destroy(&attributes);
    if (error) return error;

    struct kevent event;
    EV_SET(&event, (uintptr_t)child, EVFILT_PROC, EV_ADD | EV_ONESHOT, NOTE_EXIT, 0, NULL);
    if (kevent(queue, &event, 1, NULL, 0, NULL) < 0) {
        error = errno;
        if (error == ESRCH) return child_status(child);
    } else {
        error = next_event(queue, &event);
        if (!error && event.filter == EVFILT_PROC && event.ident == (uintptr_t)child)
            return child_status(child);
        if (!error) error = ECANCELED;
    }
    /* Death or release can arrive before mount_nfs returns. Reap our exact
     * child before retiring the namespace so it cannot attach afterward. */
    (void)kill(child, SIGKILL);
    (void)child_status(child);
    return error;
}

int main(int argc, char **argv) {
    if (argc != 7) return 1;
    pid_t parent = number(argv[1]);
    int control = number(argv[2]);
    const char *options = argv[3], *source = argv[4], *destination = argv[5];
    const char *socket_path = argv[6];
    (void)unlink(argv[0]);
    if (parent <= 0 || control < 0) return 1;
    int queue = kqueue();
    int status = 0;
    struct kevent changes[2];
    EV_SET(&changes[0], (uintptr_t)parent, EVFILT_PROC, EV_ADD | EV_ONESHOT, NOTE_EXIT, 0, NULL);
    EV_SET(&changes[1], (uintptr_t)control, EVFILT_READ, EV_ADD | EV_ONESHOT, 0, 0, NULL);
    if (queue < 0)
        status = errno;
    else if (getppid() != parent)
        status = ESRCH;
    else if (kevent(queue, changes, 2, NULL, 0, NULL) < 0)
        status = errno;
    else if (getppid() != parent)
        status = ESRCH;
    if (status) {
        /* The server never waits for a mount without an armed owner watch. */
        fprintf(stderr, "Could not watch the NFS mount owner: %s\n", strerror(status));
        (void)send(control, &status, sizeof(status), 0);
        remove_private_socket(socket_path);
        return 1;
    }

    status = mount_nfs(queue, options, source, destination);
    if (!status) {
        bool present;
        status = find_mount(source, destination, &present);
        if (!status && !present) {
            fprintf(stderr, "mount_nfs exited successfully without its expected NFS mount\n");
            status = EIO;
        }
    }
    if (send(control, &status, sizeof(status), 0) == (ssize_t)sizeof(status) && !status) {
        struct kevent event;
        /* No commands are written into the socket. Readability is release;
         * NOTE_EXIT independently fences the original process's lifetime. */
        (void)next_event(queue, &event);
    }
    return retire(source, destination, socket_path) == 0 ? 0 : 1;
}
