#ifdef __linux__
#define _GNU_SOURCE
#endif
#include <errno.h>
#include <pthread.h>
#include <poll.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

struct input_ops {
    int fd;
    void *context;
    unsigned char *(*acquire)(void *, size_t *);
    int (*publish)(void *, intptr_t, int);
    void (*finish)(void *);
};

struct input_thread {
    pthread_t thread;
    struct input_ops ops;
};

static void *run_input(void *argument) {
    struct input_ops *ops = &((struct input_thread *)argument)->ops;
#ifdef __APPLE__
    pthread_setname_np("harness-stdin");
#elif defined(__linux__)
    pthread_setname_np(pthread_self(), "harness-stdin");
#endif
    /* All Rust callbacks execute with cancellation disabled. Cancellation
       can unwind only this C stack, never a Rust callback or owned value. */
    pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
    for (;;) {
        size_t length;
        unsigned char *bytes = ops->acquire(ops->context, &length);
        if (bytes == NULL) break;
        pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
        pthread_testcancel();
        ssize_t count;
        int error = 0;
        for (;;) {
            count = read(ops->fd, bytes, length);
            if (count >= 0) { error = 0; break; }
            error = errno;
            if (error == EINTR) continue;
            if (error != EAGAIN && error != EWOULDBLOCK) break;
            struct pollfd source = { ops->fd, POLLIN, 0 };
            int ready;
            do { ready = poll(&source, 1, -1); } while (ready < 0 && errno == EINTR);
            if (ready < 0) { error = errno; break; }
            /* An unwatchable device cannot wake poll after WouldBlock. */
            if (source.revents & POLLNVAL) break;
        }
        pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
        if (!ops->publish(ops->context, (intptr_t)count, error)) break;
    }
    ops->finish(ops->context);
    return NULL;
}

int acyclic_input_start(const struct input_ops *ops, uintptr_t *handle) {
    struct input_thread *input = malloc(sizeof(*input));
    if (input == NULL) return ENOMEM;
    input->ops = *ops;
    int error = pthread_create(&input->thread, NULL, run_input, input);
    if (error != 0) { free(input); return error; }
    *handle = (uintptr_t)input;
    return 0;
}

int acyclic_input_stop(uintptr_t handle) {
    struct input_thread *input = (struct input_thread *)handle;
    /* A request before read admission stays pending; an admitted read is a
       cancellation point. Join fences every write to the Rust-owned buffer. */
    pthread_cancel(input->thread);
    int error = pthread_join(input->thread, NULL);
    if (error == 0) free(input);
    return error;
}
