/* Instrument the production translation unit; no test branches in its loop. */
#ifdef __linux__
#define _GNU_SOURCE
#endif
#include <errno.h>
#include <pthread.h>
#include <poll.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

struct counts {
    uint64_t reads, polls, acquires, publishes, finishes;
    intptr_t last_count;
    int error;
    unsigned char bytes[64];
    size_t length;
};
struct fixture {
    struct counts *counts;
    int redundant_poll, would_block;
    unsigned char byte;
};
static _Thread_local struct fixture *current;
static int counted_poll(struct pollfd *fds, nfds_t count, int timeout) {
    current->counts->polls++;
    return poll(fds, count, timeout);
}
static ssize_t counted_read(int fd, void *bytes, size_t length) {
    current->counts->reads++;
    if (current->redundant_poll) {
        struct pollfd source = { fd, POLLIN, 0 };
        counted_poll(&source, 1, -1);
    }
    if (current->would_block) {
        current->would_block = 0;
        errno = EAGAIN;
        return -1;
    }
    return read(fd, bytes, length);
}
#define read counted_read
#define poll counted_poll
#define acyclic_input_start counts_input_start
#define acyclic_input_stop counts_input_stop
#include "input_reader.c"
#undef read
#undef poll

static unsigned char *count_acquire(void *context, size_t *length) {
    struct fixture *fixture = context;
    fixture->counts->acquires++;
    *length = 1;
    return &fixture->byte;
}
static int count_publish(void *context, intptr_t count, int error) {
    struct fixture *fixture = context;
    struct counts *result = fixture->counts;
    result->publishes++;
    result->last_count = count;
    result->error = error;
    if (count > 0 && result->length < sizeof(result->bytes))
        result->bytes[result->length++] = fixture->byte;
    return count > 0 && error == 0;
}
static void count_finish(void *context) {
    ((struct fixture *)context)->counts->finishes++;
}
void acyclic_input_counts(int fd, int redundant_poll, int would_block,
                          struct counts *result) {
    struct fixture fixture = { result, redundant_poll, would_block, 0 };
    struct input_thread input = { .ops = {
        fd, &fixture, count_acquire, count_publish, count_finish
    }};
    current = &fixture;
    int previous;
    pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, &previous);
    run_input(&input);
    pthread_setcancelstate(previous, NULL);
    current = NULL;
}
