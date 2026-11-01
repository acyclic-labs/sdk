// Linux/glibc test interposer: inspect live allocations immediately before free.
#define _GNU_SOURCE
#include <malloc.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

extern void __libc_free(void *);
extern void *__libc_malloc(size_t);
extern void *__libc_realloc(void *, size_t);
static const char *active;
static unsigned char marker = 4;
static _Atomic unsigned zeroed;

__attribute__((constructor)) static void setup(void) {
    active = getenv("AUTH_SCRUB_ACTIVE");
    const char *value = getenv("AUTH_SCRUB_BYTE");
    if (value) marker = (unsigned char)strtoul(value, NULL, 10);
}

void free(void *ptr) {
    if (ptr && active && access(active, F_OK) == 0) {
        const unsigned char *bytes = ptr;
        size_t len = malloc_usable_size(ptr);
        size_t run = 0;
        for (size_t i = 0; i < len; ++i) {
            run = bytes[i] == marker ? run + 1 : 0;
            if (run == 32) _exit(86);
        }
        if (len >= 32) {
            size_t i = 0;
            while (i < 32 && bytes[i] == 0) ++i;
            if (i == 32) atomic_fetch_add_explicit(&zeroed, 1, memory_order_relaxed);
        }
    }
    __libc_free(ptr);
}

// A valid allocator may relocate every growth. Force that schedule while the
// oracle is active, so an in-place realloc cannot conceal an unscrubbed free.
void *realloc(void *ptr, size_t size) {
    if (!ptr || !active || access(active, F_OK) != 0)
        return __libc_realloc(ptr, size);
    if (!size) { free(ptr); return NULL; }
    void *next = __libc_malloc(size);
    if (!next) return NULL;
    size_t previous = malloc_usable_size(ptr);
    memcpy(next, ptr, previous < size ? previous : size);
    free(ptr);
    return next;
}

__attribute__((destructor)) static void finish(void) {
    if (active && access(active, F_OK) == 0 &&
        !atomic_load_explicit(&zeroed, memory_order_relaxed)) _exit(87);
}
