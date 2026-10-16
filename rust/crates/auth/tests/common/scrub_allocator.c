// Linux/glibc test interposer: inspect live allocations immediately before free.
#define _GNU_SOURCE
#include <malloc.h>
#include <stdlib.h>
#include <unistd.h>

extern void __libc_free(void *);
static const char *active;
static unsigned zeroed;

__attribute__((constructor)) static void setup(void) {
    active = getenv("AUTH_SCRUB_ACTIVE");
}

void free(void *ptr) {
    if (ptr && active && access(active, F_OK) == 0) {
        const unsigned char *bytes = ptr;
        size_t len = malloc_usable_size(ptr);
        size_t run = 0;
        for (size_t i = 0; i < len; ++i) {
            run = bytes[i] == 4 ? run + 1 : 0;
            if (run == 32) _exit(86);
        }
        if (len >= 32) {
            size_t i = 0;
            while (i < 32 && bytes[i] == 0) ++i;
            if (i == 32) ++zeroed;
        }
    }
    __libc_free(ptr);
}

__attribute__((destructor)) static void finish(void) {
    if (active && access(active, F_OK) == 0 && !zeroed) _exit(87);
}
