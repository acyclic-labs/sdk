/*
 * DarwinFUSE — standard FUSE option parsing API
 *
 * Compatible with libfuse 2.x fuse_opt.h interface.
 *
 * Copyright (c) 2026 Marcel Cotta. All rights reserved.
 * Licensed under the MIT License.
 */

#ifndef DARWINFUSE_FUSE_OPT_H
#define DARWINFUSE_FUSE_OPT_H

#ifdef __cplusplus
extern "C" {
#endif

/* Argument vector */
struct fuse_args {
    int     argc;
    char  **argv;
    int     allocated;  /* whether argv was allocated by fuse_opt */
};

#define FUSE_ARGS_INIT(argc, argv) { argc, argv, 0 }

/* Option template */
struct fuse_opt {
    const char *templ;     /* e.g. "--foo=%s", "-n %u", "bar" */
    unsigned long offset;  /* offsetof(struct, member), or -1 for key */
    int value;             /* value to set, or key value */
};

/* Special offset for key processing */
#define FUSE_OPT_KEY  ((unsigned long)-1)

/* End-of-list sentinel */
#define FUSE_OPT_END  { NULL, 0, 0 }

/* Special key values passed to the processing function */
#define FUSE_OPT_KEY_OPT      -1  /* unmatched option */
#define FUSE_OPT_KEY_NONOPT   -2  /* non-option argument */
#define FUSE_OPT_KEY_KEEP      1  /* keep argument in output */
#define FUSE_OPT_KEY_DISCARD   0  /* discard argument */

/*
 * Processing function callback.
 * data:   user data pointer from fuse_opt_parse
 * arg:    the current argument string
 * key:    FUSE_OPT_KEY_OPT, FUSE_OPT_KEY_NONOPT, or a user key
 * outargs: output argument list (can add args to it)
 *
 * Return: -1 on error, 0 to discard arg, 1 to keep arg
 */
typedef int (*fuse_opt_proc_t)(void *data, const char *arg,
                                int key, struct fuse_args *outargs);

/*
 * Parse arguments according to option templates.
 * Returns 0 on success, -1 on error.
 */
int fuse_opt_parse(struct fuse_args *args, void *data,
                   const struct fuse_opt opts[],
                   fuse_opt_proc_t proc);

/*
 * Add an argument to the args list.
 * Returns 0 on success, -1 on error.
 */
int fuse_opt_add_arg(struct fuse_args *args, const char *arg);

/*
 * Insert an argument at the given position.
 * Returns 0 on success, -1 on error.
 */
int fuse_opt_insert_arg(struct fuse_args *args, int pos, const char *arg);

/*
 * Free the argument list (if allocated by fuse_opt).
 */
void fuse_opt_free_args(struct fuse_args *args);

/*
 * Add a "-o opt" option to the args list.
 * Returns 0 on success, -1 on error.
 */
int fuse_opt_add_opt(char **opts, const char *opt);

/*
 * Add a "-o opt" option, escaping commas.
 * Returns 0 on success, -1 on error.
 */
int fuse_opt_add_opt_escaped(char **opts, const char *opt);

/*
 * Match a pattern against an argument.
 * Returns 1 if matching, 0 otherwise.
 */
int fuse_opt_match(const struct fuse_opt opts[], const char *opt);

#ifdef __cplusplus
}
#endif

#endif /* DARWINFUSE_FUSE_OPT_H */
