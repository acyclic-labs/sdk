/*
 * DarwinFUSE — standard FUSE option parsing implementation
 *
 * Compatible with libfuse 2.x fuse_opt_parse() and friends.
 *
 * Copyright (c) 2026 Marcel Cotta. All rights reserved.
 * Licensed under the MIT License.
 */

#include <fuse_opt.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ---- Argument list management ---- */

int fuse_opt_add_arg(struct fuse_args *args, const char *arg)
{
    if (!args || !arg) return -1;

    char **new_argv = realloc(args->allocated ? args->argv : NULL,
                               (size_t)(args->argc + 2) * sizeof(char *));
    if (!new_argv) {
        /* If first alloc, need to copy existing argv */
        if (!args->allocated) {
            new_argv = malloc((size_t)(args->argc + 2) * sizeof(char *));
            if (!new_argv) return -1;
            for (int i = 0; i < args->argc; i++)
                new_argv[i] = strdup(args->argv[i]);
        } else {
            return -1;
        }
    } else if (!args->allocated) {
        /* realloc on non-allocated argv: need to copy existing strings */
        char **copy = malloc((size_t)(args->argc + 2) * sizeof(char *));
        if (!copy) { free(new_argv); return -1; }
        for (int i = 0; i < args->argc; i++)
            copy[i] = strdup(args->argv[i]);
        new_argv = copy;
    }

    new_argv[args->argc] = strdup(arg);
    if (!new_argv[args->argc]) return -1;
    args->argc++;
    new_argv[args->argc] = NULL;
    args->argv = new_argv;
    args->allocated = 1;
    return 0;
}

int fuse_opt_insert_arg(struct fuse_args *args, int pos, const char *arg)
{
    if (!args || !arg || pos < 0 || pos > args->argc)
        return -1;

    /* Add arg at end first */
    if (fuse_opt_add_arg(args, arg) < 0)
        return -1;

    /* Shift elements from pos to argc-2 right by 1 */
    char *new_arg = args->argv[args->argc - 1];
    for (int i = args->argc - 1; i > pos; i--)
        args->argv[i] = args->argv[i - 1];
    args->argv[pos] = new_arg;

    return 0;
}

void fuse_opt_free_args(struct fuse_args *args)
{
    if (!args) return;
    if (args->allocated && args->argv) {
        for (int i = 0; i < args->argc; i++)
            free(args->argv[i]);
        free(args->argv);
    }
    args->argv = NULL;
    args->argc = 0;
    args->allocated = 0;
}

int fuse_opt_add_opt(char **opts, const char *opt)
{
    if (!opts || !opt) return -1;

    if (*opts) {
        size_t old_len = strlen(*opts);
        size_t new_len = old_len + 1 + strlen(opt) + 1;
        char *new_str = realloc(*opts, new_len);
        if (!new_str) return -1;
        new_str[old_len] = ',';
        strcpy(new_str + old_len + 1, opt);
        *opts = new_str;
    } else {
        *opts = strdup(opt);
        if (!*opts) return -1;
    }
    return 0;
}

int fuse_opt_add_opt_escaped(char **opts, const char *opt)
{
    /* For now, same as add_opt — full escaping would handle commas */
    return fuse_opt_add_opt(opts, opt);
}

/* ---- Pattern matching ---- */

/*
 * Match a single option template against an argument.
 * Templates can be:
 *   "foo"        — exact match (boolean option)
 *   "foo="       — prefix match with value (foo=bar)
 *   "foo=%s"     — prefix match, value is a string
 *   "foo=%d"     — prefix match, value is an int
 *   "foo=%u"     — prefix match, value is unsigned
 *   "-x "        — single char flag with space-separated value
 *   "-x %s"      — single char flag with value
 */
static int match_template(const char *templ, const char *arg,
                           const char **val_out)
{
    if (!templ || !arg) return 0;

    /* Find if template has a value specifier */
    const char *perc = strchr(templ, '%');
    const char *eq   = strchr(templ, '=');

    if (perc) {
        /* Template has a format specifier like "foo=%s" or "-x %s" */
        size_t prefix_len;
        if (eq && eq < perc) {
            /* "foo=%s" style — match prefix including = */
            prefix_len = (size_t)(eq - templ) + 1;
        } else {
            /* "-x %s" style — match up to space before % */
            const char *space = strchr(templ, ' ');
            if (space && space < perc)
                prefix_len = (size_t)(space - templ);
            else
                prefix_len = (size_t)(perc - templ);
        }

        if (strncmp(templ, arg, prefix_len) == 0) {
            if (val_out) *val_out = arg + prefix_len;
            return 1;
        }
        return 0;
    }

    if (eq) {
        /* "foo=" style — prefix match */
        size_t prefix_len = (size_t)(eq - templ) + 1;
        if (strncmp(templ, arg, prefix_len) == 0) {
            if (val_out) *val_out = arg + prefix_len;
            return 1;
        }
        return 0;
    }

    /* Exact match */
    if (strcmp(templ, arg) == 0) {
        if (val_out) *val_out = NULL;
        return 1;
    }

    return 0;
}

int fuse_opt_match(const struct fuse_opt opts[], const char *opt)
{
    if (!opts || !opt) return 0;
    for (int i = 0; opts[i].templ; i++) {
        if (match_template(opts[i].templ, opt, NULL))
            return 1;
    }
    return 0;
}

/* ---- Main parser ---- */

/*
 * Apply a matched option: set the value at the specified offset
 * in the data structure, or return the key for callback processing.
 */
static int apply_opt(const struct fuse_opt *opt, void *data,
                     const char *val, const char *arg,
                     int key, fuse_opt_proc_t proc,
                     struct fuse_args *outargs)
{
    if (!opt) {
        /* No matching template found — pass to callback as KEY_OPT */
        if (proc)
            return proc(data, arg, FUSE_OPT_KEY_OPT, outargs);
        /* Default: keep the argument */
        return 1;
    }

    if (opt->offset == FUSE_OPT_KEY) {
        /* Key processing: pass to callback */
        if (proc)
            return proc(data, arg, opt->value, outargs);
        return 1;
    }

    /* Set value at offset */
    if (data) {
        void *dst = (char *)data + opt->offset;
        const char *perc = strchr(opt->templ, '%');
        if (perc) {
            char fmt = perc[1];
            switch (fmt) {
            case 's':
                *(const char **)dst = val ? val : arg;
                break;
            case 'd':
                *(int *)dst = val ? atoi(val) : opt->value;
                break;
            case 'u':
                *(unsigned *)dst = val ? (unsigned)strtoul(val, NULL, 10) : (unsigned)opt->value;
                break;
            case 'l':
                if (perc[2] == 'u')
                    *(unsigned long *)dst = val ? strtoul(val, NULL, 10) : (unsigned long)opt->value;
                else
                    *(long *)dst = val ? strtol(val, NULL, 10) : (long)opt->value;
                break;
            default:
                *(int *)dst = opt->value;
                break;
            }
        } else {
            /* Boolean-style: set to opt->value */
            *(int *)dst = opt->value;
        }
    }

    return 0;  /* consumed */
}

int fuse_opt_parse(struct fuse_args *args, void *data,
                   const struct fuse_opt opts[],
                   fuse_opt_proc_t proc)
{
    if (!args || !args->argv)
        return 0;

    struct fuse_args outargs = FUSE_ARGS_INIT(0, NULL);

    /* Always keep argv[0] (program name) */
    if (args->argc > 0)
        fuse_opt_add_arg(&outargs, args->argv[0]);

    for (int i = 1; i < args->argc; i++) {
        const char *arg = args->argv[i];
        int keep = 0;

        if (strcmp(arg, "-o") == 0 && i + 1 < args->argc) {
            /* -o option: parse comma-separated sub-options */
            i++;
            const char *optstr = args->argv[i];
            char buf[2048];
            strncpy(buf, optstr, sizeof(buf) - 1);
            buf[sizeof(buf) - 1] = '\0';

            char *saveptr = NULL;
            for (char *tok = strtok_r(buf, ",", &saveptr);
                 tok != NULL;
                 tok = strtok_r(NULL, ",", &saveptr))
            {
                const char *val = NULL;
                const struct fuse_opt *matched = NULL;

                if (opts) {
                    for (int j = 0; opts[j].templ; j++) {
                        if (match_template(opts[j].templ, tok, &val)) {
                            matched = &opts[j];
                            break;
                        }
                    }
                }

                int rc = apply_opt(matched, data, val, tok,
                                   matched ? matched->value : FUSE_OPT_KEY_OPT,
                                   proc, &outargs);
                if (rc < 0) {
                    fuse_opt_free_args(&outargs);
                    return -1;
                }
                if (rc == 1) {
                    /* Keep: add as -o tok */
                    fuse_opt_add_arg(&outargs, "-o");
                    fuse_opt_add_arg(&outargs, tok);
                }
            }
        } else if (arg[0] == '-') {
            /* Flag argument */
            const char *val = NULL;
            const struct fuse_opt *matched = NULL;

            if (opts) {
                for (int j = 0; opts[j].templ; j++) {
                    if (match_template(opts[j].templ, arg, &val)) {
                        matched = &opts[j];
                        break;
                    }
                }
            }

            /* Check if this flag takes a next-arg value */
            if (matched && strchr(matched->templ, ' ') && i + 1 < args->argc) {
                i++;
                val = args->argv[i];
            }

            int rc = apply_opt(matched, data, val, arg,
                               matched ? matched->value : FUSE_OPT_KEY_OPT,
                               proc, &outargs);
            if (rc < 0) {
                fuse_opt_free_args(&outargs);
                return -1;
            }
            if (rc == 1) {
                keep = 1;
            }
        } else {
            /* Non-option argument */
            int rc;
            if (proc) {
                rc = proc(data, arg, FUSE_OPT_KEY_NONOPT, &outargs);
            } else {
                rc = 1;  /* keep by default */
            }
            if (rc < 0) {
                fuse_opt_free_args(&outargs);
                return -1;
            }
            if (rc == 1) {
                keep = 1;
            }
        }

        if (keep)
            fuse_opt_add_arg(&outargs, arg);
    }

    /* Replace original args with filtered output */
    if (args->allocated) {
        for (int i = 0; i < args->argc; i++)
            free(args->argv[i]);
        free(args->argv);
    }
    *args = outargs;

    return 0;
}
