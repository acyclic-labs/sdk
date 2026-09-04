/*
 * DarwinFUSE — NFSv4 TCP server
 *
 * Single-threaded poll()-based event loop serving NFSv4 COMPOUND requests
 * over TCP on localhost. Designed for the simple use case of a FUSE
 * filesystem replacement where only the macOS NFS client connects.
 *
 * Copyright (c) 2026 Marcel Cotta. All rights reserved.
 * Licensed under the MIT License.
 */

#include "nfs4_server.h"
#include "nfs4_ops.h"
#include "nfs4_xdr.h"
#include "rpc.h"
#include "darwinfuse_internal.h"
#include "fuse_context.h"
#include "inode_table.h"

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <stdatomic.h>

/* ---- Client connection ---- */

typedef enum {
    CLIENT_STATE_READ_MARK,     /* Reading 4-byte record mark */
    CLIENT_STATE_READ_PAYLOAD   /* Reading payload bytes */
} client_read_state_t;

typedef struct {
    int                 fd;
    client_read_state_t read_state;
    uint8_t             mark_buf[4];
    size_t              mark_read;      /* bytes of record mark read so far */
    uint8_t            *payload_buf;
    size_t              payload_len;    /* expected payload length */
    size_t              payload_read;   /* bytes of payload read so far */
    int                 last_fragment;
    nfs4_conn_state_t   nfs_state;
    pthread_mutex_t     write_lock;     /* serialize reply writes */
    _Atomic int         inflight;       /* in-flight work items (MT mode) */
    int                 closing;        /* disconnect pending (MT mode) */
} client_conn_t;

/* ---- Work queue for thread pool ---- */

typedef struct nfs4_work_item {
    client_conn_t         *client;
    uint8_t               *payload;       /* owned — freed by worker */
    size_t                 payload_len;
    struct nfs4_work_item *next;
} nfs4_work_item_t;

typedef struct {
    pthread_mutex_t  lock;
    pthread_cond_t   not_empty;
    nfs4_work_item_t *head;
    nfs4_work_item_t *tail;
    int              count;
    int              shutdown;
} work_queue_t;

/* ---- Server state ---- */

struct darwinfuse_server {
    darwinfuse_config_t config;
    int                 listen_fd;
    int                 wakeup_pipe[2]; /* self-pipe for stop signal */
    volatile int        running;
    int                 had_client;     /* true once a client connected */

    client_conn_t       clients[DFUSE_MAX_CLIENTS];
    int                 num_clients;

    /* Thread pool (enabled by nfs4_server_set_multithreaded) */
    int              multithreaded;
    int              num_threads;
    pthread_t       *worker_threads;
    work_queue_t     work_queue;
    void            *private_data;  /* propagated to worker TLS */
};

/* ---- Helpers ---- */

static void set_nonblocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags >= 0)
        fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static void set_tcp_nodelay(int fd)
{
    int flag = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
}

static void client_init(client_conn_t *c, int fd)
{
    memset(c, 0, sizeof(*c));
    c->fd = fd;
    c->read_state = CLIENT_STATE_READ_MARK;

    /* Allocate initial open file tracking array */
    c->nfs_state.open_files = calloc(OPEN_FILES_INITIAL_CAP,
                                      sizeof(nfs4_open_file_t));
    c->nfs_state.open_file_cap = OPEN_FILES_INITIAL_CAP;
    c->nfs_state.open_file_count = 0;

    /* Initialize mutexes */
    pthread_mutex_init(&c->nfs_state.lock, NULL);
    pthread_mutex_init(&c->write_lock, NULL);

    /* MT state */
    atomic_store(&c->inflight, 0);
    c->closing = 0;
}

static void client_close(client_conn_t *c)
{
    if (c->fd >= 0) {
        close(c->fd);
        c->fd = -1;
    }
    free(c->payload_buf);
    c->payload_buf = NULL;
    free(c->nfs_state.open_files);
    c->nfs_state.open_files = NULL;
    c->nfs_state.open_file_count = 0;
    c->nfs_state.open_file_cap = 0;

    /* Destroy mutexes */
    pthread_mutex_destroy(&c->nfs_state.lock);
    pthread_mutex_destroy(&c->write_lock);
}

/* ---- Work queue operations ---- */

static void work_queue_init(work_queue_t *wq)
{
    memset(wq, 0, sizeof(*wq));
    pthread_mutex_init(&wq->lock, NULL);
    pthread_cond_init(&wq->not_empty, NULL);
}

static void work_queue_destroy(work_queue_t *wq)
{
    nfs4_work_item_t *item = wq->head;
    while (item) {
        nfs4_work_item_t *next = item->next;
        free(item->payload);
        free(item);
        item = next;
    }
    pthread_mutex_destroy(&wq->lock);
    pthread_cond_destroy(&wq->not_empty);
}

static void work_queue_push(work_queue_t *wq, nfs4_work_item_t *item)
{
    pthread_mutex_lock(&wq->lock);
    item->next = NULL;
    if (wq->tail)
        wq->tail->next = item;
    else
        wq->head = item;
    wq->tail = item;
    wq->count++;
    pthread_cond_signal(&wq->not_empty);
    pthread_mutex_unlock(&wq->lock);
}

/* Returns NULL on shutdown (after draining remaining items). */
static nfs4_work_item_t *work_queue_pop(work_queue_t *wq)
{
    pthread_mutex_lock(&wq->lock);
    while (!wq->head && !wq->shutdown)
        pthread_cond_wait(&wq->not_empty, &wq->lock);
    if (wq->shutdown && !wq->head) {
        pthread_mutex_unlock(&wq->lock);
        return NULL;
    }
    nfs4_work_item_t *item = wq->head;
    wq->head = item->next;
    if (!wq->head)
        wq->tail = NULL;
    wq->count--;
    pthread_mutex_unlock(&wq->lock);
    return item;
}

static void work_queue_shutdown(work_queue_t *wq)
{
    pthread_mutex_lock(&wq->lock);
    wq->shutdown = 1;
    pthread_cond_broadcast(&wq->not_empty);
    pthread_mutex_unlock(&wq->lock);
}

/* ---- RPC message processing (shared by ST and MT paths) ---- */

/*
 * Process an RPC message and produce a reply buffer.
 * On success, sets *out_buf (caller must free) and *out_len.
 * Returns 0 on success, -1 on error.
 */
static int process_rpc_message(darwinfuse_server_t *srv, client_conn_t *c,
                                uint8_t *payload, size_t payload_len,
                                uint8_t **out_buf, size_t *out_len)
{
    xdr_buf_t req;
    xdr_init(&req, payload, payload_len);

    rpc_call_header_t rpc_hdr;
    if (rpc_parse_call(&req, &rpc_hdr) < 0) {
        DFUSE_ERR("Failed to parse RPC call");
        return -1;
    }

    uint8_t *reply_buf = malloc(DFUSE_XDR_MAXBUF + 4);
    if (!reply_buf)
        return -1;

    xdr_buf_t rep;
    xdr_init(&rep, reply_buf + 4, DFUSE_XDR_MAXBUF);

    if (rpc_hdr.program != NFS_PROGRAM) {
        rpc_encode_reply_error(&rep, rpc_hdr.xid, ACCEPT_PROG_UNAVAIL);
    } else if (rpc_hdr.version != NFS_V4) {
        rpc_encode_reply_error(&rep, rpc_hdr.xid, ACCEPT_PROG_MISMATCH);
    } else if (rpc_hdr.procedure == NFSPROC4_NULL) {
        rpc_encode_reply_accepted(&rep, rpc_hdr.xid);
    } else if (rpc_hdr.procedure == NFSPROC4_COMPOUND) {
        darwinfuse_set_context(rpc_hdr.cred_uid, rpc_hdr.cred_gid);
        rpc_encode_reply_accepted(&rep, rpc_hdr.xid);

        nfs4_request_ctx_t ctx;
        memset(&ctx, 0, sizeof(ctx));
        if (nfs4_dispatch_compound(&srv->config, &c->nfs_state,
                                    &ctx, &req, &rep) < 0) {
            DFUSE_ERR("COMPOUND dispatch failed");
            free(reply_buf);
            return -1;
        }
    } else {
        rpc_encode_reply_error(&rep, rpc_hdr.xid, ACCEPT_PROC_UNAVAIL);
    }

    uint32_t reply_len = (uint32_t)xdr_getpos(&rep);
    rpc_encode_record_mark(reply_buf, reply_len, 1);

    *out_buf = reply_buf;
    *out_len = 4 + reply_len;
    return 0;
}

/* Write a reply buffer to a client fd. Returns 0 on success, -1 on error. */
static int write_reply(int fd, const uint8_t *buf, size_t len)
{
    size_t written = 0;
    while (written < len) {
        ssize_t n = write(fd, buf + written, len - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                usleep(100);
                continue;
            }
            DFUSE_ERR("write failed: %s", strerror(errno));
            return -1;
        }
        written += (size_t)n;
    }
    return 0;
}

/* Process one complete RPC message (single-threaded path) */
static int handle_rpc_message(darwinfuse_server_t *srv, client_conn_t *c)
{
    uint8_t *reply_buf = NULL;
    size_t reply_len = 0;

    if (process_rpc_message(srv, c, c->payload_buf, c->payload_len,
                             &reply_buf, &reply_len) < 0)
        return -1;

    int rc = write_reply(c->fd, reply_buf, reply_len);
    free(reply_buf);
    return rc;
}

/* ---- Worker thread (MT path) ---- */

static void *worker_thread_func(void *arg)
{
    darwinfuse_server_t *srv = (darwinfuse_server_t *)arg;

    DFUSE_LOG("Worker thread started (tid=%p)", (void *)pthread_self());

    while (1) {
        nfs4_work_item_t *item = work_queue_pop(&srv->work_queue);
        if (!item) break;  /* shutdown — queue drained */

        client_conn_t *c = item->client;

        if (!c->closing) {
            /* Set thread-local FUSE context for this worker */
            darwinfuse_set_private_data(srv->private_data);

            uint8_t *reply_buf = NULL;
            size_t reply_len = 0;

            if (process_rpc_message(srv, c, item->payload, item->payload_len,
                                     &reply_buf, &reply_len) == 0) {
                pthread_mutex_lock(&c->write_lock);
                if (c->fd >= 0)
                    write_reply(c->fd, reply_buf, reply_len);
                pthread_mutex_unlock(&c->write_lock);
                free(reply_buf);
            }
        }

        free(item->payload);
        free(item);
        atomic_fetch_sub(&c->inflight, 1);
    }

    DFUSE_LOG("Worker thread exiting (tid=%p)", (void *)pthread_self());
    return NULL;
}

/* Read available data for a client. Returns 0 if OK, -1 if client should be closed. */
static int client_read(darwinfuse_server_t *srv, client_conn_t *c)
{
    for (;;) {
        if (c->read_state == CLIENT_STATE_READ_MARK) {
            /* Read the 4-byte TCP record mark */
            size_t need = 4 - c->mark_read;
            ssize_t n = read(c->fd, c->mark_buf + c->mark_read, need);
            if (n == 0) return -1;  /* client disconnected */
            if (n < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
                return -1;
            }
            c->mark_read += (size_t)n;
            if (c->mark_read < 4)
                return 0;

            /* Parse record mark */
            c->payload_len = rpc_parse_record_mark(c->mark_buf, &c->last_fragment);
            if (c->payload_len == 0 || c->payload_len > DFUSE_XDR_MAXBUF) {
                DFUSE_ERR("Invalid record mark length: %zu", c->payload_len);
                return -1;
            }

            c->payload_buf = malloc(c->payload_len);
            if (!c->payload_buf) return -1;
            c->payload_read = 0;
            c->read_state = CLIENT_STATE_READ_PAYLOAD;
        }

        if (c->read_state == CLIENT_STATE_READ_PAYLOAD) {
            size_t need = c->payload_len - c->payload_read;
            ssize_t n = read(c->fd, c->payload_buf + c->payload_read, need);
            if (n == 0) return -1;
            if (n < 0) {
                if (errno == EINTR) continue;
                if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
                return -1;
            }
            c->payload_read += (size_t)n;
            if (c->payload_read < c->payload_len)
                return 0;

            /* Complete message received */
            int rc = 0;
            if (srv->multithreaded) {
                /* Enqueue for worker thread processing */
                nfs4_work_item_t *item = malloc(sizeof(*item));
                if (item) {
                    item->client = c;
                    item->payload = c->payload_buf;
                    item->payload_len = c->payload_len;
                    item->next = NULL;
                    c->payload_buf = NULL;  /* ownership transferred */
                    atomic_fetch_add(&c->inflight, 1);
                    work_queue_push(&srv->work_queue, item);
                } else {
                    rc = -1;
                }
            } else {
                /* Single-threaded: process inline */
                rc = handle_rpc_message(srv, c);
            }

            /* Reset for next message */
            free(c->payload_buf);  /* NULL if transferred to work item */
            c->payload_buf = NULL;
            c->payload_len = 0;
            c->payload_read = 0;
            c->mark_read = 0;
            c->read_state = CLIENT_STATE_READ_MARK;

            if (rc < 0)
                return -1;
        }
    }
}

/* ---- Public API ---- */

darwinfuse_server_t *nfs4_server_create(const darwinfuse_config_t *config,
                                         uint16_t *port)
{
    darwinfuse_server_t *srv = calloc(1, sizeof(*srv));
    if (!srv) return NULL;

    srv->config = *config;
    srv->listen_fd = -1;
    srv->wakeup_pipe[0] = -1;
    srv->wakeup_pipe[1] = -1;

    /* Self-pipe for stop signaling */
    if (pipe(srv->wakeup_pipe) < 0) {
        free(srv);
        return NULL;
    }
    set_nonblocking(srv->wakeup_pipe[0]);
    set_nonblocking(srv->wakeup_pipe[1]);

    /* Create TCP listen socket */
    srv->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (srv->listen_fd < 0) {
        DFUSE_ERR("socket: %s", strerror(errno));
        goto fail;
    }

    int reuse = 1;
    setsockopt(srv->listen_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;  /* ephemeral port */

    if (bind(srv->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        DFUSE_ERR("bind: %s", strerror(errno));
        goto fail;
    }

    if (listen(srv->listen_fd, 5) < 0) {
        DFUSE_ERR("listen: %s", strerror(errno));
        goto fail;
    }

    /* Retrieve the assigned port */
    socklen_t addrlen = sizeof(addr);
    if (getsockname(srv->listen_fd, (struct sockaddr *)&addr, &addrlen) < 0) {
        DFUSE_ERR("getsockname: %s", strerror(errno));
        goto fail;
    }
    *port = ntohs(addr.sin_port);

    set_nonblocking(srv->listen_fd);
    srv->running = 1;

    DFUSE_LOG("NFS server listening on 127.0.0.1:%u", *port);
    return srv;

fail:
    if (srv->listen_fd >= 0) close(srv->listen_fd);
    if (srv->wakeup_pipe[0] >= 0) close(srv->wakeup_pipe[0]);
    if (srv->wakeup_pipe[1] >= 0) close(srv->wakeup_pipe[1]);
    free(srv);
    return NULL;
}

int nfs4_server_run(darwinfuse_server_t *srv)
{
    int result = 0;

    /* Start thread pool if multi-threaded */
    if (srv->multithreaded) {
        work_queue_init(&srv->work_queue);
        srv->worker_threads = calloc((size_t)srv->num_threads, sizeof(pthread_t));
        if (!srv->worker_threads) {
            DFUSE_ERR("Failed to allocate worker threads");
            return -1;
        }
        /* Rust callbacks use ordinary Rust thread stack expectations.  macOS
         * pthread defaults are too small and can fault in an otherwise valid
         * callback before Rust's stack-overflow guard can report the error. */
        pthread_attr_t worker_attr;
        int attr_rc = pthread_attr_init(&worker_attr);
        if (attr_rc != 0 ||
            pthread_attr_setstacksize(&worker_attr, 4U * 1024U * 1024U) != 0) {
            DFUSE_ERR("Failed to configure worker thread stack");
            if (attr_rc == 0)
                pthread_attr_destroy(&worker_attr);
            work_queue_destroy(&srv->work_queue);
            free(srv->worker_threads);
            srv->worker_threads = NULL;
            return -1;
        }
        for (int i = 0; i < srv->num_threads; i++) {
            if (pthread_create(&srv->worker_threads[i], &worker_attr,
                               worker_thread_func, srv) != 0) {
                DFUSE_ERR("Failed to create worker thread %d", i);
                pthread_attr_destroy(&worker_attr);
                work_queue_shutdown(&srv->work_queue);
                for (int j = 0; j < i; j++)
                    pthread_join(srv->worker_threads[j], NULL);
                work_queue_destroy(&srv->work_queue);
                free(srv->worker_threads);
                srv->worker_threads = NULL;
                return -1;
            }
        }
        pthread_attr_destroy(&worker_attr);
        DFUSE_LOG("Thread pool started with %d workers", srv->num_threads);
    }

    /* pollfd layout: [0] = listen_fd, [1] = wakeup_pipe, [2..N+1] = clients */
    struct pollfd pfds[2 + DFUSE_MAX_CLIENTS];

    while (srv->running) {
        int nfds = 0;

        /* Listen socket */
        pfds[nfds].fd = srv->listen_fd;
        pfds[nfds].events = POLLIN;
        pfds[nfds].revents = 0;
        nfds++;

        /* Wakeup pipe */
        pfds[nfds].fd = srv->wakeup_pipe[0];
        pfds[nfds].events = POLLIN;
        pfds[nfds].revents = 0;
        nfds++;

        /* Client connections (fd=-1 for closing clients — poll ignores them) */
        for (int i = 0; i < srv->num_clients; i++) {
            pfds[nfds].fd = srv->clients[i].fd;
            pfds[nfds].events = POLLIN;
            pfds[nfds].revents = 0;
            nfds++;
        }

        int ret = poll(pfds, (nfds_t)nfds, 1000);  /* 1-second timeout */
        if (ret < 0) {
            if (errno == EINTR) continue;
            DFUSE_ERR("poll: %s", strerror(errno));
            result = -1;
            break;
        }

        /* Check wakeup pipe */
        if (pfds[1].revents & POLLIN) {
            char dummy[16];
            while (read(srv->wakeup_pipe[0], dummy, sizeof(dummy)) > 0)
                ;
            if (!srv->running)
                break;
        }

        /* Accept new connections */
        if (pfds[0].revents & POLLIN) {
            struct sockaddr_in client_addr;
            socklen_t client_len = sizeof(client_addr);
            int cfd = accept(srv->listen_fd, (struct sockaddr *)&client_addr,
                             &client_len);
            if (cfd >= 0) {
                if (srv->num_clients >= DFUSE_MAX_CLIENTS) {
                    close(cfd);
                } else {
                    set_nonblocking(cfd);
                    set_tcp_nodelay(cfd);
                    client_init(&srv->clients[srv->num_clients], cfd);
                    srv->num_clients++;
                    srv->had_client = 1;
                    DFUSE_LOG("Client connected (fd=%d, total=%d)", cfd, srv->num_clients);
                }
            }
        }

        /* Process client data */
        for (int i = 0; i < srv->num_clients; i++) {
            if (srv->clients[i].closing || srv->clients[i].fd < 0)
                continue;
            int pfd_idx = 2 + i;
            if (pfds[pfd_idx].revents & (POLLIN | POLLERR | POLLHUP)) {
                if (client_read(srv, &srv->clients[i]) < 0) {
                    DFUSE_LOG("Client disconnected (fd=%d)", srv->clients[i].fd);
                    if (srv->multithreaded) {
                        /* Close fd but defer full cleanup until workers drain */
                        srv->clients[i].closing = 1;
                        close(srv->clients[i].fd);
                        srv->clients[i].fd = -1;
                    } else {
                        client_close(&srv->clients[i]);
                        /* Compact: move last client to this slot */
                        srv->num_clients--;
                        if (i < srv->num_clients)
                            srv->clients[i] = srv->clients[srv->num_clients];
                        i--;  /* re-check this slot */
                    }
                }
            }
        }

        /* Reap fully-drained closing clients (MT mode) */
        if (srv->multithreaded) {
            for (int i = srv->num_clients - 1; i >= 0; i--) {
                if (srv->clients[i].closing &&
                    atomic_load(&srv->clients[i].inflight) == 0) {
                    client_close(&srv->clients[i]);
                    srv->clients[i].closing = 0;
                }
            }
            /* Shrink num_clients if trailing slots are dead */
            while (srv->num_clients > 0 &&
                   srv->clients[srv->num_clients - 1].fd < 0 &&
                   !srv->clients[srv->num_clients - 1].closing)
                srv->num_clients--;
        }

        /* Check if all clients disconnected (mount was unmounted) */
        if (srv->had_client) {
            if (srv->multithreaded) {
                int alive = 0;
                for (int i = 0; i < srv->num_clients; i++) {
                    if (srv->clients[i].fd >= 0 || srv->clients[i].closing)
                        alive++;
                }
                if (alive == 0) {
                    DFUSE_LOG("All clients disconnected — exiting event loop");
                    break;
                }
            } else {
                if (srv->num_clients == 0) {
                    DFUSE_LOG("All clients disconnected — exiting event loop");
                    break;
                }
            }
        }
    }

    /* Shut down thread pool */
    if (srv->multithreaded && srv->worker_threads) {
        work_queue_shutdown(&srv->work_queue);
        for (int i = 0; i < srv->num_threads; i++)
            pthread_join(srv->worker_threads[i], NULL);
        work_queue_destroy(&srv->work_queue);
        free(srv->worker_threads);
        srv->worker_threads = NULL;
        DFUSE_LOG("Thread pool stopped");
    }

    return result;
}

void nfs4_server_stop(darwinfuse_server_t *srv)
{
    if (!srv) return;
    srv->running = 0;
    /* Wake up poll */
    char c = 1;
    if (write(srv->wakeup_pipe[1], &c, 1) < 0) {
        /* ignore — best effort */
    }
}

void nfs4_server_restart(darwinfuse_server_t *srv)
{
    if (!srv) return;

    /* Re-arm the running flag so nfs4_server_run() works again */
    srv->running = 1;
    srv->had_client = 0;

    /* Drain the wakeup pipe (may have leftover data from stop) */
    char buf[16];
    while (read(srv->wakeup_pipe[0], buf, sizeof(buf)) > 0)
        ;

    DFUSE_LOG("Server re-armed for new event loop");
}

void nfs4_server_destroy(darwinfuse_server_t *srv)
{
    if (!srv) return;

    for (int i = 0; i < srv->num_clients; i++)
        client_close(&srv->clients[i]);

    if (srv->listen_fd >= 0) close(srv->listen_fd);
    if (srv->wakeup_pipe[0] >= 0) close(srv->wakeup_pipe[0]);
    if (srv->wakeup_pipe[1] >= 0) close(srv->wakeup_pipe[1]);

    free(srv);
}

void nfs4_server_close_inherited_fds(darwinfuse_server_t *srv)
{
    if (!srv) return;

    /* Build a set of FDs the server needs to keep */
    int keep[3 + DFUSE_MAX_CLIENTS];
    int nkeep = 0;
    if (srv->listen_fd >= 0) keep[nkeep++] = srv->listen_fd;
    if (srv->wakeup_pipe[0] >= 0) keep[nkeep++] = srv->wakeup_pipe[0];
    if (srv->wakeup_pipe[1] >= 0) keep[nkeep++] = srv->wakeup_pipe[1];
    for (int i = 0; i < srv->num_clients; i++)
        if (srv->clients[i].fd >= 0) keep[nkeep++] = srv->clients[i].fd;

    /* Close everything from fd 3 up to a reasonable limit */
    int maxfd = (int)sysconf(_SC_OPEN_MAX);
    if (maxfd < 0 || maxfd > 4096) maxfd = 4096;

    for (int fd = 3; fd < maxfd; fd++) {
        int needed = 0;
        for (int k = 0; k < nkeep; k++) {
            if (keep[k] == fd) { needed = 1; break; }
        }
        if (!needed) close(fd);
    }
}

void nfs4_server_set_ops(darwinfuse_server_t *srv,
                          const struct fuse_operations *ops,
                          void *user_data)
{
    if (!srv) return;
    srv->config.ops = ops;
    srv->config.user_data = user_data;
}

void nfs4_server_set_inode_table(darwinfuse_server_t *srv,
                                  dfuse_inode_table_t *tbl)
{
    if (!srv) return;
    srv->config.inode_table = tbl;
}

void nfs4_server_close_inherited_pipes(darwinfuse_server_t *srv)
{
    if (!srv) return;

    /* Build a set of PIPE FDs the server needs to keep */
    int keep[2];
    int nkeep = 0;
    if (srv->wakeup_pipe[0] >= 0) keep[nkeep++] = srv->wakeup_pipe[0];
    if (srv->wakeup_pipe[1] >= 0) keep[nkeep++] = srv->wakeup_pipe[1];

    int maxfd = (int)sysconf(_SC_OPEN_MAX);
    if (maxfd < 0 || maxfd > 4096) maxfd = 4096;

    for (int fd = 3; fd < maxfd; fd++) {
        /* Skip server's wakeup pipe */
        int needed = 0;
        for (int k = 0; k < nkeep; k++) {
            if (keep[k] == fd) { needed = 1; break; }
        }
        if (needed) continue;

        /* Only close PIPE-type FDs; keep regular files, sockets, etc. */
        struct stat st;
        if (fstat(fd, &st) == 0 && S_ISFIFO(st.st_mode)) {
            close(fd);
        }
    }
}

void nfs4_server_set_multithreaded(darwinfuse_server_t *srv, int num_threads)
{
    if (!srv) return;
    if (num_threads <= 0) {
        srv->multithreaded = 0;
        srv->num_threads = 0;
        return;
    }
    if (num_threads > DFUSE_MAX_THREADS)
        num_threads = DFUSE_MAX_THREADS;
    srv->multithreaded = 1;
    srv->num_threads = num_threads;
    DFUSE_LOG("Multi-threaded mode enabled (%d threads)", num_threads);
}

void nfs4_server_set_private_data(darwinfuse_server_t *srv, void *private_data)
{
    if (!srv) return;
    srv->private_data = private_data;
}
