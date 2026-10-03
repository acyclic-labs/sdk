#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/**
 * Stable result categories for the prototype boundary.
 */
enum AcyclicStatus
#if __STDC_VERSION__ >= 202311L
  : uint32_t
#endif // __STDC_VERSION__ >= 202311L
 {
  /**
   * The operation returned a value.
   */
  Ok = 0,
  /**
   * A finite reader reached its end.
   */
  End = 1,
  /**
   * The reader has not received a record yet.
   */
  Pending = 2,
  /**
   * The caller cancelled or closed the reader.
   */
  Cancelled = 3,
  /**
   * The caller supplied an invalid argument or handle.
   */
  InvalidArgument = 4,
  /**
   * The canonical provider returned an error.
   */
  ProviderError = 5,
  /**
   * The bounded bridge queue rejected another record.
   */
  Capacity = 6,
  /**
   * A panic was contained at the ABI boundary.
   */
  Panic = 7,
};
#if __STDC_VERSION__ >= 202311L
typedef enum AcyclicStatus AcyclicStatus;
#else
typedef uint32_t AcyclicStatus;
#endif // __STDC_VERSION__ >= 202311L

/**
 * Owned bytes returned by the ABI. Release every nonempty buffer exactly once.
 */
typedef struct AcyclicBuffer {
  /**
   * Monotonic allocation identity used for checked release.
   */
  uint64_t id;
  /**
   * Pointer to bytes owned by the ABI until explicit release, or null for an empty buffer.
   */
  uint8_t *ptr;
  /**
   * Number of initialized bytes.
   */
  uintptr_t len;
  /**
   * Allocation capacity required by `acyclic_buffer_release`.
   */
  uintptr_t capacity;
} AcyclicBuffer;

/**
 * Result of an append operation.
 */
typedef struct AcyclicAppendResult {
  /**
   * Result category.
   */
  AcyclicStatus status;
  /**
   * First sequence when committed.
   */
  uint64_t start;
  /**
   * Exclusive end sequence when committed.
   */
  uint64_t end;
  /**
   * Resulting stream tail when committed, or observed tail for a conflict.
   */
  uint64_t tail;
  /**
   * UTF-8 diagnostic bytes, if any.
   */
  struct AcyclicBuffer message;
} AcyclicAppendResult;

/**
 * Result of opening a reader.
 */
typedef struct AcyclicOpenResult {
  /**
   * Result category.
   */
  AcyclicStatus status;
  /**
   * Owned reader handle on success.
   */
  uint64_t reader;
  /**
   * UTF-8 diagnostic bytes, if any.
   */
  struct AcyclicBuffer message;
} AcyclicOpenResult;

/**
 * Result of one pull from a reader.
 */
typedef struct AcyclicNextResult {
  /**
   * Result category.
   */
  AcyclicStatus status;
  /**
   * Record sequence on success.
   */
  uint64_t sequence;
  /**
   * Record payload on success.
   */
  struct AcyclicBuffer value;
  /**
   * UTF-8 diagnostic bytes, if any.
   */
  struct AcyclicBuffer message;
} AcyclicNextResult;

/**
 * Returns the ABI version used by this prototype.
 */
uint32_t acyclic_embedded_abi_version(void);

/**
 * Opens a process-local engine backed by the canonical Rust `MemoryStream`.
 */
uint64_t acyclic_embedded_engine_open(void);

/**
 * Closes an engine ID. Reader IDs retain their own engine reference until closed.
 */
void acyclic_embedded_engine_close(uint64_t engine);

/**
 * Appends one copied payload to a path.
 */
struct AcyclicAppendResult acyclic_embedded_engine_append(uint64_t engine,
                                                          const uint8_t *path_ptr,
                                                          uintptr_t path_len,
                                                          const uint8_t *value_ptr,
                                                          uintptr_t value_len);

/**
 * Opens a finite (`mode = 0`) or live (`mode = 1`) reader.
 */
struct AcyclicOpenResult acyclic_embedded_reader_open(uint64_t engine,
                                                      const uint8_t *path_ptr,
                                                      uintptr_t path_len,
                                                      uint64_t from,
                                                      uint32_t limit,
                                                      uint32_t mode);

/**
 * Pulls one record, waiting briefly for a live reader before returning `Pending`.
 */
struct AcyclicNextResult acyclic_embedded_reader_next(uint64_t reader);

/**
 * Cancels a reader and wakes its async task.
 */
void acyclic_embedded_reader_cancel(uint64_t reader);

/**
 * Closes and frees a reader handle.
 */
void acyclic_embedded_reader_close(uint64_t reader);

/**
 * Releases one owned byte buffer returned by this ABI.
 */
AcyclicStatus acyclic_buffer_release(struct AcyclicBuffer buffer);

/**
 * Releases all buffers in an append result.
 */
void acyclic_append_result_release(struct AcyclicAppendResult result);

/**
 * Releases all buffers in an open result and closes an unclaimed reader.
 */
void acyclic_open_result_release(struct AcyclicOpenResult result);

/**
 * Transfers the reader handle out of an open result. The result can then be released without
 * closing the transferred reader. Passing null is a no-op and returns zero.
 */
uint64_t acyclic_open_result_take_reader(struct AcyclicOpenResult *result);

/**
 * Releases all buffers in a next result.
 */
void acyclic_next_result_release(struct AcyclicNextResult result);
