#if defined(_WIN32)
#include <windows.h>
#else
#include <pthread.h>
#include <time.h>
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#define __STDC_VERSION__ 202311L
extern "C" {
#include "acyclic_embedded_prototype.h"
}
#undef __STDC_VERSION__

namespace {

struct Context {
  uint64_t reader;
#if defined(_WIN32)
  volatile LONG finished;
  volatile LONG status;
#else
  volatile int finished;
  volatile int status;
#endif
};

void require(bool condition, const char* message) {
  if (!condition) {
    fputs(message, stderr);
    fputc('\n', stderr);
    exit(EXIT_FAILURE);
  }
}

#if defined(_WIN32)
DWORD WINAPI pull_once(void* opaque) {
#else
void* pull_once(void* opaque) {
#endif
  Context* context = static_cast<Context*>(opaque);
  const AcyclicNextResult result = acyclic_embedded_reader_next(context->reader);
#if defined(_WIN32)
  InterlockedExchange(&context->status, static_cast<LONG>(result.status));
#else
  context->status = static_cast<int>(result.status);
#endif
  acyclic_next_result_release(result);
#if defined(_WIN32)
  InterlockedExchange(&context->finished, 1);
  return 0;
#else
  context->finished = 1;
  return nullptr;
#endif
}

const uint8_t kPath[] = "cpp/cross-thread";
const uint8_t kValue[] = "seed";

}  // namespace

int main() {
  const uint64_t engine = acyclic_embedded_engine_open();
  require(engine != 0, "engine open failed");
  const AcyclicAppendResult seed = acyclic_embedded_engine_append(
      engine, kPath, sizeof(kPath) - 1, kValue, sizeof(kValue) - 1);
  require(seed.status == Ok, "seed append failed");
  acyclic_append_result_release(seed);

  const AcyclicOpenResult opened = acyclic_embedded_reader_open(
      engine, kPath, sizeof(kPath) - 1, 0, 0, 1);
  require(opened.status == Ok, "follow reader open failed");
  const AcyclicNextResult initial = acyclic_embedded_reader_next(opened.reader);
  require(initial.status == Ok, "initial follow record failed");
  acyclic_next_result_release(initial);

  Context context{opened.reader, 0, -1};
#if defined(_WIN32)
  HANDLE thread = CreateThread(nullptr, 0, pull_once, &context, 0, nullptr);
  require(thread != nullptr, "worker thread creation failed");
  Sleep(5);
  acyclic_embedded_reader_cancel(opened.reader);
  require(WaitForSingleObject(thread, 2000) == WAIT_OBJECT_0,
          "cancelled reader worker did not wake");
  CloseHandle(thread);
#else
  pthread_t thread;
  require(pthread_create(&thread, nullptr, pull_once, &context) == 0,
          "worker thread creation failed");
  struct timespec delay {0, 5 * 1000 * 1000};
  nanosleep(&delay, nullptr);
  acyclic_embedded_reader_cancel(opened.reader);
  require(pthread_join(thread, nullptr) == 0,
          "cancelled reader worker did not wake");
#endif
  require(context.finished == 1, "worker did not finish");
  require(static_cast<int>(context.status) == static_cast<int>(Cancelled),
          "worker observed the wrong cancellation status");

  acyclic_open_result_release(opened);
  acyclic_embedded_engine_close(engine);
  puts("C++ cross-thread cancellation smoke passed");
  return EXIT_SUCCESS;
}
