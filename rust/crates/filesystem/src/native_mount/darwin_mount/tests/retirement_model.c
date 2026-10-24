/* Exhaustive callback-order model over the production retirement adapter.
   Framework calls are the boundary; the actual ownership loop is included. */
#include <CoreFoundation/CoreFoundation.h>
#include <DiskArbitration/DiskArbitration.h>
#include <errno.h>
#include <stdint.h>
#include <time.h>

static struct {
  unsigned delay, visits, submitted, released, violation;
  int source, pending, dissent, allocation_failure;
  DADiskUnmountCallback callback;
  void *context;
} model;

static DASessionRef model_session(CFAllocatorRef allocator) {
  (void)allocator;
  if (model.allocation_failure == 1) return NULL;
  return (DASessionRef)(uintptr_t)1;
}
static CFURLRef model_url(CFAllocatorRef allocator, const UInt8 *path,
                         CFIndex length, Boolean directory) {
  (void)allocator; (void)path; (void)length; (void)directory;
  if (model.allocation_failure == 2) return NULL;
  return (CFURLRef)(uintptr_t)2;
}
static DADiskRef model_disk(CFAllocatorRef allocator, DASessionRef session, CFURLRef url) {
  (void)allocator; (void)session; (void)url;
  if (model.allocation_failure == 3) return NULL;
  return (DADiskRef)(uintptr_t)3;
}
static CFRunLoopRef model_loop(void) { return (CFRunLoopRef)(uintptr_t)4; }
static void model_schedule(DASessionRef session, CFRunLoopRef loop, CFStringRef mode) {
  (void)session; (void)loop; (void)mode;
}
static void model_unschedule(DASessionRef session, CFRunLoopRef loop, CFStringRef mode) {
  (void)session; (void)loop; (void)mode;
  if (model.pending) model.violation = 1;
}
static void model_release(CFTypeRef object) {
  (void)object;
  ++model.released;
  if (model.pending) model.violation = 1;
}
static void model_unmount(DADiskRef disk, DADiskUnmountOptions options,
                          DADiskUnmountCallback callback, void *context) {
  (void)disk; (void)options;
  ++model.submitted;
  model.pending = 1;
  model.callback = callback;
  model.context = context;
}
static DAReturn model_dissent(DADissenterRef dissent) {
  (void)dissent;
  return model.dissent;
}
static SInt32 model_run(CFStringRef mode, CFTimeInterval duration, Boolean once) {
  (void)mode; (void)once;
  if (!model.source) return kCFRunLoopRunFinished;
  if (duration == 0) return kCFRunLoopRunTimedOut;
  if (++model.visits > model.delay) {
    model.pending = 0;
    model.callback((DADiskRef)(uintptr_t)3,
                   model.dissent ? (DADissenterRef)(uintptr_t)5 : NULL, model.context);
    return kCFRunLoopRunHandledSource;
  }
  return kCFRunLoopRunTimedOut;
}

#define DASessionCreate model_session
#define CFURLCreateFromFileSystemRepresentation model_url
#define DADiskCreateFromVolumePath model_disk
#define CFRunLoopGetCurrent model_loop
#define DASessionScheduleWithRunLoop model_schedule
#define DASessionUnscheduleFromRunLoop model_unschedule
#define CFRelease model_release
#define DADiskUnmount model_unmount
#define DADissenterGetStatus model_dissent
#define CFRunLoopRunInMode model_run
#define acyclic_fs_darwin_retire_mount model_retire_mount
#include "../../darwin_mount_retirement.c"

int acyclic_fs_darwin_test_retirement_custody(void) {
  for (unsigned delay = 0; delay <= 1024; ++delay) {
    for (unsigned denied = 0; denied <= 1; ++denied) {
      memset(&model, 0, sizeof(model));
      model.delay = delay;
      model.source = 1;
      model.dissent = denied ? kDAReturnNotPermitted : 0;
      int dissent = 0;
      int status = model_retire_mount("/fixture", &dissent);
      if (model.violation || model.pending || model.submitted != 1 ||
          model.released != 3 || model.visits != delay + 1 ||
          status != (denied ? EACCES : 0) || dissent != model.dissent) return 1;
    }
  }
  memset(&model, 0, sizeof(model));
  int dissent = 0;
  if (model_retire_mount("/fixture", &dissent) != ENOMEM ||
      model.submitted != 0 || model.violation || model.released != 3) return 2;
  for (unsigned stage = 1; stage <= 3; ++stage) {
    memset(&model, 0, sizeof(model));
    model.source = 1;
    model.allocation_failure = stage;
    int status = model_retire_mount("/fixture", &dissent);
    if (status != (stage == 3 ? ENOENT : ENOMEM) || model.pending ||
        model.submitted || model.violation || model.released != stage - 1) return 3;
  }
  return 0;
}
