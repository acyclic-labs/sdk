#include <CoreFoundation/CoreFoundation.h>
#include <DiskArbitration/DiskArbitration.h>
#include <errno.h>
#include <float.h>
#include <string.h>

struct retirement_result {
  int completed;
  int dissent;
};

/* Keep the original DAReturn separately: errno describes the refusal class,
   not whether the authenticated kernel mount has physically detached. */
static int retirement_error(DAReturn status) {
  switch (status) {
    case kDAReturnSuccess: return 0;
    case kDAReturnBusy:
    case kDAReturnExclusiveAccess: return EBUSY;
    case kDAReturnBadArgument: return EINVAL;
    case kDAReturnNoResources: return ENOMEM;
    case kDAReturnNotFound:
    case kDAReturnNotMounted: return ENOENT;
    case kDAReturnNotPermitted:
    case kDAReturnNotPrivileged: return EACCES;
    case kDAReturnNotReady: return ENXIO;
    case kDAReturnNotWritable: return EROFS;
    case kDAReturnUnsupported: return ENOTSUP;
    default: return EIO;
  }
}

static void retirement_completed(DADiskRef disk, DADissenterRef dissent, void *context) {
  (void)disk;
  struct retirement_result *result = context;
  result->dissent = dissent == NULL ? 0 : DADissenterGetStatus(dissent);
  result->completed = 1;
}

/* The caller holds the authenticated destination fence. Unlike diskutil's
   StorageKit path discovery, DADiskCreateFromVolumePath uses the cached mount
   table for an exact canonical mountpoint; it never opens the dead NFS root.
   Only this thread runs this private mode. Disk Arbitration has no request
   cancellation API: removing a callback source does not cancel an unmount.
   The terminal callback must therefore precede both callback-storage release
   and the caller's fence release. A deadline cannot establish retirement. */
int acyclic_fs_darwin_retire_mount(const char *destination, int *dissent) {
  int error = ENOMEM;
  DASessionRef session = DASessionCreate(NULL);
  if (session == NULL) return error;
  CFURLRef url = CFURLCreateFromFileSystemRepresentation(
      NULL, (const UInt8 *)destination, (CFIndex)strlen(destination), true);
  DADiskRef disk = url == NULL ? NULL : DADiskCreateFromVolumePath(NULL, session, url);
  if (disk != NULL) {
    CFRunLoopRef loop = CFRunLoopGetCurrent();
    CFStringRef mode = CFSTR("acyclic.fs.mount-retirement");
    DASessionScheduleWithRunLoop(session, loop, mode);
    struct retirement_result result = {0, 0};
    /* Refuse a failed source installation before submitting any request. */
    if (CFRunLoopRunInMode(mode, 0, true) == kCFRunLoopRunFinished) {
      error = ENOMEM;
    } else {
      DADiskUnmount(disk, kDADiskUnmountOptionForce, retirement_completed, &result);
      while (!result.completed) CFRunLoopRunInMode(mode, DBL_MAX, true);
      *dissent = result.dissent;
      error = retirement_error(result.dissent);
    }
    DASessionUnscheduleFromRunLoop(session, loop, mode);
    CFRelease(disk);
  } else if (url != NULL) {
    error = ENOENT;
  }
  if (url != NULL) CFRelease(url);
  CFRelease(session);
  return error;
}
