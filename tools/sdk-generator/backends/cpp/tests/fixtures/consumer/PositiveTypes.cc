#include "actors/v1/actors.pb.h"
#include "workers/v1/workers.pb.h"
#include "stream/v1/stream.pb.h"
#include <cstdint>
#include <limits>
#include <string>
void positive_types() {
  const std::string bytes("\0\xff", 2);
  acyclic::actors::v1::CreateActorRequest actor;
  actor.set_code_sha256(bytes);
  acyclic::workers::v1::PublishVersionRequest worker;
  worker.set_javascript_module(bytes);
  acyclic::stream::v1::AppendRequest append;
  append.set_if_tail(std::numeric_limits<std::uint64_t>::max());
}
