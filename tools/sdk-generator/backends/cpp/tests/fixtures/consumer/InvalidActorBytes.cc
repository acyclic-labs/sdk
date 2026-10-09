#include "actors/v1/actors.pb.h"
#include <vector>
void invalid_actor_bytes() {
  acyclic::actors::v1::CreateActorRequest actor;
  actor.set_code_sha256(std::vector<unsigned char>{0, 255});
}
