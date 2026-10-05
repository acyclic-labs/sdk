#include <iostream>
#include <string>

#include "actors/v1/actors.pb.h"

int main() {
  acyclic::actors::v1::CreateActorRequest request;
  request.set_code_sha256(std::string("\x00\x01\xff", 3));
  request.set_home_region("eu-west");
  request.set_idempotency_key("cpp-consumer");

  std::string encoded;
  if (!request.SerializeToString(&encoded) || encoded.empty()) {
    std::cerr << "generated request did not serialize\n";
    return 1;
  }

  acyclic::actors::v1::CreateActorRequest decoded;
  if (!decoded.ParseFromString(encoded) ||
      decoded.code_sha256() != std::string("\x00\x01\xff", 3) ||
      decoded.home_region() != "eu-west" ||
      decoded.idempotency_key() != "cpp-consumer") {
    std::cerr << "generated request did not round-trip\n";
    return 2;
  }

  const auto* descriptor = decoded.GetDescriptor();
  if (descriptor == nullptr ||
      descriptor->full_name() != "acyclic.actors.v1.CreateActorRequest") {
    std::cerr << "descriptor identity mismatch\n";
    return 3;
  }

  std::cout << "cpp-consumer=passed bytes=" << encoded.size()
            << " descriptor=" << descriptor->full_name() << "\n";
  return 0;
}
