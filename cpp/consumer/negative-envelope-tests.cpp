#define main acyclic_cpp_consumer_main
#include "../consumer.cpp"
#undef main

#include <cassert>
#include <stdexcept>

namespace {

SeedScenario::WireEnvelope envelope(const std::string& digest) {
  return {"protobuf-base64", "google.protobuf.Empty", "CAE=", digest};
}

template <typename Function>
void expects_failure(Function&& function) {
  bool failed = false;
  try {
    function();
  } catch (const std::runtime_error&) {
    failed = true;
  }
  assert(failed);
}

}  // namespace

int main() {
  // SHA-256 of the protobuf bytes 08 01, verified independently with the
  // platform SHA-256 implementation before being recorded here.
  const std::string digest =
      "fb8da7eb5b1b399e7321179dac9e9f65773d7331e1e30554e3911e4325e1ef19";
  auto valid = envelope(digest);
  expects_failure([&] { verify_envelope_hash(valid, "wrong"); });
  expects_failure([&] { verify_envelope_hash(envelope(std::string(64, '0')), "\x08\x01"); });
  expects_failure([&] { verify_expected_frame_count("fixture/Read", 1, 2); });
  verify_expected_frame_count("fixture/Read", 2, 2);
  return 0;
}
