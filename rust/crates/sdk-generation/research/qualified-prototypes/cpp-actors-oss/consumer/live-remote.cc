#include <cassert>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>

#include "lib.rs.h"

namespace {

std::string text(const rust::String& value) {
  return std::string(value.data(), value.size());
}

void assert_actor_result(const char* name,
                         const acyclic::actors::ActorOperationResult& result) {
  std::cerr << name << " ok=" << acyclic::actors::actor_operation_ok(result)
            << " error="
            << static_cast<int>(acyclic::actors::actor_operation_error(result))
            << " message=" << text(acyclic::actors::actor_operation_message(result))
            << "\n";
  assert(acyclic::actors::actor_operation_ok(result));
  assert(acyclic::actors::actor_operation_error(result) ==
         acyclic::actors::ErrorKind::NoError);
  assert(text(acyclic::actors::actor_operation_actor_id(result)) == "actor-a");
  assert(text(acyclic::actors::actor_operation_home_region(result)) == "eu");
  assert(acyclic::actors::actor_operation_active(result));
  assert(acyclic::actors::actor_operation_subscriptions_empty(result));
  assert(acyclic::actors::actor_operation_configuration_revision(result) == 0);
  assert(acyclic::actors::actor_operation_checkpoint_epoch(result) == 9);
  assert(!acyclic::actors::actor_operation_has_checkpoint(result));
}

}  // namespace

int main(int argc, char** argv) {
  assert(argc == 4);
  std::ifstream ca_file(argv[3]);
  std::stringstream ca_stream;
  ca_stream << ca_file.rdbuf();
  const std::string ca = ca_stream.str();

  auto client = acyclic::actors::actors_client_connect(argv[1], argv[2], ca);
  if (!acyclic::actors::actors_client_is_connected(*client)) {
    std::cerr << "connect error="
              << static_cast<int>(acyclic::actors::actors_client_error_kind(*client))
              << " message="
              << text(acyclic::actors::actors_client_error_message(*client))
              << "\n";
  }
  assert(acyclic::actors::actors_client_is_connected(*client));

  // Each call is a distinct CXX operation. Rust owns request construction,
  // async execution, domain conversion, and the typed output projection.
  assert_actor_result("create", *acyclic::actors::actors_create_actor(*client));
  assert_actor_result("update", *acyclic::actors::actors_update_actor(*client));
  assert_actor_result("inspect", *acyclic::actors::actors_inspect_actor(*client));
  assert_actor_result("add", *acyclic::actors::actors_add_subscription(*client));
  assert_actor_result("remove", *acyclic::actors::actors_remove_subscription(*client));
  assert_actor_result("resume", *acyclic::actors::actors_resume_subscription(*client));
  assert_actor_result("checkpoint", *acyclic::actors::actors_checkpoint_actor(*client));

  const auto invoke = acyclic::actors::actors_invoke_actor(*client);
  assert(acyclic::actors::actor_operation_ok(*invoke));
  assert(acyclic::actors::actor_operation_status(*invoke) == 201);
  assert(acyclic::actors::actor_operation_has_location_header(*invoke));

  auto wrong_client = acyclic::actors::actors_client_connect(
      argv[1], "wrong", ca);
  assert(acyclic::actors::actors_client_is_connected(*wrong_client));
  const auto denied = acyclic::actors::actors_inspect_actor(*wrong_client);
  assert(!acyclic::actors::actor_operation_ok(*denied));
  assert(acyclic::actors::actor_operation_error(*denied) ==
         acyclic::actors::ErrorKind::Service);

  // CXX can cancel an in-flight Rust operation through the opaque token.
  auto operation = acyclic::actors::actors_operation_new();
  auto cancelled = acyclic::actors::actor_operation_result_empty();
  std::thread worker([&] {
    cancelled = acyclic::actors::actors_inspect_actor_with_cancel(*client, *operation);
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  acyclic::actors::actors_operation_cancel(*operation);
  worker.join();
  assert(!acyclic::actors::actor_operation_ok(*cancelled));
  assert(acyclic::actors::actor_operation_error(*cancelled) ==
         acyclic::actors::ErrorKind::Cancelled);

  std::cout << "live_cxx_operations:8 authentication_rejected:true"
            << " cancellation:cancelled\n";
  return 0;
}
