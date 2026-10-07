#include <cassert>
#include <cstdint>
#include <limits>
#include "lib.rs.h"

int main() {
  auto client = acyclic::actors::actors_client_new();
  assert(!acyclic::actors::actors_client_is_connected(*client));
  auto invalid_connection = acyclic::actors::actors_client_connect_probe(
      "http://not-https.example", "token");
  assert(!invalid_connection.connected &&
         invalid_connection.error == acyclic::actors::ErrorKind::Configuration);

  auto observation = acyclic::actors::actors_sample_observation();
  assert(acyclic::actors::actor_observation_checkpoint_epoch(*observation) ==
         std::numeric_limits<std::uint64_t>::max());
  assert(acyclic::actors::actor_observation_has_checkpoint(*observation));
  assert(acyclic::actors::actor_observation_checkpoint(*observation) ==
         std::numeric_limits<std::uint64_t>::max());

  auto max = acyclic::actors::actors_validate_positive_u64(
      std::numeric_limits<std::uint64_t>::max());
  assert(max.ok && max.value == std::numeric_limits<std::uint64_t>::max());
  auto zero = acyclic::actors::actors_validate_positive_u64(0);
  assert(!zero.ok && zero.error == acyclic::actors::ErrorKind::InvalidArgument);

  auto error = acyclic::actors::actors_cancelled_error();
  assert(acyclic::actors::actors_error_kind(*error) ==
         acyclic::actors::ErrorKind::Cancelled);

  auto operation = acyclic::actors::actors_operation_new();
  assert(!acyclic::actors::actors_operation_is_cancelled(*operation));
  acyclic::actors::actors_operation_cancel(*operation);
  assert(acyclic::actors::actors_operation_is_cancelled(*operation));
}
