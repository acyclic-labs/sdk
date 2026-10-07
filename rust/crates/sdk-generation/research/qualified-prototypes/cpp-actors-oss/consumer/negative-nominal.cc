#include "lib.rs.h"
int main() {
  auto client = acyclic::actors::actors_client_new();
  // Deliberately invalid: opaque Rust types are nominal and cannot be interchanged.
  acyclic::actors::actors_operation_cancel(*client);
  return 0;
}
