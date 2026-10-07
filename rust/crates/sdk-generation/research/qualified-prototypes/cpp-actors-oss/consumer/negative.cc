#include "lib.rs.h"
int main() {
  // Deliberately invalid: CXX's generated declaration requires uint64_t.
  std::string wrong = "1";
  auto result = acyclic::actors::actors_validate_positive_u64(wrong);
  return result.ok ? 0 : 1;
}
