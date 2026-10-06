#include <acyclic/rust_typed_clients.hpp>
#include <cassert>
#include <cstdint>
#include <vector>

int main() {
  using Choice = acyclic::rust_typed::InferenceCustomerRunResultContextChoiceValue;
  Choice choice =
      acyclic::rust_typed::InferenceCustomerRunResultContextChoiceUnknown{
          902, {0, 1, 255}};
  const auto* unknown =
      std::get_if<acyclic::rust_typed::InferenceCustomerRunResultContextChoiceUnknown>(
          &choice);
  const std::vector<std::uint8_t> expected{0, 1, 255};
  assert(unknown != nullptr && unknown->raw_tag == 902 && unknown->payload == expected);
}
