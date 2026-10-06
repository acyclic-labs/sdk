#include <acyclic/rust_typed_clients.hpp>
#include <cassert>
#include <cstdint>
#include <optional>
#include <stdexcept>
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

  const std::vector<std::uint8_t> digestBytes(32, 7);
  const acyclic::rust_typed::Sha256Digest digest(digestBytes);
  using ImageChoice =
      acyclic::rust_typed::MachinesImageImmutableReferenceChoiceValue;
  const acyclic::rust_typed::Image managed(
      std::nullopt,
      ImageChoice{
          acyclic::rust_typed::MachinesImageImmutableReferenceChoiceManagedDigest{
              digest}});
  (void)managed;

  bool rejected = false;
  try {
    const acyclic::rust_typed::Image invalid(std::nullopt, ImageChoice{
        acyclic::rust_typed::MachinesImageImmutableReferenceChoiceNone{}});
    (void)invalid;
  } catch (const std::invalid_argument&) {
    rejected = true;
  }
  assert(rejected);
}
