#include <acyclic/rust_typed_clients.hpp>
#include <atomic>
#include <cassert>
#include <memory>
#include <type_traits>

class FixtureStream final : public acyclic::rust_typed::RustTypedStream<int> {
 public:
  explicit FixtureStream(std::atomic<int>& cancellations)
      : cancellations_(cancellations) {}

 protected:
  void on_cancel() noexcept override { ++cancellations_; }

 public:
  std::optional<int> next() override { return 7; }

 private:
  std::atomic<int>& cancellations_;
};

static_assert(!std::is_copy_constructible_v<FixtureStream>);
static_assert(!std::is_copy_assignable_v<FixtureStream>);

int main() {
  std::atomic<int> cancellations{0};
  acyclic::rust_typed::RustCancellationToken token;
  {
    auto stream = std::make_unique<FixtureStream>(cancellations);
    token = stream->cancellation_token();
    stream->cancel();
    stream->cancel();
    assert(cancellations.load() == 1);
  }
  assert(token.stop_requested());

  {
    auto dropped = std::make_unique<FixtureStream>(cancellations);
    (void)dropped;
  }
  assert(cancellations.load() == 1);
  return 0;
}
