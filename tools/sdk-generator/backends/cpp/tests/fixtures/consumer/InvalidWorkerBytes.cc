#include "workers/v1/workers.pb.h"
#include <vector>
void invalid_worker_bytes() {
  acyclic::workers::v1::PublishVersionRequest worker;
  worker.set_javascript_module(std::vector<unsigned char>{0, 255});
}
