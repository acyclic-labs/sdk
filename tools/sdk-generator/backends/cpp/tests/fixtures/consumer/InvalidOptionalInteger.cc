#include "stream/v2/stream.pb.h"
void invalid_optional_integer() {
  acyclic::stream::v2::AppendRequest append;
  append.set_if_tail("invalid integer");
}
