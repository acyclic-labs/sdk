#include "stream/v1/stream.pb.h"
void invalid_optional_integer() {
  acyclic::stream::v1::AppendRequest append;
  append.set_if_tail("invalid integer");
}
