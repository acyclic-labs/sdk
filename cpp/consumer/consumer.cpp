#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/grpcpp.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <google/protobuf/util/json_util.h>
#include <google/protobuf/util/message_differencer.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <fstream>
#include <iomanip>
#include <memory>
#include <iostream>
#include <optional>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

#ifdef GetMessage
#undef GetMessage
#endif

namespace {

struct SeedScenario {
  std::string family;
  std::string operation;
  std::string input;
  std::string expected;
  unsigned int order = 0;
  std::string seed;
  std::vector<std::string> depends_on;
  std::string known_output;
  struct WireEnvelope {
    std::string encoding;
    std::string message;
    std::string value;
    std::string sha256;
  };
  std::optional<WireEnvelope> wire_request;
  std::optional<WireEnvelope> wire_expected;
  std::vector<WireEnvelope> wire_expected_frames;
};

struct Scenario {
  std::string rpc;
  std::string shape;
  std::string response;
  std::vector<std::string> response_fields;
  bool allow_empty_response = false;
  std::vector<std::string> response_rules;
  std::optional<SeedScenario> rust_seed;
};

std::vector<std::string> quoted_values(const std::string& text) {
  const std::regex value(R"REGEX("([^"]+)")REGEX");
  std::vector<std::string> values;
  for (std::sregex_iterator it(text.begin(), text.end(), value), end; it != end; ++it) {
    values.push_back((*it)[1].str());
  }
  return values;
}

std::string snake_case(const std::string& value) {
  std::string result;
  for (std::size_t i = 0; i < value.size(); ++i) {
    const unsigned char character = static_cast<unsigned char>(value[i]);
    if (std::isupper(character) && i != 0 &&
        (std::islower(static_cast<unsigned char>(value[i - 1])) ||
         std::isdigit(static_cast<unsigned char>(value[i - 1])))) {
      result.push_back('_');
    }
    result.push_back(static_cast<char>(std::tolower(character)));
  }
  return result;
}

std::string json_unescape(const std::string& value) {
  std::string result;
  result.reserve(value.size());
  bool escaped = false;
  for (const char character : value) {
    if (escaped) {
      switch (character) {
        case '"': result.push_back('"'); break;
        case '\\': result.push_back('\\'); break;
        case 'n': result.push_back('\n'); break;
        case 'r': result.push_back('\r'); break;
        case 't': result.push_back('\t'); break;
        default: result.push_back(character); break;
      }
      escaped = false;
    } else if (character == '\\') {
      escaped = true;
    } else {
      result.push_back(character);
    }
  }
  if (escaped) result.push_back('\\');
  return result;
}

std::optional<std::string> json_string_at(const std::string& text, std::size_t offset) {
  if (offset >= text.size() || text[offset] != '"') return std::nullopt;
  std::string raw;
  bool escaped = false;
  for (std::size_t index = offset + 1; index < text.size(); ++index) {
    const char character = text[index];
    if (escaped) {
      raw.push_back('\\');
      raw.push_back(character);
      escaped = false;
      continue;
    }
    if (character == '\\') {
      escaped = true;
      continue;
    }
    if (character == '"') return json_unescape(raw);
    raw.push_back(character);
  }
  return std::nullopt;
}

std::size_t json_after_colon(const std::string& text, const std::string& key) {
  const std::string quoted = "\"" + key + "\"";
  const std::size_t key_offset = text.find(quoted);
  if (key_offset == std::string::npos) return std::string::npos;
  const std::size_t colon = text.find(':', key_offset + quoted.size());
  if (colon == std::string::npos) return std::string::npos;
  std::size_t value = colon + 1;
  while (value < text.size() && std::isspace(static_cast<unsigned char>(text[value]))) ++value;
  return value;
}

std::optional<std::string> json_string_field(const std::string& object, const std::string& key) {
  const std::size_t value = json_after_colon(object, key);
  if (value == std::string::npos || value >= object.size() || object[value] != '"') return std::nullopt;
  return json_string_at(object, value);
}

std::optional<std::string> json_container_field(const std::string& object,
                                                const std::string& key,
                                                char opening,
                                                char closing) {
  const std::size_t value = json_after_colon(object, key);
  if (value == std::string::npos || value >= object.size() || object[value] != opening) {
    return std::nullopt;
  }
  int depth = 0;
  bool in_string = false;
  bool escaped = false;
  for (std::size_t index = value; index < object.size(); ++index) {
    const char character = object[index];
    if (in_string) {
      if (escaped) escaped = false;
      else if (character == '\\') escaped = true;
      else if (character == '"') in_string = false;
      continue;
    }
    if (character == '"') {
      in_string = true;
      continue;
    }
    if (character == opening) ++depth;
    else if (character == closing && --depth == 0) {
      return object.substr(value, index - value + 1);
    }
  }
  return std::nullopt;
}

std::vector<std::string> json_top_level_objects(const std::string& array) {
  std::vector<std::string> objects;
  int depth = 0;
  std::size_t start = std::string::npos;
  bool in_string = false;
  bool escaped = false;
  for (std::size_t index = 0; index < array.size(); ++index) {
    const char character = array[index];
    if (in_string) {
      if (escaped) escaped = false;
      else if (character == '\\') escaped = true;
      else if (character == '"') in_string = false;
      continue;
    }
    if (character == '"') {
      in_string = true;
      continue;
    }
    if (character == '{') {
      if (depth == 0) start = index;
      ++depth;
    } else if (character == '}' && depth > 0) {
      --depth;
      if (depth == 0 && start != std::string::npos) {
        objects.push_back(array.substr(start, index - start + 1));
        start = std::string::npos;
      }
    }
  }
  return objects;
}

std::vector<std::string> json_string_array_field(const std::string& object,
                                                 const std::string& key) {
  const auto array = json_container_field(object, key, '[', ']');
  if (!array) return {};
  std::vector<std::string> values;
  for (std::size_t index = 0; index < array->size(); ++index) {
    if ((*array)[index] != '"') continue;
    const auto value = json_string_at(*array, index);
    if (!value) break;
    values.push_back(*value);
    const std::size_t end = array->find('"', index + 1);
    if (end == std::string::npos) break;
    index = end;
  }
  return values;
}

std::optional<SeedScenario::WireEnvelope> json_wire_envelope_object(const std::string& value) {
  SeedScenario::WireEnvelope envelope;
  envelope.encoding = json_string_field(value, "encoding").value_or("");
  envelope.message = json_string_field(value, "message").value_or("");
  envelope.value = json_string_field(value, "value").value_or("");
  envelope.sha256 = json_string_field(value, "sha256").value_or("");
  if (envelope.encoding.empty() || envelope.message.empty() || envelope.value.empty() || envelope.sha256.empty()) {
    throw std::runtime_error("Rust wire envelope is missing encoding, message, value, or sha256");
  }
  return envelope;
}

std::optional<SeedScenario::WireEnvelope> json_wire_envelope(const std::string& object,
                                                              const std::string& key) {
  const auto value = json_container_field(object, key, '{', '}');
  return value ? json_wire_envelope_object(*value) : std::nullopt;
}

std::vector<SeedScenario::WireEnvelope> json_wire_envelope_array(const std::string& object,
                                                                  const std::string& key) {
  const auto value = json_container_field(object, key, '[', ']');
  if (!value) return {};
  std::vector<SeedScenario::WireEnvelope> envelopes;
  for (const auto& item : json_top_level_objects(*value)) {
    const auto envelope = json_wire_envelope_object(item);
    if (envelope) envelopes.push_back(*envelope);
  }
  return envelopes;
}

std::string read_file(const std::filesystem::path& path);

std::vector<SeedScenario> rust_seed_scenarios(const std::filesystem::path& generated_root,
                                              const std::string& source_revision) {
  const std::vector<std::filesystem::path> candidates = {
      generated_root.parent_path() / "sdk-transport-fixtures-manifest.json",
      generated_root / "sdk-transport-fixtures-manifest.json",
      generated_root.parent_path().parent_path() / "sdk-transport-fixtures-manifest.json",
  };
  std::filesystem::path manifest_path;
  for (const auto& candidate : candidates) {
    if (std::filesystem::is_regular_file(candidate)) {
      manifest_path = candidate;
      break;
    }
  }
  if (manifest_path.empty()) {
    throw std::runtime_error("Rust transport fixture manifest is missing beside the generated bundle");
  }
  const std::string manifest = read_file(manifest_path);
  const std::regex source(
      R"REGEX("source"\s*:\s*\{\s*"revision"\s*:\s*"([^"]+)")REGEX");
  std::smatch source_match;
  if (!std::regex_search(manifest, source_match, source) || source_match[1].str() != source_revision) {
    throw std::runtime_error("Rust transport fixture manifest is bound to a different source revision");
  }
  const auto rpc_object = json_container_field(manifest, "rpc_scenarios", '{', '}');
  const auto scenario_array = rpc_object
      ? json_container_field(*rpc_object, "scenarios", '[', ']')
      : std::nullopt;
  const auto seed_graph = json_container_field(manifest, "seed_graph", '{', '}');
  if (!rpc_object || !scenario_array || !seed_graph) {
    throw std::runtime_error("Rust transport fixture manifest has no RPC scenario and seed graph");
  }
  std::vector<SeedScenario> result;
  for (const auto& object : json_top_level_objects(*scenario_array)) {
    SeedScenario value;
    const auto family = json_string_field(object, "family");
    const auto operation = json_string_field(object, "operation");
    const auto input = json_string_field(object, "input");
    const auto expected = json_string_field(object, "expected");
    const auto seed = json_string_field(object, "seed");
    const auto known_output = json_string_field(object, "known_output");
    const auto order_number = json_after_colon(object, "order");
    if (!family || !operation || order_number == std::string::npos) {
      continue;
    }
    value.family = *family;
    value.operation = *operation;
    value.input = input.value_or("");
    value.expected = expected.value_or("");
    std::size_t order_end = order_number;
    while (order_end < object.size() && std::isdigit(static_cast<unsigned char>(object[order_end]))) ++order_end;
    value.order = static_cast<unsigned int>(std::stoul(object.substr(order_number, order_end - order_number)));
    value.seed = seed.value_or("");
    value.depends_on = json_string_array_field(object, "depends_on");
    value.known_output = known_output.value_or("");
    value.wire_request = json_wire_envelope(object, "wire_request");
    value.wire_expected = json_wire_envelope(object, "wire_expected");
    value.wire_expected_frames = json_wire_envelope_array(object, "wire_expected_frames");
    if (value.wire_expected && !value.wire_expected_frames.empty()) {
      throw std::runtime_error("Rust scenario cannot declare both wire_expected and wire_expected_frames");
    }
    result.push_back(std::move(value));
  }
  if (result.size() != 35) {
    throw std::runtime_error("Rust transport fixture manifest does not contain the 35 RPC scenarios");
  }
  std::sort(result.begin(), result.end(), [](const SeedScenario& left, const SeedScenario& right) {
    return left.order < right.order;
  });
  for (std::size_t index = 0; index < result.size(); ++index) {
    if (result[index].order != index + 1) {
      throw std::runtime_error("Rust semantic seed graph has a non-contiguous order");
    }
    for (const auto& dependency : result[index].depends_on) {
      const bool prior = std::any_of(result.begin(), result.begin() + index,
                                     [&](const SeedScenario& candidate) {
                                       return candidate.family == result[index].family &&
                                              candidate.operation == dependency;
                                     });
      if (!prior) {
        throw std::runtime_error("Rust semantic seed graph dependency is not prior: " +
                                 result[index].family + "/" + result[index].operation +
                                 " depends on " + dependency);
      }
    }
  }
  return result;
}

std::string required_env(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr || *value == '\0') {
    throw std::runtime_error(std::string("missing ") + name);
  }
  return value;
}

std::string json_escape(const std::string& value) {
  std::string escaped;
  for (char c : value) {
    switch (c) {
      case '\\': escaped += "\\\\"; break;
      case '"': escaped += "\\\""; break;
      case '\n': escaped += "\\n"; break;
      case '\r': escaped += "\\r"; break;
      case '\t': escaped += "\\t"; break;
      default: escaped += c; break;
    }
  }
  return escaped;
}

std::string read_file(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) throw std::runtime_error("cannot read " + path.string());
  return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

std::vector<Scenario> authority_scenarios(const std::filesystem::path& path) {
  const std::string authority = read_file(path);
  // rust-authority.json is the only inventory input. Its method identity and
  // stream shape are emitted by the Rust descriptor exporter; this consumer
  // never carries a second list of SDK methods.
  const std::regex method(
      R"REGEX("rpc"\s*:\s*"([^"]+)"\s*,\s*"shape"\s*:\s*"([^"]+)"\s*,\s*"request"\s*:\s*"([^"]+)"\s*,\s*"response"\s*:\s*"([^"]+)"\s*,\s*"response_fields"\s*:\s*\[([^\]]*)\]\s*,\s*"allow_empty_response"\s*:\s*(true|false)\s*,\s*"validations"\s*:\s*\[[^\]]*\]\s*,\s*"response_rules"\s*:\s*\[([^\]]*)\])REGEX");
  std::vector<Scenario> scenarios;
  for (std::sregex_iterator it(authority.begin(), authority.end(), method), end;
       it != end; ++it) {
    scenarios.push_back({(*it)[1].str(), (*it)[2].str(), (*it)[4].str(),
                         quoted_values((*it)[5].str()), (*it)[6].str() == "true",
                         quoted_values((*it)[7].str())});
  }
  if (scenarios.empty()) throw std::runtime_error("Rust authority has no RPC methods");
  for (std::size_t i = 1; i < scenarios.size(); ++i) {
    if (scenarios[i - 1].rpc == scenarios[i].rpc) {
      throw std::runtime_error("Rust authority contains a duplicate RPC: " + scenarios[i].rpc);
    }
  }
  return scenarios;
}

std::string wire_path(const std::string& rpc) {
  const std::size_t slash = rpc.find('/');
  if (slash == std::string::npos) throw std::runtime_error("invalid RPC identity: " + rpc);
  return "/" + rpc.substr(0, slash) + "/" + rpc.substr(slash + 1);
}

std::string descriptor_name(const std::string& rpc) {
  const std::size_t slash = rpc.find('/');
  if (slash == std::string::npos) throw std::runtime_error("invalid RPC identity: " + rpc);
  return rpc.substr(0, slash) + "." + rpc.substr(slash + 1);
}

std::string family_name(const std::string& rpc) {
  const std::size_t first = rpc.find('.');
  if (first == std::string::npos) throw std::runtime_error("RPC has no package: " + rpc);
  if (rpc.compare(0, first, "inference") == 0) return "inference";
  const std::size_t second = rpc.find('.', first + 1);
  if (second == std::string::npos) throw std::runtime_error("RPC has no version: " + rpc);
  return rpc.substr(first + 1, second - first - 1);
}

std::string rpc_operation(const std::string& rpc) {
  const std::size_t slash = rpc.find('/');
  if (slash == std::string::npos || slash + 1 >= rpc.size()) {
    throw std::runtime_error("RPC has no method: " + rpc);
  }
  return snake_case(rpc.substr(slash + 1));
}

std::vector<Scenario> apply_rust_seed_graph(std::vector<Scenario> scenarios,
                                            const std::filesystem::path& generated_root,
                                            const std::string& source_revision) {
  const auto seeds = rust_seed_scenarios(generated_root, source_revision);
  std::vector<bool> consumed(scenarios.size(), false);
  std::vector<Scenario> ordered;
  ordered.reserve(scenarios.size());
  for (const auto& seed : seeds) {
    std::size_t match = scenarios.size();
    for (std::size_t index = 0; index < scenarios.size(); ++index) {
      if (consumed[index]) continue;
      if (family_name(scenarios[index].rpc) == seed.family &&
          rpc_operation(scenarios[index].rpc) == seed.operation) {
        if (match != scenarios.size()) {
          throw std::runtime_error("Rust seed graph matches duplicate RPCs for " + seed.family + "/" + seed.operation);
        }
        match = index;
      }
    }
    if (match == scenarios.size()) {
      throw std::runtime_error("Rust seed graph RPC is absent from the authority: " +
                               seed.family + "/" + seed.operation);
    }
    scenarios[match].rust_seed = seed;
    consumed[match] = true;
    ordered.push_back(std::move(scenarios[match]));
  }
  for (std::size_t index = 0; index < scenarios.size(); ++index) {
    if (!consumed[index]) ordered.push_back(std::move(scenarios[index]));
  }
  return ordered;
}

grpc::ByteBuffer to_buffer(const std::string& bytes) {
  grpc::Slice slice(bytes.data(), bytes.size());
  return grpc::ByteBuffer(&slice, 1);
}

std::string from_buffer(const grpc::ByteBuffer& buffer) {
  std::vector<grpc::Slice> slices;
  const grpc::Status status = buffer.Dump(&slices);
  if (!status.ok()) throw std::runtime_error("cannot decode gRPC response buffer");
  std::string bytes;
  for (const auto& slice : slices) bytes.append(reinterpret_cast<const char*>(slice.begin()), slice.size());
  return bytes;
}

std::string normalized_field_name(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char character) { return static_cast<char>(std::tolower(character)); });
  std::replace(value.begin(), value.end(), '-', '_');
  return value;
}

std::vector<std::pair<std::string, std::string>> seed_inputs(const std::string& input) {
  std::vector<std::pair<std::string, std::string>> values;
  std::size_t start = 0;
  while (start <= input.size()) {
    const std::size_t end = input.find(';', start);
    const std::string token = input.substr(start, end == std::string::npos ? end : end - start);
    const std::size_t equals = token.find('=');
    if (equals != std::string::npos && equals > 0) {
      values.emplace_back(normalized_field_name(token.substr(0, equals)), token.substr(equals + 1));
    }
    if (end == std::string::npos) break;
    start = end + 1;
  }
  return values;
}

std::vector<std::string> seed_path_parts(const std::string& key) {
  std::vector<std::string> parts;
  std::size_t start = 0;
  while (start <= key.size()) {
    const std::size_t end = key.find('.', start);
    parts.push_back(normalized_field_name(key.substr(start, end == std::string::npos ? end : end - start)));
    if (end == std::string::npos) break;
    start = end + 1;
  }
  return parts;
}

std::optional<std::string> decode_base64(const std::string& encoded) {
  static constexpr char alphabet[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  std::string bytes;
  int accumulator = 0;
  int bits = -8;
  for (const unsigned char character : encoded) {
    if (character == '=') break;
    const char* found = std::find(std::begin(alphabet), std::end(alphabet) - 1,
                                  static_cast<char>(character));
    if (found == std::end(alphabet) - 1) return std::nullopt;
    accumulator = (accumulator << 6) | static_cast<int>(found - alphabet);
    bits += 6;
    if (bits >= 0) {
      bytes.push_back(static_cast<char>((accumulator >> bits) & 0xff));
      bits -= 8;
    }
  }
  return bytes;
}

extern "C" unsigned char* SHA256(const unsigned char*, std::size_t, unsigned char*);

std::string sha256_hex(const std::string& input) {
  unsigned char bytes_digest[32] = {};
  if (SHA256(reinterpret_cast<const unsigned char*>(input.data()), input.size(), bytes_digest) == nullptr) {
    throw std::runtime_error("gRPC crypto SHA256 failed");
  }
  static constexpr char digits[] = "0123456789abcdef";
  std::string hex;
  hex.reserve(64);
  for (const auto byte : bytes_digest) {
    hex.push_back(digits[byte >> 4]);
    hex.push_back(digits[byte & 0xfu]);
  }
  return hex;
}

void verify_envelope_hash(const SeedScenario::WireEnvelope& envelope, const std::string& bytes) {
  if (envelope.sha256.empty()) return;
  std::string expected = envelope.sha256;
  if (expected.rfind("sha256:", 0) == 0) expected = expected.substr(7);
  if (expected.size() != 64 ||
      !std::all_of(expected.begin(), expected.end(), [](unsigned char character) {
        return std::isxdigit(character) != 0;
      })) {
    throw std::runtime_error("Rust wire envelope has an invalid sha256 digest");
  }
  std::transform(expected.begin(), expected.end(), expected.begin(),
                 [](unsigned char character) { return static_cast<char>(std::tolower(character)); });
  if (sha256_hex(bytes) != expected) throw std::runtime_error("Rust wire envelope sha256 mismatch");
}

std::unique_ptr<google::protobuf::Message> decode_wire_envelope(
    const SeedScenario::WireEnvelope& envelope,
    const google::protobuf::Descriptor* descriptor,
    google::protobuf::DynamicMessageFactory& factory,
    const std::string& role) {
  if (envelope.message != descriptor->full_name()) {
    throw std::runtime_error("Rust " + role + " message does not match the RPC descriptor: " +
                             envelope.message + " != " + std::string(descriptor->full_name()));
  }
  const auto* prototype = factory.GetPrototype(descriptor);
  if (!prototype) throw std::runtime_error("missing Rust " + role + " message prototype");
  std::unique_ptr<google::protobuf::Message> message(prototype->New());
  if (envelope.encoding == "protobuf-base64") {
    const auto bytes = decode_base64(envelope.value);
    if (!bytes || !message->ParseFromString(*bytes)) {
      throw std::runtime_error("Rust " + role + " protobuf-base64 payload is invalid");
    }
    verify_envelope_hash(envelope, *bytes);
  } else if (envelope.encoding == "protobuf-json") {
    const auto status = google::protobuf::util::JsonStringToMessage(envelope.value, message.get());
    if (!status.ok()) throw std::runtime_error("Rust " + role + " protobuf-json payload is invalid: " +
                                               std::string(status.message()));
    verify_envelope_hash(envelope, message->SerializeAsString());
  } else {
    throw std::runtime_error("Rust " + role + " envelope has unsupported encoding: " + envelope.encoding);
  }
  return message;
}

bool set_seed_scalar(google::protobuf::Message& message,
                     const google::protobuf::FieldDescriptor* field,
                     const std::string& value) {
  const auto* reflection = message.GetReflection();
  if (field->is_repeated() || field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
    return false;
  }
  if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_STRING) {
    if (field->type() == google::protobuf::FieldDescriptor::TYPE_BYTES && value.rfind("hex:", 0) == 0) {
      std::string bytes;
      const std::string hex = value.substr(4);
      for (std::size_t i = 0; i + 1 < hex.size(); i += 2) {
        bytes.push_back(static_cast<char>(std::stoi(hex.substr(i, 2), nullptr, 16)));
      }
      reflection->SetString(&message, field, bytes);
    } else if (field->type() == google::protobuf::FieldDescriptor::TYPE_BYTES && value.rfind("base64:", 0) == 0) {
      const auto bytes = decode_base64(value.substr(7));
      if (!bytes) return false;
      reflection->SetString(&message, field, *bytes);
    } else {
      reflection->SetString(&message, field, value);
    }
    return true;
  }
  if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_BOOL) {
    if (value != "true" && value != "false") return false;
    reflection->SetBool(&message, field, value == "true");
    return true;
  }
  try {
    if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_INT32) {
      reflection->SetInt32(&message, field, std::stoi(value));
      return true;
    }
    if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_INT64) {
      reflection->SetInt64(&message, field, std::stoll(value));
      return true;
    }
    if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_UINT32) {
      reflection->SetUInt32(&message, field, std::stoul(value));
      return true;
    }
    if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_UINT64) {
      reflection->SetUInt64(&message, field, std::stoull(value));
      return true;
    }
    if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_FLOAT) {
      reflection->SetFloat(&message, field, std::stof(value));
      return true;
    }
    if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE) {
      reflection->SetDouble(&message, field, std::stod(value));
      return true;
    }
  } catch (const std::exception&) {
    return false;
  }
  if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_ENUM) {
    for (int index = 0; index < field->enum_type()->value_count(); ++index) {
      const auto* enum_value = field->enum_type()->value(index);
      if (normalized_field_name(std::string(enum_value->name())) == normalized_field_name(value)) {
        reflection->SetEnum(&message, field, enum_value);
        return true;
      }
    }
  }
  return false;
}

bool apply_seed_path(google::protobuf::Message& message,
                     const std::vector<std::string>& parts,
                     std::size_t offset,
                     const std::string& value) {
  if (offset >= parts.size()) return false;
  const auto* descriptor = message.GetDescriptor();
  const auto* reflection = message.GetReflection();
  for (int index = 0; index < descriptor->field_count(); ++index) {
    const auto* field = descriptor->field(index);
    if (normalized_field_name(std::string(field->name())) != parts[offset]) continue;
    if (offset + 1 == parts.size()) return set_seed_scalar(message, field, value);
    if (field->is_repeated() || field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
      return false;
    }
    return apply_seed_path(*reflection->MutableMessage(&message, field), parts, offset + 1, value);
  }
  return false;
}

void apply_seed_input(google::protobuf::Message& request, const SeedScenario& seed) {
  const auto values = seed_inputs(seed.input);
  if (values.empty()) return;
  for (const auto& [key, value] : values) {
    if (key.find('.') != std::string::npos) {
      apply_seed_path(request, seed_path_parts(key), 0, value);
    }
  }
  std::function<bool(google::protobuf::Message&, int)> apply =
      [&](google::protobuf::Message& message, int depth) {
        if (depth > 8) return false;
        bool changed = false;
        const auto* descriptor = message.GetDescriptor();
        const auto* reflection = message.GetReflection();
        for (int index = 0; index < descriptor->field_count(); ++index) {
          const auto* field = descriptor->field(index);
          const std::string field_name = normalized_field_name(std::string(field->name()));
          for (const auto& [key, value] : values) {
            if (key.find('.') == std::string::npos && key == field_name &&
                set_seed_scalar(message, field, value)) changed = true;
          }
          if (field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE || field->is_repeated()) continue;
          // Traverse nested messages so Rust's logical seed names can populate
          // the generated request without a second handwritten request model.
          if (apply(*reflection->MutableMessage(&message, field), depth + 1)) changed = true;
        }
        return changed;
      };
  apply(request, 0);
}

void apply_logical_seed(google::protobuf::Message& request, const std::string& seed) {
  const std::size_t separator = seed.find(':');
  if (separator == std::string::npos || separator == 0 || separator + 1 >= seed.size()) return;
  const std::string kind = normalized_field_name(seed.substr(0, separator));
  const std::string value = seed.substr(separator + 1);
  std::vector<std::string> aliases;
  if (kind == "protocol") aliases = {"version"};
  if (kind == "workspace") aliases = {"name"};
  if (kind == "operation") aliases = {"operation_id", "idempotency_key", "id"};
  if (kind == "cursor") aliases = {"opaque"};
  if (kind == "generation") aliases = {"name", "generation_id"};
  if (aliases.empty()) return;
  const auto matches = [&aliases](const std::string& field_name) {
    const std::string normalized = normalized_field_name(field_name);
    return std::find(aliases.begin(), aliases.end(), normalized) != aliases.end();
  };
  std::function<void(google::protobuf::Message&, int)> apply =
      [&](google::protobuf::Message& message, int depth) {
        if (depth > 8) return;
        const auto* descriptor = message.GetDescriptor();
        const auto* reflection = message.GetReflection();
        for (int index = 0; index < descriptor->field_count(); ++index) {
          const auto* field = descriptor->field(index);
          if (kind == "cursor" && field->is_repeated() &&
              field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE &&
              normalized_field_name(std::string(field->name())) == "cursors") {
            auto* cursor = reflection->AddMessage(&message, field);
            const auto* opaque = cursor->GetDescriptor()->FindFieldByName("opaque");
            if (opaque) set_seed_scalar(*cursor, opaque, value);
          }
          if (matches(std::string(field->name())) && !field->is_repeated()) {
            set_seed_scalar(message, field, value);
          }
          if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE &&
              !field->is_repeated()) {
            apply(*reflection->MutableMessage(&message, field), depth + 1);
          }
        }
      };
  apply(request, 0);
}

void wait_for(grpc::CompletionQueue& queue, void* expected_tag, const std::string& rpc) {
  void* tag = nullptr;
  bool ok = false;
  if (!queue.Next(&tag, &ok) || tag != expected_tag || !ok) {
    throw std::runtime_error(rpc + " completion did not succeed");
  }
}

std::unique_ptr<google::protobuf::Message> request_for(
    const google::protobuf::MethodDescriptor* method,
    google::protobuf::DynamicMessageFactory& factory,
    const std::optional<SeedScenario>& seed) {
  const auto* prototype = factory.GetPrototype(method->input_type());
  if (prototype == nullptr) throw std::runtime_error("missing request prototype for " + std::string(method->full_name()));
  if (seed && seed->wire_request) {
    // A typed Rust envelope is authoritative. It bypasses all compatibility
    // identity defaults below so the consumer cannot silently manufacture a
    // different request when the source-owned bytes are present.
    return decode_wire_envelope(*seed->wire_request, method->input_type(), factory, "request");
  }
  std::unique_ptr<google::protobuf::Message> request(prototype->New());
  if (method->full_name() == "acyclic.actors.v1.ActorsService.CreateActor") {
    const auto* reflection = request->GetReflection();
    const auto* code = method->input_type()->FindFieldByName("code_sha256");
    const auto* region = method->input_type()->FindFieldByName("home_region");
    const auto* key = method->input_type()->FindFieldByName("idempotency_key");
    if (code) reflection->SetString(request.get(), code, std::string(32, '\1'));
    if (region) reflection->SetString(request.get(), region, "fixture");
    if (key) reflection->SetString(request.get(), key, "cpp-rpc-scenario");
  }
  // Seed only the contract identity fields used by the Rust fixture. Other
  // request fields stay at protobuf defaults so this probe cannot invent a
  // second request schema or accidentally exercise optional behavior.
  std::function<void(google::protobuf::Message&, int)> seed_identity =
      [&](google::protobuf::Message& message, int depth) {
        if (depth > 6) return;
        const auto* descriptor = message.GetDescriptor();
        const auto* reflection = message.GetReflection();
        for (int index = 0; index < descriptor->field_count(); ++index) {
          const auto* field = descriptor->field(index);
          const auto field_name = field->name();
          std::string name(field_name.data(), field_name.size());
          std::transform(name.begin(), name.end(), name.begin(),
                         [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
          if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
            if (!field->is_repeated() && reflection->HasField(message, field)) {
              seed_identity(*reflection->MutableMessage(&message, field), depth + 1);
            }
            continue;
          }
          if (field->is_repeated()) continue;
          const std::string value = name.find("actor_id") != std::string::npos ? "fixture-actor"
              : name.find("operation_id") != std::string::npos ? "fixture-operation"
              : name.find("idempotency_key") != std::string::npos ? "cpp-rpc-scenario"
              : name == "owner" ? "fixture-owner"
              : name == "home_region" ? "fixture"
              : name == "path" ? "/fixture"
              : name == "run_id" ? std::string(16, '\2')
              : "";
          if (value.empty()) continue;
          if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_STRING) {
            reflection->SetString(&message, field, value);
          } else if (field->type() == google::protobuf::FieldDescriptor::TYPE_BYTES) {
            reflection->SetString(&message, field, value);
          }
        }
  };
  seed_identity(*request, 0);
  if (seed) apply_logical_seed(*request, seed->seed);
  if (seed) apply_seed_input(*request, *seed);
  return request;
}

struct SemanticEvidence {
  std::string response_type;
  std::vector<std::string> present_fields;
  std::vector<std::string> checked_rules;
  std::vector<std::pair<std::string, bool>> rule_results;
  std::vector<std::pair<std::string, std::string>> concrete_values;
  std::vector<std::tuple<std::string, std::string, std::string>> identity_pairs;
  std::vector<unsigned long long> cursor_trace;
  std::vector<std::pair<unsigned long long, unsigned long long>> resume_ranges;
  std::vector<std::string> status_trace;
  bool identity_matches = false;
  std::size_t response_bytes = 0;
  std::vector<std::string> transitions;
};

std::string bytes_as_hex(const std::string& bytes) {
  static constexpr char digits[] = "0123456789abcdef";
  std::string value;
  value.reserve(bytes.size() * 2);
  for (unsigned char byte : bytes) {
    value.push_back(digits[byte >> 4]);
    value.push_back(digits[byte & 0x0f]);
  }
  return value;
}

std::string scalar_value(const google::protobuf::Message& message,
                         const google::protobuf::FieldDescriptor* field,
                         int index) {
  const auto* reflection = message.GetReflection();
  if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_STRING) {
    const std::string bytes = field->is_repeated()
        ? reflection->GetRepeatedString(message, field, index)
        : reflection->GetString(message, field);
    return field->type() == google::protobuf::FieldDescriptor::TYPE_BYTES
        ? "hex:" + bytes_as_hex(bytes)
        : bytes;
  }
  if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_BOOL) {
    return (field->is_repeated() ? reflection->GetRepeatedBool(message, field, index)
                                 : reflection->GetBool(message, field))
        ? "true" : "false";
  }
  if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_ENUM) {
    const auto* value_descriptor = field->is_repeated()
        ? reflection->GetRepeatedEnum(message, field, index)
        : reflection->GetEnum(message, field);
    return value_descriptor ? std::string(value_descriptor->name()) : "";
  }
  if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_INT32 ||
      field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_INT64) {
    return std::to_string(field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_INT32
        ? (field->is_repeated() ? reflection->GetRepeatedInt32(message, field, index)
                                : reflection->GetInt32(message, field))
        : (field->is_repeated() ? reflection->GetRepeatedInt64(message, field, index)
                                : reflection->GetInt64(message, field)));
  }
  if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_UINT32 ||
      field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_UINT64) {
    return std::to_string(field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_UINT32
        ? (field->is_repeated() ? reflection->GetRepeatedUInt32(message, field, index)
                                : reflection->GetUInt32(message, field))
        : (field->is_repeated() ? reflection->GetRepeatedUInt64(message, field, index)
                                 : reflection->GetUInt64(message, field)));
  }
  if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_FLOAT) {
    return std::to_string(field->is_repeated() ? reflection->GetRepeatedFloat(message, field, index)
                                               : reflection->GetFloat(message, field));
  }
  if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE) {
    return std::to_string(field->is_repeated() ? reflection->GetRepeatedDouble(message, field, index)
                                               : reflection->GetDouble(message, field));
  }
  return "";
}

void collect_concrete_values(const google::protobuf::Message& message,
                             const std::string& prefix,
                             std::vector<std::pair<std::string, std::string>>& values,
                             int depth = 0) {
  if (depth > 8) return;
  std::vector<const google::protobuf::FieldDescriptor*> fields;
  message.GetReflection()->ListFields(message, &fields);
  const auto* reflection = message.GetReflection();
  for (const auto* field : fields) {
    const int count = field->is_repeated() ? reflection->FieldSize(message, field) : 1;
    for (int index = 0; index < count; ++index) {
      const std::string field_name(field->name());
      const std::string name = prefix.empty() ? field_name : prefix + "." + field_name;
      if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
        const auto& child = field->is_repeated()
            ? reflection->GetRepeatedMessage(message, field, index)
            : (reflection->HasField(message, field) ? (reflection->GetMessage)(message, field)
                                                      : message);
        if (!field->is_repeated() && !reflection->HasField(message, field)) continue;
        collect_concrete_values(child, name, values, depth + 1);
      } else {
        const std::string value = scalar_value(message, field, index);
        if (!value.empty()) values.emplace_back(name, value);
      }
    }
  }
}

bool contains_identity(const google::protobuf::Message& message, int depth = 0) {
  if (depth > 8) return false;
  std::vector<const google::protobuf::FieldDescriptor*> fields;
  message.GetReflection()->ListFields(message, &fields);
  for (const auto* field : fields) {
    if (field->name().find("id") != std::string::npos ||
        field->name().find("identity") != std::string::npos ||
        field->name().find("protocol") != std::string::npos ||
        field->name().find("cursor") != std::string::npos) {
      return true;
    }
    if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
      const auto* reflection = message.GetReflection();
      if (field->is_repeated()) {
        for (int i = 0; i < reflection->FieldSize(message, field); ++i) {
          if (contains_identity(reflection->GetRepeatedMessage(message, field, i), depth + 1)) return true;
        }
      } else if (reflection->HasField(message, field) &&
                 contains_identity((reflection->GetMessage)(message, field), depth + 1)) {
        return true;
      }
    }
  }
  return false;
}

bool has_numeric_named_value(const std::vector<std::pair<std::string, std::string>>& values,
                             const std::vector<std::string>& names) {
  for (const auto& value : values) {
    std::string path = value.first;
    std::transform(path.begin(), path.end(), path.begin(),
                   [](unsigned char character) { return static_cast<char>(std::tolower(character)); });
    if (!std::any_of(names.begin(), names.end(), [&](const std::string& name) {
          return path.find(name) != std::string::npos;
        })) {
      continue;
    }
    if (!value.second.empty() &&
        std::all_of(value.second.begin(), value.second.end(),
                    [](unsigned char character) { return std::isdigit(character) != 0; })) {
      return true;
    }
  }
  return false;
}

bool has_true_named_value(const std::vector<std::pair<std::string, std::string>>& values,
                          const std::vector<std::string>& names) {
  for (const auto& value : values) {
    std::string path = value.first;
    std::transform(path.begin(), path.end(), path.begin(),
                   [](unsigned char character) { return static_cast<char>(std::tolower(character)); });
    if (value.second == "true" && std::any_of(names.begin(), names.end(), [&](const std::string& name) {
          return path.find(name) != std::string::npos;
        })) {
      return true;
    }
  }
  return false;
}

bool has_nonempty_named_value(const std::vector<std::pair<std::string, std::string>>& values,
                             const std::vector<std::string>& names) {
  for (const auto& value : values) {
    std::string path = value.first;
    std::transform(path.begin(), path.end(), path.begin(),
                   [](unsigned char character) { return static_cast<char>(std::tolower(character)); });
    if (!value.second.empty() && std::any_of(names.begin(), names.end(), [&](const std::string& name) {
          return path.find(name) != std::string::npos;
        })) {
      return true;
    }
  }
  return false;
}

std::optional<unsigned long long> cursor_number(const std::string& value) {
  std::string candidate = value;
  if (candidate.rfind("hex:", 0) == 0) {
    candidate.clear();
    const std::string hex = value.substr(4);
    for (std::size_t i = 0; i + 1 < hex.size(); i += 2) {
      const auto byte = static_cast<char>(std::stoi(hex.substr(i, 2), nullptr, 16));
      candidate.push_back(byte);
    }
  }
  std::size_t start = candidate.size();
  while (start > 0 && std::isdigit(static_cast<unsigned char>(candidate[start - 1]))) --start;
  if (start == candidate.size()) return std::nullopt;
  try {
    return std::stoull(candidate.substr(start));
  } catch (const std::exception&) {
    return std::nullopt;
  }
}

bool prove_response_rule(const std::string& rule, const SemanticEvidence& evidence) {
  std::string normalized = rule;
  std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                 [](unsigned char character) { return static_cast<char>(std::tolower(character)); });
  if (normalized.find("identity") != std::string::npos) return evidence.identity_matches;
  if (normalized == "response.bounded") {
    // This is the Rust contract's response-size bound, kept explicit so a
    // populated but unrelated scalar cannot satisfy it.
    return evidence.response_bytes <= 2u * 1024u * 1024u;
  }
  if (normalized == "cursor.valid") {
    return has_nonempty_named_value(evidence.concrete_values, {"cursor", "next"});
  }
  if (normalized == "terminal.required") {
    return has_true_named_value(evidence.concrete_values, {"terminal"});
  }
  if (normalized == "cursor.monotonic") {
    return evidence.cursor_trace.size() >= 2 &&
           std::is_sorted(evidence.cursor_trace.begin(), evidence.cursor_trace.end());
  }
  if (normalized == "resume_cursor.contiguous") {
    if (!evidence.resume_ranges.empty()) {
      for (std::size_t i = 0; i < evidence.resume_ranges.size(); ++i) {
        const auto [from, through] = evidence.resume_ranges[i];
        if (through < from) return false;
        if (i > 0 && from != evidence.resume_ranges[i - 1].second + 1) return false;
      }
      return true;
    }
    if (evidence.cursor_trace.size() < 2) return false;
    for (std::size_t i = 1; i < evidence.cursor_trace.size(); ++i) {
      if (evidence.cursor_trace[i] != evidence.cursor_trace[i - 1] + 1) return false;
    }
    return true;
  }
  throw std::runtime_error(
      "unsupported Rust response rule without a multi-response oracle " + rule);
}

void observe_response(const google::protobuf::Message& response,
                      const google::protobuf::Message& request,
                      const google::protobuf::MethodDescriptor* method,
                      const Scenario& scenario, SemanticEvidence& evidence) {
  if (evidence.response_type.empty()) {
    const auto response_type = method->output_type()->full_name();
    evidence.response_type.assign(response_type.data(), response_type.size());
    evidence.checked_rules = scenario.response_rules;
  }
  evidence.response_bytes += response.ByteSizeLong();
  std::vector<const google::protobuf::FieldDescriptor*> fields;
  response.GetReflection()->ListFields(response, &fields);
  for (const auto* field : fields) {
    const auto field_name = field->name();
    const std::string field_text(field_name.data(), field_name.size());
    evidence.present_fields.push_back(field_text);
    if (std::find(scenario.response_fields.begin(), scenario.response_fields.end(), field_text) ==
        scenario.response_fields.end()) {
      const auto method_name = method->full_name();
      throw std::runtime_error(std::string(method_name.data(), method_name.size()) +
                               " returned undeclared field " + field_text);
    }
  }
  std::vector<std::pair<std::string, std::string>> response_values;
  collect_concrete_values(response, "", response_values);
  const auto has_response_rule = [&scenario](const char* wanted) {
    return std::any_of(scenario.response_rules.begin(), scenario.response_rules.end(),
                       [wanted](const std::string& rule) {
                         std::string normalized = rule;
                         std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                                        [](unsigned char character) {
                                          return static_cast<char>(std::tolower(character));
                                        });
                         return normalized == wanted;
                       });
  };
  const bool needs_cursor_numbers = has_response_rule("cursor.monotonic");
  const bool needs_resume_ranges = has_response_rule("resume_cursor.contiguous");
  std::vector<std::pair<std::string, std::string>> request_values;
  collect_concrete_values(request, "", request_values);
  for (const auto& response_value : response_values) {
    evidence.concrete_values.push_back(response_value);
    for (const auto& request_value : request_values) {
      const auto request_leaf = request_value.first.substr(request_value.first.rfind('.') + 1);
      const auto response_leaf = response_value.first.substr(response_value.first.rfind('.') + 1);
      if (request_leaf == response_leaf && request_value.second == response_value.second) {
        evidence.identity_pairs.emplace_back(response_leaf, request_value.second, response_value.second);
        evidence.identity_matches = true;
      }
    }
  }
  for (const auto& value : response_values) {
    std::string normalized = value.first;
    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                   [](unsigned char character) { return static_cast<char>(std::tolower(character)); });
    if (needs_cursor_numbers && (normalized.find("cursor") != std::string::npos ||
                                 normalized.find("sequence") != std::string::npos)) {
      if (const auto number = cursor_number(value.second)) evidence.cursor_trace.push_back(*number);
    }
    if (normalized.find("status") != std::string::npos ||
        normalized.find("state") != std::string::npos ||
        normalized.find("terminal") != std::string::npos ||
        normalized.find("outcome") != std::string::npos) {
      if (!value.second.empty()) evidence.status_trace.push_back(value.second);
    }
  }
  std::optional<unsigned long long> from_revision;
  std::optional<unsigned long long> through_revision;
  for (const auto& value : response_values) {
    std::string normalized = value.first;
    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                   [](unsigned char character) { return static_cast<char>(std::tolower(character)); });
    if (!needs_resume_ranges) continue;
    const auto number = cursor_number(value.second);
    if (!number) continue;
    if (normalized.find("fromrevision") != std::string::npos) from_revision = number;
    if (normalized.find("throughrevision") != std::string::npos) through_revision = number;
  }
  if (from_revision && through_revision) evidence.resume_ranges.emplace_back(*from_revision, *through_revision);
}

SemanticEvidence finalize_semantic_evidence(SemanticEvidence evidence,
                                            const google::protobuf::MethodDescriptor* method,
                                            const Scenario& scenario) {
  for (const auto& rule : scenario.response_rules) {
    const bool proved = prove_response_rule(rule, evidence);
    evidence.rule_results.emplace_back(rule, proved);
    if (!proved) throw std::runtime_error("responses did not prove Rust rule " + rule);
  }
  std::sort(evidence.present_fields.begin(), evidence.present_fields.end());
  if (scenario.allow_empty_response != evidence.present_fields.empty()) {
    const auto method_name = method->full_name();
    throw std::runtime_error(std::string(method_name.data(), method_name.size()) +
                             " violated Rust empty-response rule");
  }
  if (std::any_of(scenario.response_rules.begin(), scenario.response_rules.end(),
                  [](const std::string& rule) { return rule.find("identity") != std::string::npos; }) &&
      !evidence.identity_matches) {
    const auto method_name = method->full_name();
    throw std::runtime_error(std::string(method_name.data(), method_name.size()) +
                             " did not preserve an identity field");
  }
  return evidence;
}

SemanticEvidence semantic_evidence(const google::protobuf::Message& response,
                                   const google::protobuf::Message& request,
                                   const google::protobuf::MethodDescriptor* method,
                                   const Scenario& scenario) {
  SemanticEvidence evidence;
  observe_response(response, request, method, scenario, evidence);
  return finalize_semantic_evidence(std::move(evidence), method, scenario);
}

struct InvocationResult {
  int responses;
  SemanticEvidence semantic;
};

void verify_wire_expected(const google::protobuf::Message& response,
                          const google::protobuf::MethodDescriptor* method,
                          const Scenario& scenario,
                          google::protobuf::DynamicMessageFactory& factory) {
  if (!scenario.rust_seed || !scenario.rust_seed->wire_expected) return;
  const auto expected = decode_wire_envelope(*scenario.rust_seed->wire_expected,
                                             method->output_type(), factory, "expected response");
  if (!google::protobuf::util::MessageDifferencer::Equivalent(response, *expected)) {
    throw std::runtime_error(std::string(method->full_name()) +
                             " response differs from the Rust-owned expected protobuf message");
  }
}

void verify_expected_frame_count(const std::string& rpc, int actual, std::size_t expected) {
  if (expected != 0 && actual != static_cast<int>(expected)) {
    throw std::runtime_error(rpc + " returned " + std::to_string(actual) +
                             " frames; Rust expected " + std::to_string(expected));
  }
}

InvocationResult invoke_unary(grpc::GenericStub& stub, const std::string& path,
                 const grpc::ByteBuffer& request, const google::protobuf::Message& request_message,
                 const google::protobuf::MethodDescriptor* method,
                 const Scenario& scenario, google::protobuf::DynamicMessageFactory& factory) {
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
  grpc::CompletionQueue queue;
  auto call = stub.PrepareUnaryCall(&context, path, request, &queue);
  if (!call) throw std::runtime_error("could not prepare " + path);
  call->StartCall();
  const auto* prototype = factory.GetPrototype(method->output_type());
  if (!prototype) throw std::runtime_error("missing response prototype for " + path);
  grpc::ByteBuffer response_buffer;
  grpc::Status status;
  void* const tag = reinterpret_cast<void*>(static_cast<uintptr_t>(1));
  call->Finish(&response_buffer, &status, tag);
  wait_for(queue, tag, path);
  if (!status.ok()) throw std::runtime_error(path + " failed: " + status.error_message());
  std::unique_ptr<google::protobuf::Message> response(prototype->New());
  if (!response->ParseFromString(from_buffer(response_buffer))) {
    throw std::runtime_error(path + " returned invalid protobuf bytes");
  }
  verify_wire_expected(*response, method, scenario, factory);
  SemanticEvidence evidence = semantic_evidence(*response, request_message, method, scenario);
  evidence.transitions = {"request_sent", "response_received", "completed"};
  return {1, std::move(evidence)};
}

InvocationResult invoke_stream(grpc::GenericStub& stub, const std::string& path,
                  const grpc::ByteBuffer& request, const google::protobuf::Message& request_message,
                  const google::protobuf::MethodDescriptor* method,
                  const Scenario& scenario, google::protobuf::DynamicMessageFactory& factory) {
  std::vector<std::unique_ptr<google::protobuf::Message>> expected_frames;
  if (scenario.rust_seed && scenario.rust_seed->wire_expected) {
    throw std::runtime_error(std::string(method->full_name()) +
                             " has a typed expected response but no Rust-owned frame sequence");
  }
  if (scenario.rust_seed) {
    for (const auto& envelope : scenario.rust_seed->wire_expected_frames) {
      expected_frames.push_back(decode_wire_envelope(envelope, method->output_type(), factory,
                                                     "expected stream response"));
    }
  }
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
  grpc::CompletionQueue queue;
  auto call = stub.PrepareCall(&context, path, &queue);
  if (!call) throw std::runtime_error("could not prepare " + path);
  void* const start_tag = reinterpret_cast<void*>(static_cast<uintptr_t>(1));
  void* const write_tag = reinterpret_cast<void*>(static_cast<uintptr_t>(2));
  void* const done_tag = reinterpret_cast<void*>(static_cast<uintptr_t>(3));
  void* const read_tag = reinterpret_cast<void*>(static_cast<uintptr_t>(4));
  void* const finish_tag = reinterpret_cast<void*>(static_cast<uintptr_t>(5));
  call->StartCall(start_tag);
  wait_for(queue, start_tag, path);
  call->Write(request, write_tag);
  wait_for(queue, write_tag, path);
  call->WritesDone(done_tag);
  wait_for(queue, done_tag, path);

  const auto* prototype = factory.GetPrototype(method->output_type());
  if (!prototype) throw std::runtime_error("missing response prototype for " + path);
  int responses = 0;
  SemanticEvidence semantic;
  semantic.transitions.push_back("request_sent");
  for (;;) {
    grpc::ByteBuffer response_buffer;
    call->Read(&response_buffer, read_tag);
    void* tag = nullptr;
    bool ok = false;
    if (!queue.Next(&tag, &ok) || tag != read_tag) {
      throw std::runtime_error(path + " read completion disappeared");
    }
    if (!ok) break;
    std::unique_ptr<google::protobuf::Message> response(prototype->New());
    if (!response->ParseFromString(from_buffer(response_buffer))) {
      throw std::runtime_error(path + " returned invalid protobuf bytes");
    }
    if (!expected_frames.empty()) {
      if (responses >= static_cast<int>(expected_frames.size())) {
        throw std::runtime_error(path + " returned more frames than the Rust-owned expectation");
      }
      if (!google::protobuf::util::MessageDifferencer::Equivalent(*response, *expected_frames[responses])) {
        throw std::runtime_error(path + " frame " + std::to_string(responses + 1) +
                                 " differs from the Rust-owned expected protobuf message");
      }
    }
    observe_response(*response, request_message, method, scenario, semantic);
    semantic.transitions.push_back("response_received");
    ++responses;
  }
  grpc::Status status;
  call->Finish(&status, finish_tag);
  wait_for(queue, finish_tag, path);
  if (!status.ok()) throw std::runtime_error(path + " failed: " + status.error_message());
  verify_expected_frame_count(path, responses, expected_frames.size());
  semantic.transitions.push_back("completed");
  return {responses, finalize_semantic_evidence(std::move(semantic), method, scenario)};
}

void write_scenario(const std::filesystem::path& root, const std::string& revision,
                    const Scenario& scenario, const InvocationResult& invocation, int number) {
  std::ostringstream filename;
  filename << "cpp-rpc-" << std::setw(3) << std::setfill('0') << number << ".json";
  const auto path = root / "qualification" / "consumers" / filename.str();
  std::filesystem::create_directories(path.parent_path());
  std::ofstream output(path);
  output << "{\"schema\":\"acyclic.sdk.rpc-scenario-result.v1\","
         << "\"source_revision\":\"" << json_escape(revision) << "\","
         << "\"status\":\"passed\",\"invoked\":true,\"exit_code\":0,"
         << "\"family\":\"" << json_escape(family_name(scenario.rpc)) << "\","
         << "\"rpc\":\"" << json_escape(scenario.rpc) << "\"," 
         << "\"shape\":\"" << json_escape(scenario.shape) << "\"," 
         << "\"transport\":\"grpc\",\"execution_mode\":\"remote\"," 
         << "\"rust_seed\":";
  if (scenario.rust_seed) {
    const auto emit_envelope = [&output](const char* key, const SeedScenario::WireEnvelope& envelope) {
      output << ",\"" << key << "\":{\"encoding\":\"" << json_escape(envelope.encoding)
             << "\",\"message\":\"" << json_escape(envelope.message)
             << "\",\"value\":\"" << json_escape(envelope.value)
             << "\",\"sha256\":\"" << json_escape(envelope.sha256) << "\"}";
    };
    output << "{\"family\":\"" << json_escape(scenario.rust_seed->family)
           << "\",\"operation\":\"" << json_escape(scenario.rust_seed->operation)
           << "\",\"input\":\"" << json_escape(scenario.rust_seed->input)
           << "\",\"expected\":\"" << json_escape(scenario.rust_seed->expected)
           << "\",\"order\":" << scenario.rust_seed->order
           << ",\"seed\":\"" << json_escape(scenario.rust_seed->seed)
           << "\",\"depends_on\":[";
    for (std::size_t i = 0; i < scenario.rust_seed->depends_on.size(); ++i) {
      if (i) output << ',';
      output << "\"" << json_escape(scenario.rust_seed->depends_on[i]) << "\"";
    }
    output << "],\"known_output\":\""
           << json_escape(scenario.rust_seed->known_output) << "\"";
    if (scenario.rust_seed->wire_request) emit_envelope("wire_request", *scenario.rust_seed->wire_request);
    if (scenario.rust_seed->wire_expected) emit_envelope("wire_expected", *scenario.rust_seed->wire_expected);
    if (!scenario.rust_seed->wire_expected_frames.empty()) {
      output << ",\"wire_expected_frames\":[";
      for (std::size_t index = 0; index < scenario.rust_seed->wire_expected_frames.size(); ++index) {
        if (index) output << ',';
        const auto& envelope = scenario.rust_seed->wire_expected_frames[index];
        output << "{\"encoding\":\"" << json_escape(envelope.encoding)
               << "\",\"message\":\"" << json_escape(envelope.message)
               << "\",\"value\":\"" << json_escape(envelope.value)
               << "\",\"sha256\":\"" << json_escape(envelope.sha256) << "\"}";
      }
      output << ']';
    }
    output << "}";
  } else {
    output << "null";
  }
  output
         << ",\"rpc_outcome\":{\"status\":\"ok\",\"code\":0,\"response_count\":"
         << invocation.responses << "},\"semantic_evidence\":{\"response_type\":\""
         << json_escape(invocation.semantic.response_type) << "\",\"present_fields\":[";
  for (std::size_t i = 0; i < invocation.semantic.present_fields.size(); ++i) {
    if (i) output << ',';
    output << "\"" << json_escape(invocation.semantic.present_fields[i]) << "\"";
  }
  output << "],\"checked_rules\":[";
  for (std::size_t i = 0; i < invocation.semantic.checked_rules.size(); ++i) {
    if (i) output << ',';
    output << "\"" << json_escape(invocation.semantic.checked_rules[i]) << "\"";
  }
  output << "],\"rule_results\":{";
  for (std::size_t i = 0; i < invocation.semantic.rule_results.size(); ++i) {
    if (i) output << ',';
    output << "\"" << json_escape(invocation.semantic.rule_results[i].first) << "\":"
           << (invocation.semantic.rule_results[i].second ? "true" : "false");
  }
  output << "},\"concrete_values\":[";
  for (std::size_t i = 0; i < invocation.semantic.concrete_values.size(); ++i) {
    if (i) output << ',';
    output << "{\"path\":\"" << json_escape(invocation.semantic.concrete_values[i].first)
           << "\",\"value\":\"" << json_escape(invocation.semantic.concrete_values[i].second)
           << "\"}";
  }
  output << "],\"identity_matches\":" << (invocation.semantic.identity_matches ? "true" : "false")
         << ",\"observations\":{\"identity_pairs\":[";
  for (std::size_t i = 0; i < invocation.semantic.identity_pairs.size(); ++i) {
    if (i) output << ',';
    output << "{\"field\":\"" << json_escape(std::get<0>(invocation.semantic.identity_pairs[i]))
           << "\",\"request\":\"" << json_escape(std::get<1>(invocation.semantic.identity_pairs[i]))
           << "\",\"response\":\"" << json_escape(std::get<2>(invocation.semantic.identity_pairs[i]))
           << "\"}";
  }
  output << "],\"cursor_trace\":[";
  for (std::size_t i = 0; i < invocation.semantic.cursor_trace.size(); ++i) {
    if (i) output << ',';
    output << invocation.semantic.cursor_trace[i];
  }
  output << "],\"status_trace\":[";
  for (std::size_t i = 0; i < invocation.semantic.status_trace.size(); ++i) {
    if (i) output << ',';
    output << "\"" << json_escape(invocation.semantic.status_trace[i]) << "\"";
  }
  output << "],\"transitions\":[";
  for (std::size_t i = 0; i < invocation.semantic.transitions.size(); ++i) {
    if (i) output << ',';
    output << "{\"kind\":\"" << json_escape(invocation.semantic.transitions[i]) << "\"}";
  }
  output << "]}";
  output << "},\"checks\":[\"invocation\",\"transport\",\"receiver-response\",\"serialization\"]}\n";
}

}  // namespace

int main() {
  try {
    const char* fixture = std::getenv("FIXTURE_GRPC_ADDRESS");
    if (fixture == nullptr || *fixture == '\0') {
      std::cout << "cpp-consumer=generated-only (FIXTURE_GRPC_ADDRESS not set)\n";
      return 0;
    }
    const std::filesystem::path generated_root = required_env("ACYCLIC_GENERATED_ROOT");
    const char* scenario_value = std::getenv("ACYCLIC_SCENARIO_OUTPUT");
    const std::filesystem::path scenario_root = scenario_value ? scenario_value : generated_root;
    const std::string revision = required_env("ACYCLIC_SOURCE_REVISION");
    const auto scenarios = apply_rust_seed_graph(
        authority_scenarios(generated_root / "rust-authority.json"), generated_root, revision);
    auto channel = grpc::CreateChannel(fixture, grpc::InsecureChannelCredentials());
    grpc::GenericStub stub(channel);
    google::protobuf::DynamicMessageFactory factory;
    int total_responses = 0;
    int number = 0;
    for (const auto& scenario : scenarios) {
      const std::string path = wire_path(scenario.rpc);
      const auto* method = google::protobuf::DescriptorPool::generated_pool()->FindMethodByName(
          descriptor_name(scenario.rpc));
      if (!method) throw std::runtime_error("generated descriptor missing " + scenario.rpc);
      const auto request = request_for(method, factory, scenario.rust_seed);
      const InvocationResult invocation = scenario.shape == "unary"
          ? invoke_unary(stub, path, to_buffer(request->SerializeAsString()), *request, method, scenario, factory)
          : invoke_stream(stub, path, to_buffer(request->SerializeAsString()), *request, method, scenario, factory);
      if (invocation.responses <= 0) throw std::runtime_error(scenario.rpc + " returned no responses");
      write_scenario(scenario_root, revision, scenario, invocation, ++number);
      total_responses += invocation.responses;
    }
    std::cout << "cpp-rpc-scenarios=passed methods=" << scenarios.size()
              << " responses=" << total_responses << "\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "cpp-rpc-scenarios=failed: " << error.what() << "\n";
    return 1;
  }
}
