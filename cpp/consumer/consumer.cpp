#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/grpcpp.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <fstream>
#include <iomanip>
#include <memory>
#include <iostream>
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

struct Scenario {
  std::string rpc;
  std::string shape;
  std::string response;
  std::vector<std::string> response_fields;
  bool allow_empty_response = false;
  std::vector<std::string> response_rules;
};

std::vector<std::string> quoted_values(const std::string& text) {
  const std::regex value(R"REGEX("([^"]+)")REGEX");
  std::vector<std::string> values;
  for (std::sregex_iterator it(text.begin(), text.end(), value), end; it != end; ++it) {
    values.push_back((*it)[1].str());
  }
  return values;
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
  std::sort(scenarios.begin(), scenarios.end(), [](const Scenario& left, const Scenario& right) {
    return left.rpc < right.rpc;
  });
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

void wait_for(grpc::CompletionQueue& queue, void* expected_tag, const std::string& rpc) {
  void* tag = nullptr;
  bool ok = false;
  if (!queue.Next(&tag, &ok) || tag != expected_tag || !ok) {
    throw std::runtime_error(rpc + " completion did not succeed");
  }
}

std::unique_ptr<google::protobuf::Message> request_for(
    const google::protobuf::MethodDescriptor* method,
    google::protobuf::DynamicMessageFactory& factory) {
  const auto* prototype = factory.GetPrototype(method->input_type());
  if (prototype == nullptr) throw std::runtime_error("missing request prototype for " + std::string(method->full_name()));
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
    return !evidence.cursor_trace.empty();
  }
  if (normalized == "terminal.required") {
    return has_true_named_value(evidence.concrete_values, {"terminal"});
  }
  if (normalized == "cursor.monotonic") {
    return evidence.cursor_trace.size() >= 2 &&
           std::is_sorted(evidence.cursor_trace.begin(), evidence.cursor_trace.end());
  }
  if (normalized == "resume_cursor.contiguous") {
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
    if (normalized.find("cursor") != std::string::npos ||
        normalized.find("sequence") != std::string::npos) {
      try {
        std::size_t consumed = 0;
        const auto number = std::stoull(value.second, &consumed);
        if (consumed == value.second.size()) evidence.cursor_trace.push_back(number);
      } catch (const std::exception&) {
        throw std::runtime_error("cursor observation is not a non-negative integer");
      }
    }
    if (normalized.find("status") != std::string::npos ||
        normalized.find("state") != std::string::npos ||
        normalized.find("terminal") != std::string::npos ||
        normalized.find("outcome") != std::string::npos) {
      if (!value.second.empty()) evidence.status_trace.push_back(value.second);
    }
  }
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
  SemanticEvidence evidence = semantic_evidence(*response, request_message, method, scenario);
  evidence.transitions = {"request_sent", "response_received", "completed"};
  return {1, std::move(evidence)};
}

InvocationResult invoke_stream(grpc::GenericStub& stub, const std::string& path,
                  const grpc::ByteBuffer& request, const google::protobuf::Message& request_message,
                  const google::protobuf::MethodDescriptor* method,
                  const Scenario& scenario, google::protobuf::DynamicMessageFactory& factory) {
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
    observe_response(*response, request_message, method, scenario, semantic);
    semantic.transitions.push_back("response_received");
    ++responses;
  }
  grpc::Status status;
  call->Finish(&status, finish_tag);
  wait_for(queue, finish_tag, path);
  if (!status.ok()) throw std::runtime_error(path + " failed: " + status.error_message());
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
         << "\"rpc_outcome\":{\"status\":\"ok\",\"code\":0,\"response_count\":"
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
    const auto scenarios = authority_scenarios(generated_root / "rust-authority.json");
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
      const auto request = request_for(method, factory);
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
