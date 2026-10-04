#include <grpcpp/generic/generic_stub.h>
#include <grpcpp/grpcpp.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <memory>
#include <iostream>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace {

struct Scenario {
  std::string rpc;
  std::string shape;
};

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
      R"REGEX("rpc"\s*:\s*"([^"]+)"\s*,\s*"shape"\s*:\s*"([^"]+)")REGEX");
  std::vector<Scenario> scenarios;
  for (std::sregex_iterator it(authority.begin(), authority.end(), method), end;
       it != end; ++it) {
    scenarios.push_back({(*it)[1].str(), (*it)[2].str()});
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
  return request;
}

int invoke_unary(grpc::GenericStub& stub, const std::string& path,
                 const grpc::ByteBuffer& request, const google::protobuf::MethodDescriptor* method,
                 google::protobuf::DynamicMessageFactory& factory) {
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
  return 1;
}

int invoke_stream(grpc::GenericStub& stub, const std::string& path,
                  const grpc::ByteBuffer& request, const google::protobuf::MethodDescriptor* method,
                  google::protobuf::DynamicMessageFactory& factory) {
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
    ++responses;
  }
  grpc::Status status;
  call->Finish(&status, finish_tag);
  wait_for(queue, finish_tag, path);
  if (!status.ok()) throw std::runtime_error(path + " failed: " + status.error_message());
  return responses;
}

void write_scenario(const std::filesystem::path& root, const std::string& revision,
                    const Scenario& scenario, int response_count, int number) {
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
         << response_count << "},\"checks\":[\"invocation\",\"transport\",\"serialization\"]}\n";
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
      const int responses = scenario.shape == "unary"
          ? invoke_unary(stub, path, to_buffer(request->SerializeAsString()), method, factory)
          : invoke_stream(stub, path, to_buffer(request->SerializeAsString()), method, factory);
      if (responses <= 0) throw std::runtime_error(scenario.rpc + " returned no responses");
      write_scenario(scenario_root, revision, scenario, responses, ++number);
      total_responses += responses;
    }
    std::cout << "cpp-rpc-scenarios=passed methods=" << scenarios.size()
              << " responses=" << total_responses << "\n";
    return 0;
  } catch (const std::exception& error) {
    std::cerr << "cpp-rpc-scenarios=failed: " << error.what() << "\n";
    return 1;
  }
}
