// Controls prepared from checksum-verified accepted Rust source.
#define main descriptor_controls
#include "message_consumer.cc"
#undef main
#include <grpcpp/grpcpp.h>
#include <grpcpp/support/server_interceptor.h>
#include <chrono>
#include <map>
#include <mutex>
#include "actors/v1/actors.grpc.pb.h"
#include "workers/v1/workers.grpc.pb.h"
#include "stream/v2/stream.grpc.pb.h"
using RpcType = grpc::experimental::ServerRpcInfo::Type;
std::mutex audit_mutex;
std::map<std::string, std::pair<RpcType, int>> observed;
class AuditFactory final : public grpc::experimental::ServerInterceptorFactoryInterface {
 public:
 grpc::experimental::Interceptor* CreateServerInterceptor(grpc::experimental::ServerRpcInfo* info) override {
  std::lock_guard<std::mutex> lock(audit_mutex);
  auto found = observed.find(info->method());
  if (found == observed.end()) observed.emplace(info->method(), std::make_pair(info->type(), 1));
  else { if (found->second.first != info->type()) found->second.second = -1000; else ++found->second.second; }
  return nullptr;
 }
};
void seed_present(Message& message, int seed) {
 auto reflection=message.GetReflection(); std::vector<const FieldDescriptor*> fields;
 reflection->ListFields(message, &fields);
 for (auto field : fields) {
  const bool repeated=field->is_repeated(); const int count=repeated?reflection->FieldSize(message, field):1;
  for (int i=0;i<count;++i) switch(field->cpp_type()) {
   case FieldDescriptor::CPPTYPE_MESSAGE: seed_present(*(repeated?reflection->MutableRepeatedMessage(&message,field,i):reflection->MutableMessage(&message,field)),seed);break;
   case FieldDescriptor::CPPTYPE_STRING: { auto value=field->type()==FieldDescriptor::TYPE_BYTES?std::string("\0\xff",2)+std::to_string(seed):std::string("probe-")+std::to_string(seed); if(repeated)reflection->SetRepeatedString(&message,field,i,value);else reflection->SetString(&message,field,value);break; }
   case FieldDescriptor::CPPTYPE_UINT32: if(repeated)reflection->SetRepeatedUInt32(&message,field,i,std::numeric_limits<uint32_t>::max()-seed);else reflection->SetUInt32(&message,field,std::numeric_limits<uint32_t>::max()-seed);break;
   case FieldDescriptor::CPPTYPE_UINT64: if(repeated)reflection->SetRepeatedUInt64(&message,field,i,std::numeric_limits<uint64_t>::max()-seed);else reflection->SetUInt64(&message,field,std::numeric_limits<uint64_t>::max()-seed);break;
   case FieldDescriptor::CPPTYPE_INT32: if(repeated)reflection->SetRepeatedInt32(&message,field,i,120+seed);else reflection->SetInt32(&message,field,120+seed);break;
   case FieldDescriptor::CPPTYPE_INT64: if(repeated)reflection->SetRepeatedInt64(&message,field,i,120+seed);else reflection->SetInt64(&message,field,120+seed);break;
   case FieldDescriptor::CPPTYPE_DOUBLE: if(repeated)reflection->SetRepeatedDouble(&message,field,i,1.25+seed);else reflection->SetDouble(&message,field,1.25+seed);break;
   case FieldDescriptor::CPPTYPE_FLOAT: if(repeated)reflection->SetRepeatedFloat(&message,field,i,1.25f+seed);else reflection->SetFloat(&message,field,1.25f+seed);break;
   case FieldDescriptor::CPPTYPE_BOOL: if(repeated)reflection->SetRepeatedBool(&message,field,i,seed%2);else reflection->SetBool(&message,field,seed%2);break;
   case FieldDescriptor::CPPTYPE_ENUM: break;
  }
 }
}
void sample(Message& message, int seed) { populate(message); seed_present(message, seed); }
template<class Request> bool request_matches(const Request& request) {
 Request expected; sample(expected, 1);
 return google::protobuf::util::MessageDifferencer::Equals(request, expected);
}
class ActorsServiceProbe final : public acyclic::actors::v1::ActorsService::Service {
 public:
 grpc::Status CreateActor(grpc::ServerContext*, const acyclic::actors::v1::CreateActorRequest* request, acyclic::actors::v1::CreateActorResponse* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
 grpc::Status UpdateActor(grpc::ServerContext*, const acyclic::actors::v1::UpdateActorRequest* request, acyclic::actors::v1::UpdateActorResponse* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
 grpc::Status InspectActor(grpc::ServerContext*, const acyclic::actors::v1::InspectActorRequest* request, acyclic::actors::v1::InspectActorResponse* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
 grpc::Status AddSubscription(grpc::ServerContext*, const acyclic::actors::v1::AddSubscriptionRequest* request, acyclic::actors::v1::AddSubscriptionResponse* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
 grpc::Status RemoveSubscription(grpc::ServerContext*, const acyclic::actors::v1::RemoveSubscriptionRequest* request, acyclic::actors::v1::RemoveSubscriptionResponse* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
 grpc::Status ResumeSubscription(grpc::ServerContext*, const acyclic::actors::v1::ResumeSubscriptionRequest* request, acyclic::actors::v1::ResumeSubscriptionResponse* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
 grpc::Status CheckpointActor(grpc::ServerContext*, const acyclic::actors::v1::CheckpointActorRequest* request, acyclic::actors::v1::CheckpointActorResponse* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
 grpc::Status InvokeActor(grpc::ServerContext*, const acyclic::actors::v1::InvokeActorRequest* request, acyclic::actors::v1::InvokeActorResponse* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
};
class WorkersServiceProbe final : public acyclic::workers::v1::WorkersService::Service {
 public:
 grpc::Status PublishVersion(grpc::ServerContext*, const acyclic::workers::v1::PublishVersionRequest* request, acyclic::workers::v1::PublishVersionResponse* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
 grpc::Status SelectDeployment(grpc::ServerContext*, const acyclic::workers::v1::SelectDeploymentRequest* request, acyclic::workers::v1::SelectDeploymentResponse* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
 grpc::Status SubmitJob(grpc::ServerContext*, const acyclic::workers::v1::SubmitJobRequest* request, acyclic::workers::v1::SubmitJobResponse* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
 grpc::Status InspectJob(grpc::ServerContext*, const acyclic::workers::v1::InspectJobRequest* request, acyclic::workers::v1::InspectJobResponse* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
 grpc::Status CancelJob(grpc::ServerContext*, const acyclic::workers::v1::CancelJobRequest* request, acyclic::workers::v1::CancelJobResponse* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
 grpc::Status InvokeVersion(grpc::ServerContext*, const acyclic::workers::v1::InvokeVersionRequest* request, acyclic::workers::v1::InvokeResponse* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
 grpc::Status InvokeDeployment(grpc::ServerContext*, const acyclic::workers::v1::InvokeDeploymentRequest* request, acyclic::workers::v1::InvokeResponse* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
};
class StreamServiceProbe final : public acyclic::stream::v2::StreamService::Service {
 public:
 grpc::Status InspectIdempotency(grpc::ServerContext*, const acyclic::stream::v2::InspectIdempotencyRequest* request, acyclic::stream::v2::InspectIdempotencyResponse* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
 grpc::Status Append(grpc::ServerContext*, const acyclic::stream::v2::AppendRequest* request, acyclic::stream::v2::AppendResponse* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
 grpc::Status Tail(grpc::ServerContext*, const acyclic::stream::v2::TailRequest* request, acyclic::stream::v2::TailResponse* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
 grpc::Status Fork(grpc::ServerContext*, const acyclic::stream::v2::ForkRequest* request, acyclic::stream::v2::ForkReceipt* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
 grpc::Status Read(grpc::ServerContext*, const acyclic::stream::v2::ReadRequest* request, grpc::ServerWriter<acyclic::stream::v2::ReadResponse>* writer) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  for (int i=0; i<2; ++i) { acyclic::stream::v2::ReadResponse response; sample(response, 2+i); if (!writer->Write(response)) return grpc::Status(grpc::StatusCode::CANCELLED, "stream write failed"); }
  return grpc::Status::OK;
 }
 grpc::Status Follow(grpc::ServerContext*, const acyclic::stream::v2::FollowRequest* request, grpc::ServerWriter<acyclic::stream::v2::ReadResponse>* writer) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  for (int i=0; i<2; ++i) { acyclic::stream::v2::ReadResponse response; sample(response, 2+i); if (!writer->Write(response)) return grpc::Status(grpc::StatusCode::CANCELLED, "stream write failed"); }
  return grpc::Status::OK;
 }
 grpc::Status Children(grpc::ServerContext*, const acyclic::stream::v2::ChildrenRequest* request, grpc::ServerWriter<acyclic::stream::v2::ChildrenResponse>* writer) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  for (int i=0; i<2; ++i) { acyclic::stream::v2::ChildrenResponse response; sample(response, 2+i); if (!writer->Write(response)) return grpc::Status(grpc::StatusCode::CANCELLED, "stream write failed"); }
  return grpc::Status::OK;
 }
 grpc::Status ChildrenPage(grpc::ServerContext*, const acyclic::stream::v2::ChildrenPageRequest* request, acyclic::stream::v2::ChildrenPageResponse* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
 grpc::Status Commit(grpc::ServerContext*, const acyclic::stream::v2::CommitRequest* request, acyclic::stream::v2::CommitResponse* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
 grpc::Status ReadCommit(grpc::ServerContext*, const acyclic::stream::v2::ReadCommitRequest* request, acyclic::stream::v2::CommittedEnvelope* response) override {
  if (!request_matches(*request)) return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "populated request differs");
  sample(*response, 2);
  return grpc::Status::OK;
 }
};
int main(int argc, char** argv) { try {
 check(descriptor_controls(argc, argv) == 0, "Rust descriptor/message controls failed");
 grpc::ServerBuilder builder; int port = 0;
 builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
 ActorsServiceProbe ActorsService_server; builder.RegisterService(&ActorsService_server);
 WorkersServiceProbe WorkersService_server; builder.RegisterService(&WorkersService_server);
 StreamServiceProbe StreamService_server; builder.RegisterService(&StreamService_server);
 std::vector<std::unique_ptr<grpc::experimental::ServerInterceptorFactoryInterface>> interceptors;
 interceptors.emplace_back(new AuditFactory()); builder.experimental().SetInterceptorCreators(std::move(interceptors));
 auto server = builder.BuildAndStart(); check(server != nullptr && port > 0, "loopback server failed");
 auto channel = grpc::CreateChannel("127.0.0.1:" + std::to_string(port), grpc::InsecureChannelCredentials());
 check(channel->WaitForConnected(std::chrono::system_clock::now() + std::chrono::seconds(15)), "loopback connection timed out");
 auto ActorsService_client = acyclic::actors::v1::ActorsService::NewStub(channel);
 auto WorkersService_client = acyclic::workers::v1::WorkersService::NewStub(channel);
 auto StreamService_client = acyclic::stream::v2::StreamService::NewStub(channel);
 int calls = 0;
 {
  acyclic::actors::v1::CreateActorRequest request; sample(request, 1);
  acyclic::actors::v1::CreateActorResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::actors::v1::CreateActorResponse response;
  auto status = ActorsService_client->CreateActor(&context, request, &response);
  check(status.ok(), "/acyclic.actors.v1.ActorsService/CreateActor failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.actors.v1.ActorsService/CreateActor response payload differs");
  ++calls;
 }
 {
  acyclic::actors::v1::UpdateActorRequest request; sample(request, 1);
  acyclic::actors::v1::UpdateActorResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::actors::v1::UpdateActorResponse response;
  auto status = ActorsService_client->UpdateActor(&context, request, &response);
  check(status.ok(), "/acyclic.actors.v1.ActorsService/UpdateActor failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.actors.v1.ActorsService/UpdateActor response payload differs");
  ++calls;
 }
 {
  acyclic::actors::v1::InspectActorRequest request; sample(request, 1);
  acyclic::actors::v1::InspectActorResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::actors::v1::InspectActorResponse response;
  auto status = ActorsService_client->InspectActor(&context, request, &response);
  check(status.ok(), "/acyclic.actors.v1.ActorsService/InspectActor failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.actors.v1.ActorsService/InspectActor response payload differs");
  ++calls;
 }
 {
  acyclic::actors::v1::AddSubscriptionRequest request; sample(request, 1);
  acyclic::actors::v1::AddSubscriptionResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::actors::v1::AddSubscriptionResponse response;
  auto status = ActorsService_client->AddSubscription(&context, request, &response);
  check(status.ok(), "/acyclic.actors.v1.ActorsService/AddSubscription failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.actors.v1.ActorsService/AddSubscription response payload differs");
  ++calls;
 }
 {
  acyclic::actors::v1::RemoveSubscriptionRequest request; sample(request, 1);
  acyclic::actors::v1::RemoveSubscriptionResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::actors::v1::RemoveSubscriptionResponse response;
  auto status = ActorsService_client->RemoveSubscription(&context, request, &response);
  check(status.ok(), "/acyclic.actors.v1.ActorsService/RemoveSubscription failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.actors.v1.ActorsService/RemoveSubscription response payload differs");
  ++calls;
 }
 {
  acyclic::actors::v1::ResumeSubscriptionRequest request; sample(request, 1);
  acyclic::actors::v1::ResumeSubscriptionResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::actors::v1::ResumeSubscriptionResponse response;
  auto status = ActorsService_client->ResumeSubscription(&context, request, &response);
  check(status.ok(), "/acyclic.actors.v1.ActorsService/ResumeSubscription failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.actors.v1.ActorsService/ResumeSubscription response payload differs");
  ++calls;
 }
 {
  acyclic::actors::v1::CheckpointActorRequest request; sample(request, 1);
  acyclic::actors::v1::CheckpointActorResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::actors::v1::CheckpointActorResponse response;
  auto status = ActorsService_client->CheckpointActor(&context, request, &response);
  check(status.ok(), "/acyclic.actors.v1.ActorsService/CheckpointActor failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.actors.v1.ActorsService/CheckpointActor response payload differs");
  ++calls;
 }
 {
  acyclic::actors::v1::InvokeActorRequest request; sample(request, 1);
  acyclic::actors::v1::InvokeActorResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::actors::v1::InvokeActorResponse response;
  auto status = ActorsService_client->InvokeActor(&context, request, &response);
  check(status.ok(), "/acyclic.actors.v1.ActorsService/InvokeActor failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.actors.v1.ActorsService/InvokeActor response payload differs");
  ++calls;
 }
 {
  acyclic::workers::v1::PublishVersionRequest request; sample(request, 1);
  acyclic::workers::v1::PublishVersionResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::workers::v1::PublishVersionResponse response;
  auto status = WorkersService_client->PublishVersion(&context, request, &response);
  check(status.ok(), "/acyclic.workers.v1.WorkersService/PublishVersion failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.workers.v1.WorkersService/PublishVersion response payload differs");
  ++calls;
 }
 {
  acyclic::workers::v1::SelectDeploymentRequest request; sample(request, 1);
  acyclic::workers::v1::SelectDeploymentResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::workers::v1::SelectDeploymentResponse response;
  auto status = WorkersService_client->SelectDeployment(&context, request, &response);
  check(status.ok(), "/acyclic.workers.v1.WorkersService/SelectDeployment failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.workers.v1.WorkersService/SelectDeployment response payload differs");
  ++calls;
 }
 {
  acyclic::workers::v1::SubmitJobRequest request; sample(request, 1);
  acyclic::workers::v1::SubmitJobResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::workers::v1::SubmitJobResponse response;
  auto status = WorkersService_client->SubmitJob(&context, request, &response);
  check(status.ok(), "/acyclic.workers.v1.WorkersService/SubmitJob failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.workers.v1.WorkersService/SubmitJob response payload differs");
  ++calls;
 }
 {
  acyclic::workers::v1::InspectJobRequest request; sample(request, 1);
  acyclic::workers::v1::InspectJobResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::workers::v1::InspectJobResponse response;
  auto status = WorkersService_client->InspectJob(&context, request, &response);
  check(status.ok(), "/acyclic.workers.v1.WorkersService/InspectJob failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.workers.v1.WorkersService/InspectJob response payload differs");
  ++calls;
 }
 {
  acyclic::workers::v1::CancelJobRequest request; sample(request, 1);
  acyclic::workers::v1::CancelJobResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::workers::v1::CancelJobResponse response;
  auto status = WorkersService_client->CancelJob(&context, request, &response);
  check(status.ok(), "/acyclic.workers.v1.WorkersService/CancelJob failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.workers.v1.WorkersService/CancelJob response payload differs");
  ++calls;
 }
 {
  acyclic::workers::v1::InvokeVersionRequest request; sample(request, 1);
  acyclic::workers::v1::InvokeResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::workers::v1::InvokeResponse response;
  auto status = WorkersService_client->InvokeVersion(&context, request, &response);
  check(status.ok(), "/acyclic.workers.v1.WorkersService/InvokeVersion failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.workers.v1.WorkersService/InvokeVersion response payload differs");
  ++calls;
 }
 {
  acyclic::workers::v1::InvokeDeploymentRequest request; sample(request, 1);
  acyclic::workers::v1::InvokeResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::workers::v1::InvokeResponse response;
  auto status = WorkersService_client->InvokeDeployment(&context, request, &response);
  check(status.ok(), "/acyclic.workers.v1.WorkersService/InvokeDeployment failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.workers.v1.WorkersService/InvokeDeployment response payload differs");
  ++calls;
 }
 {
  acyclic::stream::v2::InspectIdempotencyRequest request; sample(request, 1);
  acyclic::stream::v2::InspectIdempotencyResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::stream::v2::InspectIdempotencyResponse response;
  auto status = StreamService_client->InspectIdempotency(&context, request, &response);
  check(status.ok(), "/acyclic.stream.v2.StreamService/InspectIdempotency failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.stream.v2.StreamService/InspectIdempotency response payload differs");
  ++calls;
 }
 {
  acyclic::stream::v2::AppendRequest request; sample(request, 1);
  acyclic::stream::v2::AppendResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::stream::v2::AppendResponse response;
  auto status = StreamService_client->Append(&context, request, &response);
  check(status.ok(), "/acyclic.stream.v2.StreamService/Append failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.stream.v2.StreamService/Append response payload differs");
  ++calls;
 }
 {
  acyclic::stream::v2::TailRequest request; sample(request, 1);
  acyclic::stream::v2::TailResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::stream::v2::TailResponse response;
  auto status = StreamService_client->Tail(&context, request, &response);
  check(status.ok(), "/acyclic.stream.v2.StreamService/Tail failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.stream.v2.StreamService/Tail response payload differs");
  ++calls;
 }
 {
  acyclic::stream::v2::ForkRequest request; sample(request, 1);
  acyclic::stream::v2::ForkReceipt expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::stream::v2::ForkReceipt response;
  auto status = StreamService_client->Fork(&context, request, &response);
  check(status.ok(), "/acyclic.stream.v2.StreamService/Fork failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.stream.v2.StreamService/Fork response payload differs");
  ++calls;
 }
 {
  acyclic::stream::v2::ReadRequest request; sample(request, 1);
  acyclic::stream::v2::ReadResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  auto reader = StreamService_client->Read(&context, request);
  acyclic::stream::v2::ReadResponse response; int count=0;
  while (reader->Read(&response)) { acyclic::stream::v2::ReadResponse frame; sample(frame, 2+count); check(google::protobuf::util::MessageDifferencer::Equals(frame, response), "/acyclic.stream.v2.StreamService/Read streamed payload/order differs"); ++count; }
  auto status = reader->Finish(); check(status.ok() && count==2, "/acyclic.stream.v2.StreamService/Read streaming status/count differs: " + status.error_message());
  ++calls;
 }
 {
  acyclic::stream::v2::FollowRequest request; sample(request, 1);
  acyclic::stream::v2::ReadResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  auto reader = StreamService_client->Follow(&context, request);
  acyclic::stream::v2::ReadResponse response; int count=0;
  while (reader->Read(&response)) { acyclic::stream::v2::ReadResponse frame; sample(frame, 2+count); check(google::protobuf::util::MessageDifferencer::Equals(frame, response), "/acyclic.stream.v2.StreamService/Follow streamed payload/order differs"); ++count; }
  auto status = reader->Finish(); check(status.ok() && count==2, "/acyclic.stream.v2.StreamService/Follow streaming status/count differs: " + status.error_message());
  ++calls;
 }
 {
  acyclic::stream::v2::ChildrenRequest request; sample(request, 1);
  acyclic::stream::v2::ChildrenResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  auto reader = StreamService_client->Children(&context, request);
  acyclic::stream::v2::ChildrenResponse response; int count=0;
  while (reader->Read(&response)) { acyclic::stream::v2::ChildrenResponse frame; sample(frame, 2+count); check(google::protobuf::util::MessageDifferencer::Equals(frame, response), "/acyclic.stream.v2.StreamService/Children streamed payload/order differs"); ++count; }
  auto status = reader->Finish(); check(status.ok() && count==2, "/acyclic.stream.v2.StreamService/Children streaming status/count differs: " + status.error_message());
  ++calls;
 }
 {
  acyclic::stream::v2::ChildrenPageRequest request; sample(request, 1);
  acyclic::stream::v2::ChildrenPageResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::stream::v2::ChildrenPageResponse response;
  auto status = StreamService_client->ChildrenPage(&context, request, &response);
  check(status.ok(), "/acyclic.stream.v2.StreamService/ChildrenPage failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.stream.v2.StreamService/ChildrenPage response payload differs");
  ++calls;
 }
 {
  acyclic::stream::v2::CommitRequest request; sample(request, 1);
  acyclic::stream::v2::CommitResponse expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::stream::v2::CommitResponse response;
  auto status = StreamService_client->Commit(&context, request, &response);
  check(status.ok(), "/acyclic.stream.v2.StreamService/Commit failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.stream.v2.StreamService/Commit response payload differs");
  ++calls;
 }
 {
  acyclic::stream::v2::ReadCommitRequest request; sample(request, 1);
  acyclic::stream::v2::CommittedEnvelope expected; sample(expected, 2);
  grpc::ClientContext context; context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(15));
  acyclic::stream::v2::CommittedEnvelope response;
  auto status = StreamService_client->ReadCommit(&context, request, &response);
  check(status.ok(), "/acyclic.stream.v2.StreamService/ReadCommit failed: " + status.error_message());
  check(google::protobuf::util::MessageDifferencer::Equals(expected, response), "/acyclic.stream.v2.StreamService/ReadCommit response payload differs");
  ++calls;
 }
 server->Shutdown(); server->Wait();
 check(calls == 25, "incomplete client call inventory");
 std::lock_guard<std::mutex> lock(audit_mutex);
 check(observed.size()==25, "actual server method path inventory differs");
 check(observed.count("/acyclic.actors.v1.ActorsService/CreateActor")==1 && observed.at("/acyclic.actors.v1.ActorsService/CreateActor")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.actors.v1.ActorsService/CreateActor");
 check(observed.count("/acyclic.actors.v1.ActorsService/UpdateActor")==1 && observed.at("/acyclic.actors.v1.ActorsService/UpdateActor")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.actors.v1.ActorsService/UpdateActor");
 check(observed.count("/acyclic.actors.v1.ActorsService/InspectActor")==1 && observed.at("/acyclic.actors.v1.ActorsService/InspectActor")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.actors.v1.ActorsService/InspectActor");
 check(observed.count("/acyclic.actors.v1.ActorsService/AddSubscription")==1 && observed.at("/acyclic.actors.v1.ActorsService/AddSubscription")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.actors.v1.ActorsService/AddSubscription");
 check(observed.count("/acyclic.actors.v1.ActorsService/RemoveSubscription")==1 && observed.at("/acyclic.actors.v1.ActorsService/RemoveSubscription")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.actors.v1.ActorsService/RemoveSubscription");
 check(observed.count("/acyclic.actors.v1.ActorsService/ResumeSubscription")==1 && observed.at("/acyclic.actors.v1.ActorsService/ResumeSubscription")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.actors.v1.ActorsService/ResumeSubscription");
 check(observed.count("/acyclic.actors.v1.ActorsService/CheckpointActor")==1 && observed.at("/acyclic.actors.v1.ActorsService/CheckpointActor")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.actors.v1.ActorsService/CheckpointActor");
 check(observed.count("/acyclic.actors.v1.ActorsService/InvokeActor")==1 && observed.at("/acyclic.actors.v1.ActorsService/InvokeActor")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.actors.v1.ActorsService/InvokeActor");
 check(observed.count("/acyclic.workers.v1.WorkersService/PublishVersion")==1 && observed.at("/acyclic.workers.v1.WorkersService/PublishVersion")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.workers.v1.WorkersService/PublishVersion");
 check(observed.count("/acyclic.workers.v1.WorkersService/SelectDeployment")==1 && observed.at("/acyclic.workers.v1.WorkersService/SelectDeployment")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.workers.v1.WorkersService/SelectDeployment");
 check(observed.count("/acyclic.workers.v1.WorkersService/SubmitJob")==1 && observed.at("/acyclic.workers.v1.WorkersService/SubmitJob")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.workers.v1.WorkersService/SubmitJob");
 check(observed.count("/acyclic.workers.v1.WorkersService/InspectJob")==1 && observed.at("/acyclic.workers.v1.WorkersService/InspectJob")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.workers.v1.WorkersService/InspectJob");
 check(observed.count("/acyclic.workers.v1.WorkersService/CancelJob")==1 && observed.at("/acyclic.workers.v1.WorkersService/CancelJob")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.workers.v1.WorkersService/CancelJob");
 check(observed.count("/acyclic.workers.v1.WorkersService/InvokeVersion")==1 && observed.at("/acyclic.workers.v1.WorkersService/InvokeVersion")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.workers.v1.WorkersService/InvokeVersion");
 check(observed.count("/acyclic.workers.v1.WorkersService/InvokeDeployment")==1 && observed.at("/acyclic.workers.v1.WorkersService/InvokeDeployment")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.workers.v1.WorkersService/InvokeDeployment");
 check(observed.count("/acyclic.stream.v2.StreamService/InspectIdempotency")==1 && observed.at("/acyclic.stream.v2.StreamService/InspectIdempotency")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.stream.v2.StreamService/InspectIdempotency");
 check(observed.count("/acyclic.stream.v2.StreamService/Append")==1 && observed.at("/acyclic.stream.v2.StreamService/Append")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.stream.v2.StreamService/Append");
 check(observed.count("/acyclic.stream.v2.StreamService/Tail")==1 && observed.at("/acyclic.stream.v2.StreamService/Tail")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.stream.v2.StreamService/Tail");
 check(observed.count("/acyclic.stream.v2.StreamService/Fork")==1 && observed.at("/acyclic.stream.v2.StreamService/Fork")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.stream.v2.StreamService/Fork");
 check(observed.count("/acyclic.stream.v2.StreamService/Read")==1 && observed.at("/acyclic.stream.v2.StreamService/Read")==std::make_pair(RpcType::SERVER_STREAMING, 1), "actual server path/type/count differs: /acyclic.stream.v2.StreamService/Read");
 check(observed.count("/acyclic.stream.v2.StreamService/Follow")==1 && observed.at("/acyclic.stream.v2.StreamService/Follow")==std::make_pair(RpcType::SERVER_STREAMING, 1), "actual server path/type/count differs: /acyclic.stream.v2.StreamService/Follow");
 check(observed.count("/acyclic.stream.v2.StreamService/Children")==1 && observed.at("/acyclic.stream.v2.StreamService/Children")==std::make_pair(RpcType::SERVER_STREAMING, 1), "actual server path/type/count differs: /acyclic.stream.v2.StreamService/Children");
 check(observed.count("/acyclic.stream.v2.StreamService/ChildrenPage")==1 && observed.at("/acyclic.stream.v2.StreamService/ChildrenPage")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.stream.v2.StreamService/ChildrenPage");
 check(observed.count("/acyclic.stream.v2.StreamService/Commit")==1 && observed.at("/acyclic.stream.v2.StreamService/Commit")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.stream.v2.StreamService/Commit");
 check(observed.count("/acyclic.stream.v2.StreamService/ReadCommit")==1 && observed.at("/acyclic.stream.v2.StreamService/ReadCommit")==std::make_pair(RpcType::UNARY, 1), "actual server path/type/count differs: /acyclic.stream.v2.StreamService/ReadCommit");
 std::cout << "PASS C++ 25 generated client/server loopback calls, Rust file descriptors, populated payloads and actual method paths/stream types\n";
 return 0;
 } catch (const std::exception& error) { std::cerr << error.what() << "\n"; return 1; }
}
