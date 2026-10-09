#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/util/message_differencer.h>
#include "actors/v1/actors.pb.h"
#include "workers/v1/workers.pb.h"
#include "stream/v2/stream.pb.h"
using namespace google::protobuf;
void check(bool ok, const std::string& message) { if (!ok) throw std::runtime_error(message); }
void populate(Message& message, int depth=0) {
 auto descriptor=message.GetDescriptor();auto reflection=message.GetReflection();
 for(int i=0;i<descriptor->field_count();++i) {
  auto field=descriptor->field(i);bool repeated=field->is_repeated();
  switch(field->cpp_type()) {
   case FieldDescriptor::CPPTYPE_INT32: if(repeated)reflection->AddInt32(&message,field,123);else reflection->SetInt32(&message,field,123);break;
   case FieldDescriptor::CPPTYPE_INT64: if(repeated)reflection->AddInt64(&message,field,123);else reflection->SetInt64(&message,field,123);break;
   case FieldDescriptor::CPPTYPE_UINT32: if(repeated)reflection->AddUInt32(&message,field,123);else reflection->SetUInt32(&message,field,123);break;
   case FieldDescriptor::CPPTYPE_UINT64: if(repeated)reflection->AddUInt64(&message,field,123);else reflection->SetUInt64(&message,field,123);break;
   case FieldDescriptor::CPPTYPE_DOUBLE: if(repeated)reflection->AddDouble(&message,field,1.25);else reflection->SetDouble(&message,field,1.25);break;
   case FieldDescriptor::CPPTYPE_FLOAT: if(repeated)reflection->AddFloat(&message,field,1.25);else reflection->SetFloat(&message,field,1.25);break;
   case FieldDescriptor::CPPTYPE_BOOL: if(repeated)reflection->AddBool(&message,field,true);else reflection->SetBool(&message,field,true);break;
   case FieldDescriptor::CPPTYPE_ENUM: {auto value=field->enum_type()->value(field->enum_type()->value_count()-1);if(repeated)reflection->AddEnum(&message,field,value);else reflection->SetEnum(&message,field,value);break;}
   case FieldDescriptor::CPPTYPE_STRING: {auto value=field->type()==FieldDescriptor::TYPE_BYTES?std::string("\0\xff",2):std::string("probe");if(repeated)reflection->AddString(&message,field,value);else reflection->SetString(&message,field,value);break;}
   case FieldDescriptor::CPPTYPE_MESSAGE: if(depth<2)populate(*(repeated?reflection->AddMessage(&message,field):reflection->MutableMessage(&message,field)),depth+1);break;
  }
 }
}
void roundtrip(const Message& message) {
 std::unique_ptr<Message> restored(message.New());std::string wire;
 check(message.SerializeToString(&wire)&&restored->ParseFromString(wire),"wire roundtrip failed");
 check(util::MessageDifferencer::Equals(message,*restored),"message contents differ");
}
int main(int argc,char** argv) { try {
 check(argc==4,"three descriptor paths required");
 const char* names[]={"actors/v1/actors.proto","workers/v1/workers.proto","stream/v2/stream.proto"};int methods=0;
 for(int n=0;n<3;++n) {
  std::ifstream input(argv[n+1],std::ios::binary);FileDescriptorSet rust;check(rust.ParseFromIstream(&input),"cannot parse Rust descriptor set");
  FileDescriptorProto expected,actual;bool found=false;
  for(const auto& file:rust.file())if(file.name()==names[n]){expected=file;found=true;}
  check(found,"Rust descriptor absent");expected.clear_source_code_info();expected.mutable_unknown_fields()->DeleteByNumber(8042);
  auto file=DescriptorPool::generated_pool()->FindFileByName(names[n]);check(file!=nullptr,"generated file descriptor absent");file->CopyTo(&actual);actual.clear_source_code_info();actual.mutable_unknown_fields()->DeleteByNumber(8042);
  std::string differences;util::MessageDifferencer comparison;comparison.ReportDifferencesToString(&differences);
  check(comparison.Compare(expected,actual),std::string("file descriptor differs: ")+names[n]+"\n"+differences);
  for(int s=0;s<file->service_count();++s)for(int m=0;m<file->service(s)->method_count();++m) {
   auto method=file->service(s)->method(m);++methods;
   for(auto type:{method->input_type(),method->output_type()}) {
    auto prototype=MessageFactory::generated_factory()->GetPrototype(type);check(prototype!=nullptr,"generated RPC message absent");
    std::unique_ptr<Message> message(prototype->New());populate(*message);roundtrip(*message);
   }
  }
 }
 const std::string bytes("\0\xff",2);
 acyclic::actors::v1::CreateActorRequest actor;actor.set_code_sha256(bytes);check(actor.code_sha256()==bytes,"Actor bytes changed");roundtrip(actor);
 acyclic::workers::v1::PublishVersionRequest worker;worker.set_javascript_module(bytes);check(worker.javascript_module()==bytes,"Worker bytes changed");roundtrip(worker);
 acyclic::stream::v2::AppendRequest append;append.set_path("probe");append.add_records(bytes);append.add_records("");append.set_if_tail(std::numeric_limits<uint64_t>::max());append.set_idempotency_key(bytes);check(append.has_if_tail()&&append.if_tail()==std::numeric_limits<uint64_t>::max()&&append.records(0)==bytes&&append.records(1).empty(),"Append bounds or bytes changed");roundtrip(append);
 acyclic::stream::v2::AppendRequest zero;zero.set_if_tail(0);check(zero.has_if_tail()&&zero.SerializeAsString()==std::string("\x18\0",2),"optional zero presence differs");roundtrip(zero);zero.clear_if_tail();check(!zero.has_if_tail(),"optional clear differs");
 acyclic::stream::v2::ReadRequest read;read.set_from(std::numeric_limits<uint64_t>::max());read.set_limit(std::numeric_limits<uint32_t>::max());check(read.SerializeAsString()==std::string("\x10\xff\xff\xff\xff\xff\xff\xff\xff\xff\x01\x18\xff\xff\xff\xff\x0f",17),"unsigned maximum wire bits differ");roundtrip(read);
 acyclic::stream::v2::AppendResponse response;response.mutable_committed()->set_tail(123);roundtrip(response);response.mutable_conflict()->set_actual_tail(456);check(!response.has_committed()&&response.has_conflict(),"oneof switch differs");roundtrip(response);response.clear_outcome();check(response.outcome_case()==acyclic::stream::v2::AppendResponse::OUTCOME_NOT_SET,"oneof clear differs");
 std::cout<<"PASS C++ complete file descriptors, "<<methods<<" RPC message pairs, bytes, unsigned bounds, optional zero and oneof controls\n";return 0;
 } catch(const std::exception& error){std::cerr<<error.what()<<"\n";return 1;}
}
