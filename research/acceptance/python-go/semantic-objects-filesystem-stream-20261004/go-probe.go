package main

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "os"
    "strings"
    "time"

    _ "github.com/acyclic-labs/sdk/go/gen/filesystem/v2"
    _ "github.com/acyclic-labs/sdk/go/gen/objects/v2"
    _ "github.com/acyclic-labs/sdk/go/gen/stream/v2"
    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/credentials/insecure"
    "google.golang.org/grpc/status"
    "google.golang.org/protobuf/reflect/protoreflect"
    "google.golang.org/protobuf/reflect/protoregistry"
    "google.golang.org/protobuf/types/dynamicpb"
)

type row struct { Operation, Status string; Details any `json:"details,omitempty"`; Error string `json:"error,omitempty"` }

func descriptor(name string) protoreflect.ServiceDescriptor {
    d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(name)); if err != nil { panic(err) }
    return d.(protoreflect.ServiceDescriptor)
}
func msg(md protoreflect.MessageDescriptor) *dynamicpb.Message { return dynamicpb.NewMessage(md) }
func setString(m protoreflect.Message, name, value string) { if f:=m.Descriptor().Fields().ByName(protoreflect.Name(name)); f!=nil { m.Set(f, protoreflect.ValueOfString(value)) } }
func setBytes(m protoreflect.Message, name string, value []byte) { if f:=m.Descriptor().Fields().ByName(protoreflect.Name(name)); f!=nil { m.Set(f, protoreflect.ValueOfBytes(value)) } }
func setBool(m protoreflect.Message, name string, value bool) { if f:=m.Descriptor().Fields().ByName(protoreflect.Name(name)); f!=nil { m.Set(f, protoreflect.ValueOfBool(value)) } }
func nested(m protoreflect.Message, name string) protoreflect.Message { return m.Mutable(m.Descriptor().Fields().ByName(protoreflect.Name(name))).Message() }
func rpcPath(service, method string) string { return "/"+service+"/"+method }

func main() {
    address := os.Getenv("FIXTURE_GRPC_ADDRESS"); if address=="" { address="127.0.0.1:58315" }
    ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second); defer cancel()
    conn, err := grpc.DialContext(ctx,address,grpc.WithTransportCredentials(insecure.NewCredentials()),grpc.WithBlock()); if err!=nil { panic(err) }; defer conn.Close()
    rows := []row{}
    objects := descriptor("acyclic.objects.v2.ObjectsService")
    putMd := objects.Methods().ByName("PutObject"); put, err := conn.NewStream(ctx,&grpc.StreamDesc{ClientStreams:true},rpcPath(string(objects.FullName()),"PutObject")); if err!=nil { panic(err) }
    header := msg(putMd.Input()); h:=nested(header,"header"); setString(nested(h,"bucket"),"name","fixture-bucket"); setString(h,"object_key","hello.txt")
    body:=msg(putMd.Input()); setBytes(body,"body",[]byte("hello")); complete:=msg(putMd.Input()); setBool(complete,"complete",true)
    if err=put.SendMsg(header); err==nil { err=put.SendMsg(body) }; if err==nil { err=put.SendMsg(complete) }; if err==nil { err=put.CloseSend() }; if err==nil { resp:=msg(putMd.Output()); err=put.RecvMsg(resp) }; if err!=nil { panic(err) }
    rows=append(rows,row{Operation:"ObjectsService/PutObject",Status:"passed",Details:map[string]any{"request_frames":3}})

    getMd:=objects.Methods().ByName("GetObject"); get:=msg(getMd.Input()); setString(nested(get,"bucket"),"name","fixture-bucket"); setString(get,"object_key","hello.txt")
    getStream,err:=conn.NewStream(ctx,&grpc.StreamDesc{ServerStreams:true},rpcPath(string(objects.FullName()),"GetObject")); if err!=nil { panic(err) }; if err=getStream.SendMsg(get); err==nil { err=getStream.CloseSend() }; if err!=nil { panic(err) }
    var kinds []string; var etag string; var size uint64; var payload []byte
    for { out:=msg(getMd.Output()); er:=getStream.RecvMsg(out); if er==io.EOF { break }; if er!=nil { panic(er) }; which:=out.WhichOneof(out.Descriptor().Oneofs().ByName("frame")); if which==nil { panic(fmt.Sprintf("GetObject response had no frame: %v",out)) }; kinds=append(kinds,string(which.Name())); if string(which.Name())=="header" { hm:=out.Get(which).Message(); om:=hm.Get(hm.Descriptor().Fields().ByName("object")).Message(); etag=om.Get(om.Descriptor().Fields().ByName("etag")).String(); size=om.Get(om.Descriptor().Fields().ByName("size")).Uint() } else { payload=append(payload,out.Get(which).Bytes()...) } }
    if strings.Join(kinds,",")!="header,body" || string(payload)!="rust-owned-object-payload" || etag!="fixture-object-etag" || size!=uint64(len(payload)) { panic(fmt.Sprintf("unexpected GetObject: kinds=%v etag=%s size=%d payload=%q",kinds,etag,size,payload)) }
    rows=append(rows,row{Operation:"ObjectsService/GetObject",Status:"passed",Details:map[string]any{"frame_kinds":kinds,"etag":etag,"size":size,"body_utf8":string(payload)}})

    fs:=descriptor("acyclic.filesystem.v2.FilesystemService"); exportMd:=fs.Methods().ByName("Export"); export:=msg(exportMd.Input()); gr:=nested(export,"generation"); setBytes(gr,"generation_id",[]byte("fixture-generation")); ex,err:=conn.NewStream(ctx,&grpc.StreamDesc{ServerStreams:true},rpcPath(string(fs.FullName()),"Export")); if err!=nil { panic(err) }; if err=ex.SendMsg(export); err==nil { err=ex.CloseSend() }; if err!=nil { panic(err) }; out:=msg(exportMd.Output()); if err=ex.RecvMsg(out); err!=nil { panic(err) }; chunk:=out; cursor:=chunk.Get(chunk.Descriptor().Fields().ByName("cursor")).Bytes(); objectID:=chunk.Get(chunk.Descriptor().Fields().ByName("object_id")).Bytes(); contents:=chunk.Get(chunk.Descriptor().Fields().ByName("contents")).Bytes(); terminal:=chunk.Get(chunk.Descriptor().Fields().ByName("terminal")).Bool(); if string(cursor)!="fixture-export-cursor-1" || string(objectID)!="fixture-export-object-1" || string(contents)!="rust-owned-filesystem-export" || !terminal { panic("unexpected Export chunk") }; if er:=ex.RecvMsg(msg(exportMd.Output())); er!=io.EOF { panic(fmt.Sprintf("expected Export EOF, got %v",er)) }; rows=append(rows,row{Operation:"FilesystemService/Export",Status:"passed",Details:map[string]any{"cursor":string(cursor),"object_id":string(objectID),"contents_utf8":string(contents),"terminal":terminal}})

    streamSvc:=descriptor("acyclic.stream.v2.StreamService"); followMd:=streamSvc.Methods().ByName("Follow"); followCtx,followCancel:=context.WithCancel(ctx); defer followCancel(); fr:=msg(followMd.Input()); setString(fr,"path","fixture/events"); follow,err:=conn.NewStream(followCtx,&grpc.StreamDesc{ServerStreams:true},rpcPath(string(streamSvc.FullName()),"Follow")); if err!=nil { panic(err) }; if err=follow.SendMsg(fr); err==nil { err=follow.CloseSend() }; if err!=nil { panic(err) }; followCancel(); recvErr:=follow.RecvMsg(msg(followMd.Output())); cancelled:=status.Code(recvErr)==codes.Canceled; if !cancelled { panic(fmt.Sprintf("expected cancellation, got %v",recvErr)) }; rows=append(rows,row{Operation:"StreamService/Follow",Status:"passed",Details:map[string]any{"event":"cancellation","grpc_code":status.Code(recvErr).String()}})
    appendMd:=streamSvc.Methods().ByName("Append"); ar:=msg(appendMd.Input()); setString(ar,"path","fixture/events"); setBytes(ar,"idempotency_key",[]byte("semantic-recovery")); records:=ar.Descriptor().Fields().ByName("records"); ar.Mutable(records).List().Append(protoreflect.ValueOfBytes([]byte("recovered"))); response:=msg(appendMd.Output()); if err=conn.Invoke(ctx,rpcPath(string(streamSvc.FullName()),"Append"),ar,response); err!=nil { panic(err) }; rows=append(rows,row{Operation:"StreamService/Append",Status:"passed",Details:map[string]any{"event":"recovery","response_descriptor":string(response.ProtoReflect().Descriptor().FullName())}})
    doc:=map[string]any{"schema":"acyclic.sdk.go.semantic-objects-filesystem-stream.v1","consumer":"go","execution_mode":"remote","transport":"grpc","fixture_address":address,"fixture_binary_sha256":os.Getenv("FIXTURE_BINARY_SHA256"),"fixture_source_sha256":os.Getenv("FIXTURE_SOURCE_SHA256"),"model_source_sha256":os.Getenv("MODEL_SOURCE_SHA256"),"package_sha256":os.Getenv("PACKAGE_SHA256"),"rows":rows,"passed":len(rows),"failed":0}; enc,_:=json.MarshalIndent(doc,"","  "); path:=os.Getenv("SEMANTIC_OUTPUT"); if path=="" { path="semantic-go.json" }; if err=os.WriteFile(path,append(enc,'\n'),0644); err!=nil { panic(err) }; fmt.Printf("{\"consumer\":\"go\",\"passed\":%d,\"failed\":0}\n",len(rows))
}
