package main

import (
  "context"
  "crypto/tls"
  "crypto/x509"
  "encoding/json"
  "fmt"
  "math"
  "net/url"
  "os"
  "time"

  actors "github.com/acyclic-labs/sdk/go/gen/actors/v1"
  "google.golang.org/grpc"
  "google.golang.org/grpc/codes"
  "google.golang.org/grpc/credentials"
  "google.golang.org/grpc/metadata"
  "google.golang.org/grpc/status"
  "google.golang.org/protobuf/proto"
)

type fixtureOptions struct { Endpoint string `json:"endpoint"`; Token string `json:"token"`; CaCertificate string `json:"caCertificate"`; ActorID string `json:"actorId"` }
func must[T any](v T, err error) T { if err != nil { panic(err) }; return v }
func main() {
  if len(os.Args) != 2 { panic("usage: consumer options") }
  var o fixtureOptions; f:=must(os.Open(os.Args[1])); defer f.Close(); must(struct{}{}, json.NewDecoder(f).Decode(&o))
  roots:=x509.NewCertPool(); if !roots.AppendCertsFromPEM([]byte(o.CaCertificate)){panic("bad fixture ca")}; u:=must(url.Parse(o.Endpoint)); cfg:=credentials.NewTLS(&tls.Config{RootCAs:roots,ServerName:u.Hostname(),MinVersion:tls.VersionTLS12}); conn:=must(grpc.Dial(u.Host,grpc.WithTransportCredentials(cfg))); defer conn.Close(); c:=actors.NewActorsServiceClient(conn)
  auth:=func(ctx context.Context) context.Context{return metadata.AppendToOutgoingContext(ctx,"authorization","Bearer "+o.Token)}
  code:=bytes32(1); limits:=&actors.ActorLimits{HandlerTimeoutMillis:9,MemoryBytes:1<<63,CheckpointBytes:math.MaxUint64}
  if _,err:=c.CreateActor(auth(context.Background()),&actors.CreateActorRequest{CodeSha256:code,HomeRegion:"eu",Limits:limits,IdempotencyKey:"go-signed-create"});err!=nil{panic(fmt.Errorf("create: %w",err))}
  if _,err:=c.UpdateActor(auth(context.Background()),&actors.UpdateActorRequest{ActorId:o.ActorID,CodeSha256:code,ExpectedConfigurationRevision:0,IdempotencyKey:"go-signed-update"});err!=nil{panic(fmt.Errorf("update: %w",err))}
  if got:=must(c.InspectActor(auth(context.Background()),&actors.InspectActorRequest{ActorId:o.ActorID}));got.GetActor().GetActorId()!=o.ActorID{panic("inspect actor mismatch")}
  big:=uint64(9007199254740993); if _,err:=c.AddSubscription(auth(context.Background()),&actors.AddSubscriptionRequest{ActorId:o.ActorID,Subscription:&actors.SubscriptionSpec{SubscriptionId:"go-signed-sub",StreamPath:"events/input",Start:&actors.SubscriptionStart{Start:&actors.SubscriptionStart_Cursor{Cursor:big}}},IdempotencyKey:"go-signed-add"});err!=nil{panic(fmt.Errorf("add: %w",err))}
  if _,err:=c.RemoveSubscription(auth(context.Background()),&actors.RemoveSubscriptionRequest{ActorId:o.ActorID,SubscriptionId:"go-signed-sub",IdempotencyKey:"go-signed-remove"});err!=nil{panic(fmt.Errorf("remove: %w",err))}
  if _,err:=c.ResumeSubscription(auth(context.Background()),&actors.ResumeSubscriptionRequest{ActorId:o.ActorID,SubscriptionId:"go-signed-sub",IdempotencyKey:"go-signed-resume"});err!=nil{panic(fmt.Errorf("resume: %w",err))}
  if _,err:=c.CheckpointActor(auth(context.Background()),&actors.CheckpointActorRequest{ActorId:o.ActorID,IdempotencyKey:"checkpoint-a"});err!=nil{panic(fmt.Errorf("checkpoint: %w",err))}
  if _,err:=c.InvokeActor(auth(context.Background()),&actors.InvokeActorRequest{ActorId:o.ActorID,Method:"POST",Url:"/result",Body:[]byte{1,2,3},Headers:[]*actors.Header{{Name:"content-type",Value:"application/json"}}});err!=nil{panic(fmt.Errorf("invoke: %w",err))}
  max:=uint64(math.MaxUint64); obs:=&actors.ActorObservation{CodeSha256:[]byte{0,255},CheckpointUnixMillis:&max,CheckpointEpoch:max,ConfigurationRevision:max,State:actors.ActorState(99)}; wire:=must(proto.Marshal(obs)); dec:=&actors.ActorObservation{}; if err:=proto.Unmarshal(wire,dec);err!=nil||dec.GetCheckpointUnixMillis()!=max||dec.GetCheckpointEpoch()!=max||dec.GetConfigurationRevision()!=max||dec.GetState()!=actors.ActorState(99){panic("u64/presence/enum failed")}; one:=&actors.SubscriptionStart{Start:&actors.SubscriptionStart_Cursor{Cursor:max}}; oneWire:=must(proto.Marshal(one)); oneDec:=&actors.SubscriptionStart{}; if err:=proto.Unmarshal(oneWire,oneDec);err!=nil||oneDec.GetStart().(*actors.SubscriptionStart_Cursor).Cursor!=max{panic("oneof failed")}
  bad:=metadata.AppendToOutgoingContext(context.Background(),"authorization","Bearer wrong"); _,err:=c.InspectActor(bad,&actors.InspectActorRequest{ActorId:o.ActorID}); if status.Code(err)!=codes.Unauthenticated{panic(fmt.Sprintf("typed unauth=%v",status.Code(err)))}; cancelled,cancel:=context.WithCancel(context.Background()); cancel(); _,err=c.InspectActor(auth(cancelled),&actors.InspectActorRequest{ActorId:o.ActorID}); if status.Code(err)!=codes.Canceled{panic(fmt.Sprintf("typed cancel=%v",status.Code(err)))}; deadline,stop:=context.WithTimeout(auth(context.Background()),5*time.Second); defer stop(); if _,err=c.InspectActor(deadline,&actors.InspectActorRequest{ActorId:o.ActorID});err!=nil{panic(err)}
  fmt.Println("GO_SIGNED_1269511_ACTORS_ALL8_PASS"); fmt.Println("GO_SIGNED_1269511_U64_PRESENCE_TYPED_ERRORS_PASS")
}
func bytes32(v byte) []byte { b:=make([]byte,32); for i:=range b{b[i]=v}; return b }

