-module(compare_descriptor).
-export([run/1]).
-include("gpb_descriptor.hrl").
-include("gpb.hrl").

run(Paths) ->
    Results=[begin
      {ok,Bin}=file:read_file(Path),
      #'FileDescriptorSet'{file=ExpectedFiles}=gpb_descriptor:decode_msg(Bin,'FileDescriptorSet'),
      #'FileDescriptorSet'{file=ActualFiles}=gpb_descriptor:decode_msg(Pb:descriptor(),'FileDescriptorSet'),
      Package=atom_to_list(Pb:get_package_name()),
      [Expected]=[F || F<-ExpectedFiles,F#'FileDescriptorProto'.package=:=Package],
      [Actual]=[F || F<-ActualFiles,F#'FileDescriptorProto'.package=:=Package],
      E=normalize(Expected#'FileDescriptorProto'{name=filename:basename(Expected#'FileDescriptorProto'.name),source_code_info=undefined}),
      A=normalize(Actual#'FileDescriptorProto'{name=filename:basename(Actual#'FileDescriptorProto'.name),source_code_info=undefined}),
      adversarial(Actual),
      Fields=record_info(fields,'FileDescriptorProto'),
      Different=[{K,EV,AV} || {K,{EV,AV}}<-lists:zip(Fields,lists:zip(tl(tuple_to_list(E)),tl(tuple_to_list(A)))), EV=/=AV],
      io:format("~p differing modeled file descriptor fields: ~p~n",[Pb,[K || {K,_,_}<-Different]]),
      lists:foreach(fun({K,EV,AV})->lists:foreach(fun({P,X,Y})->io:format("~p ~p expected=~p actual=~p~n",[Pb,P,X,Y]) end,diff(EV,AV,[K])) end,Different),
      Different=:=[]
    end || {Pb,Path}<-lists:zip([actors_pb,workers_pb,stream_pb],Paths)],
    case lists:all(fun(X)->X end,Results) of true -> ok;false -> error(modeled_descriptor_mismatch) end.

%% Normalize declared defaults, default JSON spelling, unordered message/enum
%% declarations and compiler-made proto3-optional oneof labels. Presence-bearing
%% fields (including oneof_index) and real oneof names remain untouched.
normalize(T) when is_tuple(T) ->
    Defs=gpb_descriptor:fetch_msg_def(element(1,T)),
    N=lists:foldl(fun(#field{rnum=Index,opts=Opts},Acc)->
      Value=case element(Index,Acc) of undefined -> proplists:get_value(default,Opts);V -> V end,
      setelement(Index,Acc,normalize(Value))
    end,T,Defs),
    case N of
      F=#'FileDescriptorProto'{} -> F#'FileDescriptorProto'{message_type=sort_named(F#'FileDescriptorProto'.message_type),enum_type=sort_named(F#'FileDescriptorProto'.enum_type)};
      M=#'DescriptorProto'{} -> M#'DescriptorProto'{nested_type=sort_named(M#'DescriptorProto'.nested_type),enum_type=sort_named(M#'DescriptorProto'.enum_type),oneof_decl=synthetic_oneofs(M)};
      F=#'FieldDescriptorProto'{json_name=undefined} -> F#'FieldDescriptorProto'{json_name=json_name(F#'FieldDescriptorProto'.name)};
      _ -> N
    end;
normalize(L) when is_list(L) -> [normalize(V) || V<-L];
normalize(V) -> V.

sort_named(L) -> lists:keysort(2,L).
json_name(Name) -> json_name(Name,false,[]).
json_name([],_,Acc) -> lists:reverse(Acc);
json_name([$_|Rest],_,Acc) -> json_name(Rest,true,Acc);
json_name([C|Rest],Upper,Acc) ->
    N=case Upper andalso C>=$a andalso C=<$z of true -> C-32;false -> C end,
    json_name(Rest,false,[N|Acc]).

diff(E,A,_) when E=:=A -> [];
diff(E,A,P) when is_tuple(E),is_tuple(A),tuple_size(E)=:=tuple_size(A),element(1,E)=:=element(1,A) ->
    lists:append([diff(element(N,E),element(N,A),P++[{element(1,E),N}]) || N<-lists:seq(2,tuple_size(E))]);
diff(E=[EH|_],A=[AH|_],P) when is_tuple(EH),is_tuple(AH),length(E)=:=length(A) ->
    lists:append([diff(X,Y,P++[N]) || {N,{X,Y}}<-lists:zip(lists:seq(1,length(E)),lists:zip(E,A))]);
diff(E,A,P) -> [{P,E,A}].

synthetic_oneofs(M) ->
    [case [F || F<-M#'DescriptorProto'.field,F#'FieldDescriptorProto'.oneof_index=:=I] of
      [#'FieldDescriptorProto'{proto3_optional=true,name=Name}] -> G#'OneofDescriptorProto'{name="$synthetic:"++Name};
      _ -> G
    end || {I,G}<-lists:zip(lists:seq(0,length(M#'DescriptorProto'.oneof_decl)-1),M#'DescriptorProto'.oneof_decl)].

adversarial(File) ->
    [M|Rest]=File#'FileDescriptorProto'.message_type,
    [F|Fields]=M#'DescriptorProto'.field,
    WrongNumber=File#'FileDescriptorProto'{message_type=[M#'DescriptorProto'{field=[F#'FieldDescriptorProto'{number=F#'FieldDescriptorProto'.number+1000}|Fields]}|Rest]},
    true=normalize(File)=/=normalize(WrongNumber),
    [Service|OtherServices]=File#'FileDescriptorProto'.service,
    [Method|Methods]=Service#'ServiceDescriptorProto'.method,
    WrongMethod=File#'FileDescriptorProto'{service=[Service#'ServiceDescriptorProto'{method=[Method#'MethodDescriptorProto'{input_type=".wrong.Message"}|Methods]}|OtherServices]},
    true=normalize(File)=/=normalize(WrongMethod),
    WithReal=[D || D<-File#'FileDescriptorProto'.message_type,lists:any(fun(Field)->Field#'FieldDescriptorProto'.oneof_index=/=undefined andalso Field#'FieldDescriptorProto'.proto3_optional=/=true end,D#'DescriptorProto'.field)],
    lists:foreach(fun(D)->
      [Real|_]=[Field || Field<-D#'DescriptorProto'.field,Field#'FieldDescriptorProto'.oneof_index=/=undefined,Field#'FieldDescriptorProto'.proto3_optional=/=true],
      Index=Real#'FieldDescriptorProto'.oneof_index,
      Groups=[case I=:=Index of true -> G#'OneofDescriptorProto'{name="wrong_real_oneof"};false -> G end || {I,G}<-lists:zip(lists:seq(0,length(D#'DescriptorProto'.oneof_decl)-1),D#'DescriptorProto'.oneof_decl)],
      true=normalize(D)=/=normalize(D#'DescriptorProto'{oneof_decl=Groups})
    end,WithReal),
    io:format("PASS ~s descriptor mutation controls preserve field numbers, RPC input types and real oneof names~n",[File#'FileDescriptorProto'.package]).
