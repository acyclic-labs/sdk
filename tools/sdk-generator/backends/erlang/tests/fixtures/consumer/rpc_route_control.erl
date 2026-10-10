-module(rpc_route_control).
-export([run/0]).
-include("gpb_descriptor.hrl").

snake(Name) ->
    list_to_atom(string:lowercase(re:replace(atom_to_list(Name), "([a-z0-9])([A-Z])", "\\1_\\2", [global,{return,list}]))).

run() ->
    {ok,_}=application:ensure_all_started(grpcbox),
    ets:new(rpc_route_counts,[named_table,public,set]),
    ets:new(rpc_vectors,[named_table,public,set]),
    Services=[{actors_pb,'acyclic.actors.v1.ActorsService',acyclic_actors_v_1_actors_service_client},
              {workers_pb,'acyclic.workers.v1.WorkersService',acyclic_workers_v_1_workers_service_client},
              {stream_pb,'acyclic.stream.v1.StreamService',acyclic_stream_v_1_stream_service_client}],
    Paths=init:get_plain_arguments(),3=length(Paths),
    lists:foreach(fun({{Pb,S,_},Path})->admit_vectors(Pb,S,Path) end,lists:zip(Services,Paths)),
    Specs=[begin
      {{service,S},Methods}=Pb:get_service_def(S),
      Mod=list_to_atom(atom_to_list(Pb)++"_route_fixture"),
      Functions=[begin
        #{name:=N,input_stream:=false,output_stream:=Streaming}=Method,
        F=snake(N),
        case Streaming of
          false -> io_lib:format("~p(Ctx,Input) -> {Expected,Output}=ets:lookup_element(rpc_vectors,{~p,~p},2), Expected=Input, ets:update_counter(rpc_route_counts,{~p,~p},{2,1},{{~p,~p},0}), {ok,Output,Ctx}.~n",[F,S,N,S,N,S,N]);
          true -> io_lib:format("~p(Input,Stream) -> {Expected,Output}=ets:lookup_element(rpc_vectors,{~p,~p},2), Expected=Input, ets:update_counter(rpc_route_counts,{~p,~p},{2,1},{{~p,~p},0}), grpcbox_stream:send(Output,Stream), ok.~n",[F,S,N,S,N,S,N])
        end
      end || Method<-Methods],
      Source=iolist_to_binary([io_lib:format("-module(~p).~n-export([",[Mod]),
        lists:join(",",[io_lib:format("~p/2",[snake(maps:get(name,M))]) || M<-Methods]),
        "]).\n",Functions]),
      File=atom_to_list(Mod)++".erl",ok=file:write_file(File,Source),
      {ok,Mod,Bin}=compile:file(File,[binary,return_errors,warnings_as_errors]),
      {module,Mod}=code:load_binary(Mod,File,Bin),
      {Pb,S,Client,Mod,Methods}
    end || {Pb,S,Client}<-Services],
    Opts=#{listen_opts=>#{port=>0,ip=>{127,0,0,1}},pool_opts=>#{size=>1},
           grpc_opts=>#{service_protos=>[Pb || {Pb,_,_,_,_}<-Specs],services=>maps:from_list([{S,Mod} || {_,S,_,Mod,_}<-Specs])}},
    {ok,Sup}=grpcbox:start_server(Opts),
    {grpcbox_socket,SocketPid,worker,_}=lists:keyfind(grpcbox_socket,1,supervisor:which_children(Sup)),
    {Socket,_}=sys:get_state(SocketPid),
    {ok,{{127,0,0,1},Port}}=inet:sockname(Socket),true=Port>0,
    {ok,_}=grpcbox_channel_sup:start_child(route_control,[{http,"127.0.0.1",Port,[]}],#{sync_start=>true}),
    Counts=[begin
      lists:foreach(fun(#{name:=N,output_stream:=Streaming})->
        {Input,Expected}=ets:lookup_element(rpc_vectors,{S,N},2),
        case Streaming of
          false -> {ok,Expected,_Metadata}=apply(Client,snake(N),[Input,#{channel=>route_control}]);
          true ->
            {ok,Stream}=apply(Client,snake(N),[Input,#{channel=>route_control}]),
            {ok,_Headers}=grpcbox_client:recv_headers(Stream),
            {ok,Expected}=grpcbox_client:recv_data(Stream),
            {ok,{<<"0">>,_,_}}=grpcbox_client:recv_trailers(Stream),
            stream_finished=grpcbox_client:recv_data(Stream)
        end,
        [{{S,N},1}]=ets:lookup(rpc_route_counts,{S,N})
      end,Methods),
      length(Methods)
    end || {_Pb,S,Client,_,Methods}<-Specs],
    26=lists:sum(Counts),26=ets:info(rpc_route_counts,size),
    ok=grpcbox_channel:stop(route_control),
    ok=application:stop(grpcbox),
    scalar_controls(),
    io:format("PASS archive-installed Erlang clients: 26 populated native TCP RPC pairs, independent Rust-descriptor wire vectors, unsigned limits, optional zero, oneofs and three encoding rejections on port ~p~n",[Port]),ok.

admit_vectors(Pb,Service,Path) ->
    {ok,Bytes}=file:read_file(Path),
    #'FileDescriptorSet'{file=Files}=gpb_descriptor:decode_msg(Bytes,'FileDescriptorSet'),
    [File]=[F || F<-Files, F#'FileDescriptorProto'.package=:=atom_to_list(Pb:get_package_name())],
    Prefix=File#'FileDescriptorProto'.package,
    Messages=maps:from_list(flatten_messages(File#'FileDescriptorProto'.message_type,Prefix)),
    Enums=maps:from_list([{Prefix++"."++E#'EnumDescriptorProto'.name,E} || E<-File#'FileDescriptorProto'.enum_type]++flatten_enums(File#'FileDescriptorProto'.message_type,Prefix)),
    [RustService]=[S || S<-File#'FileDescriptorProto'.service, Prefix++"."++S#'ServiceDescriptorProto'.name=:=atom_to_list(Service)],
    {{service,Service},ActualMethods}=Pb:get_service_def(Service),
    true=length(ActualMethods)=:=length(RustService#'ServiceDescriptorProto'.method),
    lists:foreach(fun(M)->
      Name=list_to_atom(M#'MethodDescriptorProto'.name),
      [Actual]=[A || A=#{name:=N}<-ActualMethods,N=:=Name],
      #{input:=I,output:=O,input_stream:=IS,output_stream:=OS}=Actual,
      true=IS=:=(M#'MethodDescriptorProto'.client_streaming=:=true),
      true=OS=:=(M#'MethodDescriptorProto'.server_streaming=:=true),
      true=Pb:msg_name_to_fqbin(I)=:=list_to_binary(tl(M#'MethodDescriptorProto'.input_type)),
      true=Pb:msg_name_to_fqbin(O)=:=list_to_binary(tl(M#'MethodDescriptorProto'.output_type)),
      {Input,InputWire}=sample(Pb,tl(M#'MethodDescriptorProto'.input_type),Messages,Enums,1,0),
      {Output,OutputWire}=sample(Pb,tl(M#'MethodDescriptorProto'.output_type),Messages,Enums,2,0),
      InputWire=Pb:encode_msg(Input,I),OutputWire=Pb:encode_msg(Output,O),
      Input=Pb:decode_msg(InputWire,I),Output=Pb:decode_msg(OutputWire,O),
      ets:insert(rpc_vectors,{{Service,Name},{Input,Output}})
    end,RustService#'ServiceDescriptorProto'.method).

flatten_messages(Values,Prefix) -> lists:append([begin N=Prefix++"."++M#'DescriptorProto'.name,[{N,M}|flatten_messages(M#'DescriptorProto'.nested_type,N)] end || M<-Values]).
flatten_enums(Values,Prefix) -> lists:append([begin N=Prefix++"."++M#'DescriptorProto'.name,[{N++"."++E#'EnumDescriptorProto'.name,E} || E<-M#'DescriptorProto'.enum_type]++flatten_enums(M#'DescriptorProto'.nested_type,N) end || M<-Values]).

sample(Pb,Name,Messages,Enums,Seed,Depth) ->
    D=maps:get(Name,Messages),Internal=Pb:fqbin_to_msg_name(list_to_binary(Name)),
    {Map,_,Wire}=lists:foldl(fun(F,{Acc,Groups,Bytes})->
      #'FieldDescriptorProto'{name=FieldName,number=Number,type=Type,label=Label,oneof_index=Index,proto3_optional=Optional}=F,
      Real=Index=/=undefined andalso Optional=/=true,
      case (Real andalso lists:member(Index,Groups)) orelse (Depth>=4 andalso Type=:='TYPE_MESSAGE') of
        true -> {Acc,Groups,Bytes};
        false ->
          {Value,Tag,Encoded}=value(Pb,F,Messages,Enums,Seed,Depth),
          Key=list_to_atom(FieldName),Full=iolist_to_binary([varint((Number bsl 3) bor Tag),Encoded]),
          {MapValue,FieldWire}=case Label of 'LABEL_REPEATED' -> true=(Tag=:=2),{[Value,Value],[Full,Full]};_ -> {Value,Full} end,
          case Real of
            true -> Group=lists:nth(Index+1,D#'DescriptorProto'.oneof_decl),{maps:put(list_to_atom(Group#'OneofDescriptorProto'.name),{Key,MapValue},Acc),[Index|Groups],[Bytes,FieldWire]};
            false -> {maps:put(Key,MapValue,Acc),Groups,[Bytes,FieldWire]}
          end
      end
    end,{Pb:decode_msg(<<>>,Internal),[],[]},D#'DescriptorProto'.field),
    {Map,iolist_to_binary(Wire)}.

value(Pb,F=#'FieldDescriptorProto'{type='TYPE_MESSAGE'},Messages,Enums,Seed,Depth) ->
    {Map,Wire}=sample(Pb,tl(F#'FieldDescriptorProto'.type_name),Messages,Enums,Seed,Depth+1),{Map,2,length_delimited(Wire)};
value(_,F=#'FieldDescriptorProto'{type='TYPE_ENUM'},_,Enums,_,_) ->
    E=maps:get(tl(F#'FieldDescriptorProto'.type_name),Enums),[V|_]=[V || V<-E#'EnumDescriptorProto'.value,V#'EnumValueDescriptorProto'.number=/=0],
    {list_to_atom(V#'EnumValueDescriptorProto'.name),0,varint(V#'EnumValueDescriptorProto'.number)};
value(_,#'FieldDescriptorProto'{type='TYPE_BYTES'},_,_,Seed,_) -> V = <<0,255,Seed>>,{V,2,length_delimited(V)};
value(_,#'FieldDescriptorProto'{type='TYPE_STRING'},_,_,Seed,_) -> V=list_to_binary("probe-"++integer_to_list(Seed)),{V,2,length_delimited(V)};
value(_,#'FieldDescriptorProto'{type='TYPE_BOOL'},_,_,_,_) -> {true,0,<<1>>};
value(_,#'FieldDescriptorProto'{type='TYPE_UINT64'},_,_,Seed,_) -> V=18446744073709551615-Seed,{V,0,varint(V)};
value(_,#'FieldDescriptorProto'{type='TYPE_UINT32'},_,_,Seed,_) -> V=4294967295-Seed,{V,0,varint(V)};
value(_,#'FieldDescriptorProto'{type=Type},_,_,Seed,_) when Type=:='TYPE_INT32';Type=:='TYPE_INT64' -> V=120+Seed,{V,0,varint(V)}.

length_delimited(Bin) -> [varint(byte_size(Bin)),Bin].
varint(N) when N<128 -> <<N>>;
varint(N) -> [<<((N band 127) bor 128)>>,varint(N bsr 7)].

scalar_controls() ->
    R=stream_pb:decode_msg(<<>>,acyclic_read_request),
    <<16,255,255,255,255,255,255,255,255,255,1,24,255,255,255,255,15>>=stream_pb:encode_msg(R#{from=>18446744073709551615,limit=>4294967295},acyclic_read_request),
    A=stream_pb:decode_msg(<<>>,acyclic_append_request),
    <<24,0>>=stream_pb:encode_msg(A#{if_tail=>0},acyclic_append_request),
    <<>>=stream_pb:encode_msg(maps:remove(if_tail,A),acyclic_append_request),
    lists:foreach(fun({Branch,Type,Field,Number})->
      Child=(stream_pb:decode_msg(<<>>,Type))#{Field=>Number},
      Response=(stream_pb:decode_msg(<<>>,acyclic_append_response))#{outcome=>{Branch,Child}},
      Response=stream_pb:decode_msg(stream_pb:encode_msg(Response,acyclic_append_response),acyclic_append_response)
    end,[{committed,acyclic_append_receipt,tail,123},{conflict,acyclic_tail_conflict,actual_tail,456}]),
    reject(actors_pb,acyclic_create_actor_request,code_sha256),
    reject(workers_pb,acyclic_publish_version_request,javascript_module),
    reject(stream_pb,acyclic_append_request,if_tail).

reject(Pb,Type,Field) ->
    Message=(Pb:decode_msg(<<>>,Type))#{Field=>invalid_value},
    try Pb:encode_msg(Message,Type) of _ -> error({invalid_field_accepted,Field})
    catch error:{gpb_type_error,Details} -> true=string:find(lists:flatten(io_lib:format("~p",[Details])),atom_to_list(Field))=/=nomatch end.
