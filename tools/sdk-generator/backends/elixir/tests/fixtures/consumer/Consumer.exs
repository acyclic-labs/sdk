defmodule InstalledProbe do
  def check(true, _message), do: :ok
  def check(false, message), do: raise(message)
  def mod(name), do: Module.concat(Enum.map(String.split(String.trim_leading(name, "."), "."), &Macro.camelize/1))
  def normalized(%module{} = descriptor), do: module.decode(module.encode(descriptor))
  def flatten(messages, prefix) do
    Enum.flat_map(messages, fn message ->
      name = prefix <> "." <> message.name
      [{name, message} | flatten(message.nested_type, name)]
    end)
  end
  def enums(messages, prefix) do
    Enum.flat_map(messages, fn message ->
      name = prefix <> "." <> message.name
      Enum.map(message.enum_type, &{name <> "." <> &1.name, &1}) ++ enums(message.nested_type, name)
    end)
  end
  def sample(name, messages, enum_types, seed, depth \\ 0) do
    descriptor = Map.fetch!(messages, String.trim_leading(name, "."))
    module = mod(name)
    Enum.reduce(descriptor.field, {struct(module), MapSet.new()}, fn field, {message, groups} ->
      key = String.to_atom(field.name)
      real_oneof = field.oneof_index != nil and field.proto3_optional != true
      if (real_oneof and MapSet.member?(groups, field.oneof_index)) or (depth >= 4 and field.type == :TYPE_MESSAGE) do
        {message, groups}
      else
        value = case field.type do
          :TYPE_MESSAGE -> sample(field.type_name, messages, enum_types, seed, depth + 1)
          :TYPE_ENUM -> enum_types |> Map.fetch!(String.trim_leading(field.type_name, ".")) |> Map.fetch!(:value) |> Enum.find(fn value -> value.number != 0 end) |> Map.fetch!(:name) |> String.to_atom()
          :TYPE_BYTES -> <<0, 255, seed>>
          :TYPE_STRING -> "probe-#{seed}"
          :TYPE_BOOL -> true
          :TYPE_UINT64 -> 18_446_744_073_709_551_615 - seed
          :TYPE_UINT32 -> 4_294_967_295 - seed
          type when type in [:TYPE_FLOAT, :TYPE_DOUBLE] -> 1.25 + seed
          _ -> 120 + seed
        end
        value = if field.label == :LABEL_REPEATED, do: [value, value], else: value
        if real_oneof do
          group = descriptor.oneof_decl |> Enum.at(field.oneof_index) |> Map.fetch!(:name) |> String.to_atom()
          {Map.put(message, group, {key, value}), MapSet.put(groups, field.oneof_index)}
        else
          {Map.put(message, key, value), groups}
        end
      end
    end) |> elem(0)
  end
  def roundtrip(%module{} = message) do
    wire = module.encode(message)
    check(module.decode(wire) == message, "populated roundtrip differs: #{inspect(module)}")
    wire
  end
  def reject(message, field) do
    error = try do
      Protobuf.encode(message)
      nil
    rescue
      error in Protobuf.EncodeError -> Exception.message(error)
    end
    check(is_binary(error) and String.contains?(error, field), "invalid field accepted or unrelated rejection: #{field}")
  end
  def run(paths) do
    check(length(paths) == 3, "three Rust descriptor sets required")
    expected_names = ["actors/v1/actors.proto", "workers/v1/workers.proto", "stream/v1/stream.proto"]
    files = Enum.zip(paths, expected_names) |> Enum.map(fn {path, name} ->
      set = Google.Protobuf.FileDescriptorSet.decode(File.read!(path))
      file = Enum.find(set.file, &(&1.name == name))
      check(file != nil, "Rust file descriptor absent: #{name}")
      file
    end)
    messages = Map.new(Enum.flat_map(files, &flatten(&1.message_type, &1.package)))
    enum_types = Map.new(Enum.flat_map(files, fn file -> Enum.map(file.enum_type, &{file.package <> "." <> &1.name, &1}) ++ enums(file.message_type, file.package) end))
    for {name, descriptor} <- Map.to_list(messages) ++ Map.to_list(enum_types) do
      module = mod(name)
      check(normalized(module.descriptor()) == normalized(descriptor), "Rust descriptor differs: #{name}")
    end
    methods = for file <- files, service <- file.service, method <- service.method do
      module = Module.concat(mod(file.package <> "." <> service.name), Service)
      check(module.__meta__(:name) == file.package <> "." <> service.name, "service name differs")
      check(normalized(module.descriptor()) == normalized(service), "Rust service descriptor differs")
      actual = Enum.find(module.__rpc_calls__(), &(to_string(elem(&1, 0)) == method.name))
      check(actual != nil, "generated RPC absent: #{method.name}")
      {_, {request, request_stream}, {response, response_stream}, _} = actual
      check(request == mod(method.input_type) and response == mod(method.output_type), "RPC message type differs")
      check(request_stream == (method.client_streaming == true) and response_stream == (method.server_streaming == true), "RPC streaming shape differs")
      for name <- [method.input_type, method.output_type], do: roundtrip(sample(name, messages, enum_types, 1))
      method.name
    end
    check(length(methods) == 25, "incomplete RPC inventory")
    bytes = <<0, 255>>
    roundtrip(%Acyclic.Actors.V1.CreateActorRequest{code_sha256: bytes})
    roundtrip(%Acyclic.Workers.V1.PublishVersionRequest{javascript_module: bytes})
    wire = roundtrip(%Acyclic.Stream.V1.ReadRequest{from: 18_446_744_073_709_551_615, limit: 4_294_967_295})
    check(wire == <<16,255,255,255,255,255,255,255,255,255,1,24,255,255,255,255,15>>, "unsigned maximum wire differs")
    check(roundtrip(%Acyclic.Stream.V1.AppendRequest{if_tail: 0}) == <<24,0>>, "optional zero presence differs")
    check(roundtrip(%Acyclic.Stream.V1.AppendRequest{if_tail: nil}) == <<>>, "optional clear differs")
    roundtrip(%Acyclic.Stream.V1.AppendResponse{outcome: {:committed, %Acyclic.Stream.V1.AppendReceipt{tail: 123}}})
    roundtrip(%Acyclic.Stream.V1.AppendResponse{outcome: {:conflict, %Acyclic.Stream.V1.TailConflict{actual_tail: 456}}})
    roundtrip(%Acyclic.Stream.V1.AppendResponse{outcome: nil})
    reject(%Acyclic.Actors.V1.CreateActorRequest{code_sha256: :invalid_bytes}, "code_sha256")
    reject(%Acyclic.Workers.V1.PublishVersionRequest{javascript_module: :invalid_bytes}, "javascript_module")
    reject(%Acyclic.Stream.V1.AppendRequest{if_tail: "invalid integer"}, "if_tail")
    IO.puts("PASS Elixir Rust message/enum/service descriptors, 25 populated RPC message pairs, bytes, unsigned bounds, optional presence, oneofs and 3 runtime type rejections")
  end
end

InstalledProbe.run(System.argv())
