[sdk, ebin] = System.argv()
sdk = Path.expand(sdk)
ebin = Path.expand(ebin)
expected =
  for file <- Path.wildcard(Path.join(sdk, "lib/**/*.ex")),
      [_, name] <- Regex.scan(~r/^defmodule ([A-Za-z0-9_.]+) do$/m, File.read!(file)),
      into: %{} do
    {"Elixir." <> name, Path.expand(file)}
  end
if map_size(expected) == 0, do: raise("no generated SDK modules")
actual = Path.wildcard(Path.join(ebin, "*.beam")) |> Enum.map(&Path.basename(&1, ".beam")) |> Enum.sort()
if actual != (Map.keys(expected) |> Enum.sort()), do: raise("compiled SDK module inventory differs")
for {name, source} <- expected do
  module = String.to_existing_atom(name)
  {:module, ^module} = Code.ensure_loaded(module)
  if Path.expand(to_string(:code.which(module))) != Path.join(ebin, name <> ".beam"),
    do: raise("loaded SDK module path differs: " <> name)
  compiled_source = module.module_info(:compile) |> Keyword.fetch!(:source) |> to_string() |> Path.expand()
  if compiled_source != source, do: raise("compiled SDK source path differs: " <> name)
end
IO.puts("PASS Elixir exact compiled SDK module inventory, source paths and loaded BEAM paths")
