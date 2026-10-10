-module(installed_provenance).
-export([run/5]).

inside(Root,File) ->
    R=filename:absname(Root), F=filename:absname(File),
    F=:=R orelse lists:prefix(R++"/",F).

run(Sdk,Dependencies,Tools,Otp,Consumer) ->
    Otp=code:root_dir(),
    true=inside(Otp,code:lib_dir(kernel)),
    Beams=filelib:wildcard(filename:join([Sdk,"ebin","*.beam"])),
    9=length(Beams),
    lists:foreach(fun(File)->
      Module=list_to_atom(filename:basename(File,".beam")),
      {module,Module}=code:ensure_loaded(Module),
      File=code:which(Module),
      Source=filename:join([Sdk,"src",atom_to_list(Module)++".erl"]),
      Source=proplists:get_value(source,Module:module_info(compile)),
      io:format("SDK ~p ~s source ~s~n",[Module,File,Source])
    end,Beams),
    lists:foreach(fun({Module,File})->
      case File of
        preloaded -> ok;
        _ when is_list(File) ->
          true=lists:any(fun(Root)->inside(Root,File) end,[Otp,filename:join(Sdk,"ebin"),Dependencies,filename:join(Tools,"gpb-4.21.7/ebin"),Consumer]),
          io:format("LOADED ~p ~s~n",[Module,File]);
        _ -> error({unexpected_loaded_module,Module,File})
      end
    end,code:all_loaded()),
    io:format("PASS installed SDK and loaded-module provenance~n"),ok.
