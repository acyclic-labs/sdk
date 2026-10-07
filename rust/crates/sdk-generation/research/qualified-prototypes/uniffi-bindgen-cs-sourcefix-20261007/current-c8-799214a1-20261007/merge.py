from pathlib import Path
D=Path('/tmp/c8-799-gen/domain/acyclic_actors.cs').read_text()
F=Path('/tmp/c8-799-gen/facade/acyclic_actors_uniffi.cs').read_text()
marker='// Public interface members begin here.'
startD=D.index('#if NET8_0_OR_GREATER\nstatic partial class _UniFFILib')
startF=F.index('#if NET8_0_OR_GREATER\nstatic partial class _UniFFILib')
endD=D.index(marker,startD)
endF=F.index(marker,startF)
needed=['ffi_acyclic_actors_rustbuffer_alloc','ffi_acyclic_actors_rustbuffer_free','ffi_acyclic_actors_uniffi_contract_version']
def declaration(text,name,begin):
    pos=text.index(name,begin)
    pre=text.rfind('#if NET8_0_OR_GREATER',0,pos)
    end=text.index('\n    );',pos)+len('\n    );')
    return text[pre:end]
extra='\n'.join(declaration(D,n,startD) for n in needed)
facade_native=F[startF:endF]
insert=facade_native.rfind('\n    static void uniffiCheckContractApiVersion')
facade_native=facade_native[:insert]+'\n'+extra+'\n'+facade_native[insert:]
domain_suffix=D[D.index('class FfiConverterUInt32'):]
facade_int=F[F.index('class FfiConverterInt32'):F.index('class FfiConverterBoolean:')]
facade_client=F[F.index('public interface IActorsClient'):F.index('class FfiConverterTypeAddSubscriptionRequest:')]
facade_async=F[F.index('class ConcurrentHandleMap'):]
merged=D[:startD]+facade_native+'\n'+facade_int+domain_suffix+'\n'+facade_client+facade_async
Path('/tmp/c8-799-gen/merged-c8-799.cs').write_text(merged)
print(len(merged), merged.count('public record '), merged.count('CancellationToken'))

