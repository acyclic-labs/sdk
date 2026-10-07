import json
from pathlib import Path
p=Path('receipt-combined.json')
j=json.loads(p.read_text(encoding='utf-8-sig'))
j['status']='QUALIFIED_CSHARP_STREAM_WINDOWS_LINUX_MACOS_COMBINED_NUGET_CURRENT_PRODUCER'
j['scope']='single combined parent package; fresh external restore/build/runtime qualification on Windows, WSL Linux, and macOS; no publish, merge, registry, or deployment'
j['fresh_windows_consumer']={
 'host':'Windows', 'platform':'Windows x64', 'dotnet':'8.0.425 task-local',
 'package_cache':'C:/Users/varun/.codex/tmp-csharp-stream-packages-combined-win',
 'restore_log':'evidence/combined-windows/consumer-restore.log','restore_log_sha256':'D36B3D321B3CA2D4CC5B0F6D004C3C30D62B2F3A3053A959825DB3F74DE68F84',
 'build_log':'evidence/combined-windows/consumer-build.log','build_log_sha256':'FD0E46D55D04FBC3FB2465204AFE17CE8EBFEBDF99D1C7BAB4ACFCF30155AEC0',
 'local_run_log':'evidence/combined-windows/consumer-local-run.log','local_run_log_sha256':'53DBE0BBE7C1E276A80EE6250A2C7DB123D4C96CE3D125D9DC36BBA8383CF560',
 'remote_run_log':'evidence/combined-windows/consumer-remote-run.log','remote_run_log_sha256':'5E5A7CB1097905F23E360CCEE22BB89D20002F8DD902EE524869B8ACFB7803EB',
 'negative_build_log':'evidence/combined-windows/negative-build.log','negative_build_log_sha256':'E5145D4A3B54B51102B6EE15FAD2C827A5B5ECE0A7E337DDB1FE65675DE526C0',
 'fixture_exe_sha256':'7835D929ACD9C2A975330C53250CFAF7F27D610E623A3E64B180B100DE1A455D',
 'fixture_endpoint':'https://localhost:55003','fixture_ca_sha256':'AE27CD0D766FA2D7E0DC02B6599ACFCE58EBE1390FC0E1511217EE1A9E6CB5E3',
 'passes':['CSHARP_STREAM_LOCAL_ORDERED_TYPED_U64_PASS','CSHARP_STREAM_LOCAL_CANCELLATION_CLOSE_RECOVERY_PASS','CSHARP_STREAM_REMOTE_TLS_ORDERED_TYPED_U64_PASS','CSHARP_STREAM_REMOTE_CANCELLATION_SERVER_CLOSE_RECOVERY_PASS','negative compile CS1729 RecordCursor does not contain a constructor that takes 2 arguments'],
 'server_markers':['follow-open','follow-closed']
}
j['fresh_linux_consumer']={
 'host':'WSL2', 'platform':'Linux x86_64', 'dotnet':'8.0.425 task-local',
 'package_cache':'/tmp/nuget-csharp-stream-combined-linux',
 'restore_log':'evidence/combined-linux/consumer-restore.log','restore_log_sha256':'533184408CBFAB36425226795E2133B980B67F3F099F0379AEA64BB307EDC5E3',
 'build_log':'evidence/combined-linux/consumer-build.log','build_log_sha256':'308BC54E4A9739D716FF98106760BD7AA7244203B21B6CB07324E3826DD61CC7',
 'local_run_log':'evidence/combined-linux/consumer-local-run.log','local_run_log_sha256':'70AB938B582B4010E06377031D239A35FF87EAD670ECB930E9B6290018C63E04',
 'remote_run_log':'evidence/combined-linux/consumer-remote-run.log','remote_run_log_sha256':'6FBDF9E5E8E4F6A02D559DD068865A76DC4E14AE2CB30BCE3908C5B360BE8A84',
 'negative_build_log':'evidence/combined-linux/negative-build.log','negative_build_log_sha256':'D58748A57DD6BFFEE02F60796180C8AF76E958AD1283021DC8831B908D109F8B',
 'fixture_exe_sha256':'677B0E48EB037BC825D61E497EBC A0C323FAE350FE2A5CEA8C64C88A5FF41C94'.replace(' ',''),
 'fixture_endpoint':'https://localhost:38203','fixture_ca_sha256':'438ABFDAC3CBDE2956A852982D115E4B941860BA2A066F2D613E5A6C5159EDFB',
 'passes':['CSHARP_STREAM_LOCAL_ORDERED_TYPED_U64_PASS','CSHARP_STREAM_LOCAL_CANCELLATION_CLOSE_RECOVERY_PASS','CSHARP_STREAM_REMOTE_TLS_ORDERED_TYPED_U64_PASS','CSHARP_STREAM_REMOTE_CANCELLATION_SERVER_CLOSE_RECOVERY_PASS','negative compile CS1729 RecordCursor does not contain a constructor that takes 2 arguments'],
 'server_markers':['follow-open','follow-closed']
}
j['cross_platform_boundary']={'windows':'fresh combined package restore/build/local+TLS remote/negative above','linux':'fresh combined package restore/build/local+TLS remote/negative above','macos':'fresh combined package restore/build/local+TLS remote above','asset_identity':'all three consumers restored version 0.1.0-c3d82376-combined; installed native hashes match package and exact c3d82376 producer','all_operations':'all eight local/remote unary/stream roots exercised; full-width ulong boundaries and cancellation recovery passed'}
p.write_text(json.dumps(j,indent=2)+'\n',encoding='utf-8')
print('updated',p)
