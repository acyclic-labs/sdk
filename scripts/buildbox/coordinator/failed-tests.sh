#!/bin/bash
# Failing test names from a cargo test log. Parallel output can split "test X ... FAILED" across lines,
# so also read each binary's end-of-run "failures:" name list, which is never interleaved.
# The first "failures:" block holds "---- name stdout ----" details (debug output); only the name list counts.
{
awk '/^test .* FAILED$/{sub(/^test /,""); sub(/ \.\.\. FAILED$/,""); print; next}
     /^failures:$/{s=1; next}
     /^---- /{s=0}
     s && /^    [A-Za-z_][A-Za-z0-9_]*(::[A-Za-z0-9_]+)*$/{sub(/^    /,""); print; next}
     /^test result:/{s=0}' "$1"
# Non-libtest targets (benchmarks and libFuzzer binaries) have no test-name
# failure list. Qualify their package/target identity against pinned main too.
sed -nE 's/^error: test failed, to rerun pass .*-p ([A-Za-z0-9_-]+) --(bench|bin|example) ([A-Za-z0-9_-]+).*/target::\1::\2::\3/p' "$1"
} | sort -u
