// Node counts the file itself as a passing test when its name pattern matches
// nothing. The file's own summary counts cases before Node adds that result.
export default async function* checkSelection(events) {
    let selected = false;
    for await (const {type, data} of events) {
        if (type !== 'test:summary') continue;
        if (data.file !== undefined) {
            selected ||= data.counts.tests > 0;
        } else if (!selected && data.counts.failed === 0) {
            process.exitCode = 1;
            yield 'Configured test name pattern selected no cases.\n';
        }
    }
}
