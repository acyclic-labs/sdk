pub(crate) fn document(bytes: usize) -> String {
    let mut record = String::from(
        "# Streaming\n\nA paragraph with **bold**, *emphasis*, and [a link](url).\n\n\
         ```rust\nlet answer = 42;\n```\n\n\
         | name | value |\n| --- | --- |\n| item | 42 |\n\n\
         - first\n- second\n\n\
         $x + y$ and $$z = 42$$\n\n\
         :file[src/lib.rs#L1-L4]\n\n::file[src/lib.rs#L1-4]{lines=1-4}\n\n",
    );
    record.extend(std::iter::repeat_n('x', 1024 - record.len() - 2));
    record.push_str("\n\n");
    let mut source = record.repeat(bytes.div_ceil(record.len()));
    source.truncate(bytes);
    source
}
