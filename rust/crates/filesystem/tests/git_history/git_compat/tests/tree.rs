use acyclic_fs::{
    Fs, GitGrepMatch, IdempotencyKey, TransactionCommit, grep_git_generation, walk_git_tree,
};
use acyclic_stream::OwnedBytes;

use crate::transaction::commit;

#[tokio::test]
async fn tree_walk_and_grep_are_generation_bound_and_portable()
-> Result<(), Box<dyn std::error::Error>> {
    let workspace = unbounded!(Fs::memory(), create_workspace("git-inspection"))?;
    let mut transaction = unbounded!(
        workspace,
        begin_transaction(IdempotencyKey::from_bytes([0x32; 16]))
    )?;
    unbounded!(transaction, create_dir_all("/src"))?;
    for (path, text) in [
        ("/src/a.txt", "one\nneedle\n"),
        ("/src/b.txt", "needle two\n"),
        ("/src/large.txt", "needle is too large"),
    ] {
        transaction.write_text(path, text).await?;
    }
    unbounded!(
        transaction,
        write(
            "/src/binary.bin",
            OwnedBytes::from_static(b"needle\xffbinary")
        )
    )?;
    let TransactionCommit::Committed(generation) = commit(&mut transaction).await? else {
        return Err("grep fixture did not commit".into());
    };
    let entries = walk_git_tree(&generation, Some("src"), 10).await?;
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.path.as_str())
            .collect::<Vec<_>>(),
        vec!["src/a.txt", "src/b.txt", "src/binary.bin", "src/large.txt"]
    );
    let result = grep_git_generation(&generation, "needle", Some("src"), 10, 16, 10).await?;
    assert!(result.truncated);
    let [first, second] = result.matches.as_slice() else {
        return Err("expected two grep matches".into());
    };
    assert_eq!(first.line, 2);
    assert_eq!(second.line, 1);

    let single = grep_git_generation(&generation, "needle", Some("src/a.txt"), 1, 16, 10).await?;
    assert!(!single.truncated);
    let [hit] = single.matches.as_slice() else {
        return Err("expected one grep match".into());
    };
    assert_eq!(hit.path, "src/a.txt");
    assert_eq!(hit.line, 2);
    Ok(())
}

#[tokio::test]
async fn unbounded_grep_matches_utf8_lines_across_physical_read_chunks()
-> Result<(), Box<dyn std::error::Error>> {
    let workspace = unbounded!(Fs::memory(), create_workspace("git-chunked-grep"))?;
    let mut transaction = unbounded!(workspace, begin_transaction(IdempotencyKey::new()))?;
    let text = format!(
        "{}needle\r\nnext éneedle\r\nlast needle\r",
        "é".repeat(32_766)
    );
    transaction.write_text("/text", &text).await?;
    let mut binary = b"needle\n".to_vec();
    binary.extend(vec![b'x'; 65_536]);
    binary.push(0xff);
    unbounded!(transaction, write("/binary", OwnedBytes::from(binary)))?;
    let TransactionCommit::Committed(generation) = commit(&mut transaction).await? else {
        return Err("grep fixture did not commit".into());
    };
    let found = grep_git_generation(&generation, "needle", None, None, None, None).await?;
    let expected: Vec<_> = text
        .lines()
        .enumerate()
        .filter(|(_, line)| line.contains("needle"))
        .map(|(index, line)| GitGrepMatch {
            path: "text".to_owned(),
            line: index as u64 + 1,
            text: line.to_owned(),
        })
        .collect();
    assert_eq!(found.matches, expected);
    assert!(!found.truncated);
    let bounded = grep_git_generation(&generation, "needle", None, None, None, 1).await?;
    assert_eq!(
        bounded.matches,
        expected.into_iter().take(1).collect::<Vec<_>>()
    );
    assert!(bounded.truncated);
    assert_eq!(
        unbounded!(generation, read("/text", None))?.as_ref(),
        text.as_bytes()
    );
    assert!(unbounded!(generation, read("/text", 16)).is_err());
    Ok(())
}
