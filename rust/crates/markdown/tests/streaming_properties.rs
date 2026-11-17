//! Reproducible streaming-prefix and corpus invariants.
#[path = "support/streaming_input.rs"]
mod input;
use proptest::prelude::*;

#[test]
fn syntax_data_preserves_bundled_grammars_and_rejects_invalid_envelopes()
-> Result<(), Box<dyn std::error::Error>> {
    let corpus = acyclic_test_support::crate_path!("fuzz/corpus/syntax_data");
    let theme_data = std::fs::read(corpus.join("default.themedump"))?;
    syntect::test_support::check_syntax_data(&theme_data);
    // The envelope matrix is a property of the dump type; one packdump runs it.
    let newlines = std::fs::read(corpus.join("default_newlines.packdump"))?;
    syntect::test_support::check_syntax_data(&newlines);
    let nonewlines = std::fs::read(corpus.join("default_nonewlines.packdump"))?;
    syntect::test_support::check_syntax_round_trip(&nonewlines);
    Ok(())
}

struct SyntaxBundle {
    themes: syntect::highlighting::ThemeSet,
    newlines: syntect::parsing::SyntaxSet,
    nonewlines: syntect::parsing::SyntaxSet,
}

/// The parsed bundled data, loaded once per case process. Every partition
/// uses the same bundle instead of decoding the dumps again.
fn syntax_bundle() -> Result<&'static SyntaxBundle, &'static str> {
    static BUNDLE: std::sync::OnceLock<Result<SyntaxBundle, String>> = std::sync::OnceLock::new();
    BUNDLE
        .get_or_init(|| {
            let corpus = acyclic_test_support::crate_path!("fuzz/corpus/syntax_data");
            let themes = syntect::dumps::from_reader(
                &std::fs::read(corpus.join("default.themedump"))
                    .map_err(|error| format!("read default.themedump: {error}"))?[..],
            )
            .map_err(|error| format!("default.themedump must decode as ThemeSet: {error}"))?;
            let newlines = syntect::dumps::from_uncompressed_data(
                &std::fs::read(corpus.join("default_newlines.packdump"))
                    .map_err(|error| format!("read default_newlines.packdump: {error}"))?,
            )
            .map_err(|error| {
                format!("default_newlines.packdump must decode as SyntaxSet: {error}")
            })?;
            let nonewlines = syntect::dumps::from_uncompressed_data(
                &std::fs::read(corpus.join("default_nonewlines.packdump"))
                    .map_err(|error| format!("read default_nonewlines.packdump: {error}"))?,
            )
            .map_err(|error| {
                format!("default_nonewlines.packdump must decode as SyntaxSet: {error}")
            })?;
            Ok(SyntaxBundle {
                themes,
                newlines,
                nonewlines,
            })
        })
        .as_ref()
        .map_err(String::as_str)
}

fn syntax_total() -> Result<usize, &'static str> {
    let bundle = syntax_bundle()?;
    Ok(bundle.newlines.syntaxes().len() + bundle.nonewlines.syntaxes().len())
}

fn highlight_syntax_partition(assigned: Vec<usize>) -> Result<(), Box<dyn std::error::Error>> {
    use syntect::easy::HighlightLines;

    let bundle = syntax_bundle()?;
    let theme = bundle
        .themes
        .themes
        .values()
        .next()
        .ok_or("the bundled theme set is empty")?;
    let newlines = bundle.newlines.syntaxes().len();
    let source = "let answer = 42; // syntax data\n";
    for index in assigned {
        let (syntaxes, local) = if index < newlines {
            (&bundle.newlines, index)
        } else {
            (&bundle.nonewlines, index - newlines)
        };
        let syntax = syntaxes
            .syntaxes()
            .get(local)
            .ok_or("a syntax partition index is outside its bundle")?;
        let mut highlighter = HighlightLines::new(syntax, theme);
        let spans = highlighter.highlight_line(source, syntaxes)?;
        assert_eq!(
            spans.into_iter().map(|(_, text)| text).collect::<String>(),
            source
        );
    }
    Ok(())
}

acyclic_test_support::partitioned_tests! {
    #[test] fn -> Result<(), Box<dyn std::error::Error>>;
    HIGHLIGHT_PARTITIONS, syntax_total()?, highlight_syntax_partition;
    0 => highlight_syntax_partition_0,
    1 => highlight_syntax_partition_1,
    2 => highlight_syntax_partition_2,
    3 => highlight_syntax_partition_3,
    4 => highlight_syntax_partition_4,
    5 => highlight_syntax_partition_5,
    6 => highlight_syntax_partition_6,
    7 => highlight_syntax_partition_7,
}

#[test]
fn streaming_seed_corpus() -> Result<(), Box<dyn std::error::Error>> {
    let corpus = acyclic_test_support::crate_path!("fuzz/corpus/streaming");
    for entry in std::fs::read_dir(corpus)? {
        input::check(&std::fs::read(entry?.path())?);
    }
    Ok(())
}

#[test]
fn control_before_unicode_punctuation_keeps_literal_delimiters()
-> Result<(), Box<dyn std::error::Error>> {
    let bytes = include_bytes!("../fuzz/corpus/streaming/control-delimiter-context");
    input::check(bytes);
    let mut variant = *bytes;
    let control = bytes
        .iter()
        .position(|&byte| byte == 0x0e)
        .ok_or("the control-delimiter corpus seed has no control byte")?;
    for before in 0..=127 {
        *variant
            .get_mut(control)
            .ok_or("the control byte is inside the corpus seed")? = before;
        input::check(&variant);
    }
    for count in 1..=8 {
        let mut escaped = bytes
            .get(..control)
            .ok_or("the control byte is inside the corpus seed")?
            .to_vec();
        escaped.extend(std::iter::repeat_n(b'\\', count));
        escaped.extend_from_slice(
            bytes
                .get(control + 1..)
                .ok_or("the control byte is inside the corpus seed")?,
        );
        input::check(&escaped);
    }
    Ok(())
}

#[test]
#[should_panic(expected = "changed Markdown rows were not replaced")]
fn negative_control_detects_append_only_rows() {
    input::check_without_row_replacement(b"|a", true);
}

proptest! {
    #![proptest_config(ProptestConfig { rng_seed: proptest::test_runner::RngSeed::Fixed(0x6d61_726b_0000_0006), ..ProptestConfig::with_cases(64) })]
    #[test]
    fn streaming_and_normalized_replay_preserve_every_prefix(
        data in prop_oneof![
            prop::collection::vec(any::<u8>(), 0..512),
            prop::collection::vec(any::<char>(), 0..96)
                .prop_map(|chars| chars.into_iter().collect::<String>().into_bytes()),
        ],
    ) {
        input::check(&data);
    }
}
