//! Methods for dumping serializable structs to a compressed binary format,
//! used to allow fast startup times
//!
//! Currently syntect serializes [`SyntaxSet`] structs with [`dump_to_uncompressed_file`]
//! into `.packdump` files and likewise [`ThemeSet`] structs to `.themedump` files with [`dump_to_file`].
//!
//! You can use these methods to manage your own caching of compiled syntaxes and
//! themes. And even your own `serde::Serialize` structures if you want to
//! be consistent with your format.
//!
//! [`SyntaxSet`]: ../parsing/struct.SyntaxSet.html
//! [`dump_to_uncompressed_file`]: fn.dump_to_uncompressed_file.html
//! [`ThemeSet`]: ../highlighting/struct.ThemeSet.html
//! [`dump_to_file`]: fn.dump_to_file.html
#[cfg(feature = "default-themes")]
use crate::highlighting::ThemeSet;
#[cfg(feature = "default-syntaxes")]
use crate::parsing::SyntaxSet;
#[cfg(feature = "dump-load")]
use flate2::bufread::ZlibDecoder;
#[cfg(feature = "dump-create")]
use flate2::write::ZlibEncoder;
#[cfg(feature = "dump-create")]
use flate2::Compression;
#[cfg(feature = "dump-load")]
use serde::de::DeserializeOwned;
#[cfg(feature = "dump-create")]
use serde::ser::Serialize;
use std::fs::File;
#[cfg(feature = "dump-load")]
use std::io::BufRead;
#[cfg(feature = "dump-create")]
use std::io::{BufWriter, Write};
use std::io::{Error, ErrorKind, Read, Result};
use std::path::Path;

/// Dumps an object to the given writer in a compressed binary format
///
/// The writer is encoded with version 1 MessagePack and compressed with `flate2`.
#[cfg(feature = "dump-create")]
pub fn dump_to_writer<T: Serialize, W: Write>(to_dump: &T, output: W) -> Result<()> {
    serialize_to_writer_impl(to_dump, output, true)
}

/// Dumps an object to a binary array in the same format as [`dump_to_writer`]
///
/// [`dump_to_writer`]: fn.dump_to_writer.html
#[cfg(feature = "dump-create")]
pub fn dump_binary<T: Serialize>(o: &T) -> Vec<u8> {
    let mut v = Vec::new();
    dump_to_writer(o, &mut v).unwrap();
    v
}

/// Dumps an encodable object to a file at a given path, in the same format as [`dump_to_writer`]
///
/// If a file already exists at that path it will be overwritten. The files created are encoded with
/// version 1 MessagePack and then compressed with the `flate2` crate.
///
/// [`dump_to_writer`]: fn.dump_to_writer.html
#[cfg(feature = "dump-create")]
pub fn dump_to_file<T: Serialize, P: AsRef<Path>>(o: &T, path: P) -> Result<()> {
    dump_to_file_impl(o, path, true)
}

/// A helper function for decoding and decompressing data from a reader
#[cfg(feature = "dump-load")]
pub fn from_reader<T: DeserializeOwned, R: BufRead>(input: R) -> Result<T> {
    deserialize_from_reader_impl(input, true)
}

/// Returns a fully loaded object from a binary dump.
///
/// This function panics if the dump is invalid.
#[cfg(feature = "dump-load")]
pub fn from_binary<T: DeserializeOwned>(v: &[u8]) -> T {
    from_reader(v).unwrap()
}

/// Returns a fully loaded object from a binary dump file.
#[cfg(feature = "dump-load")]
pub fn from_dump_file<T: DeserializeOwned, P: AsRef<Path>>(path: P) -> Result<T> {
    let contents = std::fs::read(path)?;
    from_reader(&contents[..])
}

/// To be used when serializing a [`SyntaxSet`] to a file. A [`SyntaxSet`]
/// itself shall not be compressed, because the data for its lazy-loaded
/// syntaxes are already compressed. Compressing another time just results in
/// bad performance.
#[cfg(feature = "dump-create")]
pub fn dump_to_uncompressed_file<T: Serialize, P: AsRef<Path>>(o: &T, path: P) -> Result<()> {
    dump_to_file_impl(o, path, false)
}

#[cfg(feature = "dump-create")]
fn dump_to_file_impl<T: Serialize, P: AsRef<Path>>(
    o: &T,
    path: P,
    compression: bool,
) -> Result<()> {
    let mut out = BufWriter::new(File::create(path)?);
    serialize_to_writer_impl(o, &mut out, compression)?;
    out.flush()
}

/// To be used when deserializing a [`SyntaxSet`] that was previously written to
/// file using [dump_to_uncompressed_file].
#[cfg(feature = "dump-load")]
pub fn from_uncompressed_dump_file<T: DeserializeOwned, P: AsRef<Path>>(path: P) -> Result<T> {
    let contents = std::fs::read(path)?;
    deserialize_from_reader_impl(&contents[..], false)
}

/// To be used when deserializing a [`SyntaxSet`] from raw data, for example
/// data that has been embedded in your own binary with the [`include_bytes!`]
/// macro.
#[cfg(feature = "dump-load")]
pub fn from_uncompressed_data<T: DeserializeOwned>(v: &[u8]) -> Result<T> {
    deserialize_from_reader_impl(v, false)
}

/// Private low level helper function used to implement the public API.
#[cfg(feature = "dump-create")]
fn serialize_to_writer_impl<T: Serialize, W: Write>(
    to_dump: &T,
    output: W,
    use_compression: bool,
) -> Result<()> {
    if use_compression {
        let mut encoder = ZlibEncoder::new(output, Compression::best());
        write_dump(to_dump, &mut encoder)?;
        encoder.finish()?;
        Ok(())
    } else {
        write_dump(to_dump, output)
    }
}

#[cfg(feature = "dump-create")]
fn write_dump<T: Serialize, W: Write>(value: &T, mut output: W) -> Result<()> {
    output.write_all(&1u32.to_be_bytes())?;
    value
        .serialize(
            &mut rmp_serde::Serializer::new(output)
                .with_struct_map()
                .with_human_readable(),
        )
        .map_err(Error::other)
}

#[cfg(feature = "dump-load")]
fn deserialize_from_reader_impl<T: DeserializeOwned, R: BufRead>(
    input: R,
    use_compression: bool,
) -> Result<T> {
    if use_compression {
        read_dump(ZlibDecoder::new(input))
    } else {
        read_dump(input)
    }
}

#[cfg(feature = "dump-load")]
fn read_dump<T: DeserializeOwned, R: Read>(mut input: R) -> Result<T> {
    let mut version = [0; 4];
    input.read_exact(&mut version)?;
    if u32::from_be_bytes(version) != 1 {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "unsupported syntax data version",
        ));
    }
    let value = T::deserialize(&mut rmp_serde::Deserializer::new(&mut input).with_human_readable())
        .map_err(Error::other)?;
    if input.read(&mut [0])? != 0 {
        return Err(Error::new(ErrorKind::InvalidData, "trailing syntax data"));
    }
    Ok(value)
}

#[cfg(feature = "default-syntaxes")]
impl SyntaxSet {
    /// Instantiates a new syntax set from a binary dump of Sublime Text's default open source
    /// syntax definitions.
    ///
    /// These dumps are included in this library's binary for convenience.
    ///
    /// This method loads the version for parsing line strings with no `\n` characters at the end.
    /// If you're able to efficiently include newlines at the end of strings, use
    /// [`load_defaults_newlines`] since it works better. See [`SyntaxSetBuilder::add_from_folder`]
    /// for more info on this issue.
    ///
    /// This is the recommended way of creating a syntax set for non-advanced use cases. It is also
    /// significantly faster than loading the YAML files.
    ///
    /// Note that you can load additional syntaxes after doing this. If you want you can even use
    /// the fact that SyntaxDefinitions are serializable as version 1 MessagePack to cache dumps of
    /// additional syntaxes yourself.
    ///
    /// [`load_defaults_newlines`]: #method.load_defaults_nonewlines
    /// [`SyntaxSetBuilder::add_from_folder`]: struct.SyntaxSetBuilder.html#method.add_from_folder
    pub fn load_defaults_nonewlines() -> SyntaxSet {
        #[cfg(feature = "metadata")]
        {
            let mut ps: SyntaxSet =
                from_uncompressed_data(include_bytes!("../assets/default_nonewlines.packdump"))
                    .unwrap();
            let metadata = from_binary(include_bytes!("../assets/default_metadata.packdump"));
            ps.metadata = metadata;
            ps
        }
        #[cfg(not(feature = "metadata"))]
        {
            from_uncompressed_data(include_bytes!("../assets/default_nonewlines.packdump")).unwrap()
        }
    }

    /// Same as [`load_defaults_nonewlines`] but for parsing line strings with newlines at the end.
    ///
    /// These are separate methods because thanks to linker garbage collection, only the serialized
    /// dumps for the method(s) you call will be included in the binary (each is ~200kb for now).
    ///
    /// [`load_defaults_nonewlines`]: #method.load_defaults_nonewlines
    pub fn load_defaults_newlines() -> SyntaxSet {
        #[cfg(feature = "metadata")]
        {
            let mut ps: SyntaxSet =
                from_uncompressed_data(include_bytes!("../assets/default_newlines.packdump"))
                    .unwrap();
            let metadata = from_binary(include_bytes!("../assets/default_metadata.packdump"));
            ps.metadata = metadata;
            ps
        }
        #[cfg(not(feature = "metadata"))]
        {
            from_uncompressed_data(include_bytes!("../assets/default_newlines.packdump")).unwrap()
        }
    }
}

#[cfg(feature = "default-themes")]
impl ThemeSet {
    /// Loads the set of default themes
    /// Currently includes (these are the keys for the map):
    ///
    /// - `base16-ocean.dark`,`base16-eighties.dark`,`base16-mocha.dark`,`base16-ocean.light`
    /// - `InspiredGitHub` from [here](https://github.com/sethlopezme/InspiredGitHub.tmtheme)
    /// - `Solarized (dark)` and `Solarized (light)`
    pub fn load_defaults() -> ThemeSet {
        from_binary(include_bytes!("../assets/default.themedump"))
    }
}
