//! Standard FUSE notifications over the transport's atomic publication primitive.

use super::*;
use std::io::{self, IoSlice};

pub(super) trait PacketSink: Send + Sync {
    /// Success acknowledges the complete packet, including all borrowed slices.
    fn publish(&self, packet: &[IoSlice<'_>]) -> io::Result<()>;
}

#[derive(Clone)]
pub(super) struct CacheNotifier(Arc<dyn PacketSink>);

impl CacheNotifier {
    pub(super) fn new(sink: Arc<dyn PacketSink>) -> Self {
        Self(sink)
    }

    fn inode(&self, inode: u64, offset: i64) -> io::Result<()> {
        let mut packet = [0; 40];
        encode_field(&mut packet, 0, &40_u32.to_ne_bytes())?;
        encode_field(&mut packet, 4, &2_i32.to_ne_bytes())?;
        encode_field(&mut packet, 16, &inode.to_ne_bytes())?;
        encode_field(&mut packet, 24, &offset.to_ne_bytes())?;
        self.0.publish(&[IoSlice::new(&packet)])
    }

    pub(super) fn inval_attributes(&self, inode: u64) -> io::Result<()> {
        self.inode(inode, -1)
    }
}

impl KernelCacheNotifier for CacheNotifier {
    fn inval_inode(&self, inode: u64) -> io::Result<()> {
        self.inode(inode, 0)
    }

    fn inval_entry(&self, parent: u64, name: &[u8]) -> io::Result<()> {
        let length = 33_usize.checked_add(name.len()).ok_or_else(overflow)?;
        let length = u32::try_from(length).map_err(|_| overflow())?;
        let name_length = u32::try_from(name.len()).map_err(|_| overflow())?;
        let mut packet = [0; 32];
        encode_field(&mut packet, 0, &length.to_ne_bytes())?;
        encode_field(&mut packet, 4, &3_i32.to_ne_bytes())?;
        encode_field(&mut packet, 16, &parent.to_ne_bytes())?;
        encode_field(&mut packet, 24, &name_length.to_ne_bytes())?;
        self.0.publish(&[
            IoSlice::new(&packet),
            IoSlice::new(name),
            IoSlice::new(&[0]),
        ])
    }
}

fn overflow() -> io::Error {
    io::Error::from_raw_os_error(libc::EOVERFLOW)
}

fn encode_field(packet: &mut [u8], offset: usize, bytes: &[u8]) -> io::Result<()> {
    packet
        .get_mut(offset..offset + bytes.len())
        .ok_or_else(overflow)?
        .copy_from_slice(bytes);
    Ok(())
}
