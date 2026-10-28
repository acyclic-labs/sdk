//! vhost's standard message/SCM_RIGHTS decoder delegates to physical custody.
//! Filesystem dispatch and source authority are not established by this owner.

use super::vring_control::{FEATURES, QUEUE_COUNT, VringControl};
use super::{Arc, NativeMountError};
use std::fs::File;
use vhost::backend::VhostUserMemoryRegionBase;
use vhost::vhost_user::{
    Error, GpuBackend, Result, VhostUserBackendReqHandlerMut,
    message::{
        VhostTransferStateDirection, VhostTransferStatePhase, VhostUserConfigFlags,
        VhostUserInflight, VhostUserLog, VhostUserMemoryRegion, VhostUserProtocolFeatures,
        VhostUserShMemConfig, VhostUserSharedMsg, VhostUserSingleMemoryRegion,
        VhostUserVringAddrFlags, VhostUserVringState,
    },
};

pub(super) struct VhostControl {
    device: Arc<VringControl>,
    owned: bool,
    negotiated: bool,
    config: [u8; 40],
    // The trait returns an unwrapped Vec. Retain its maximum actual storage
    // through the serial handler's physical send, including send failure.
    _config_reply: crate::NativeAllocation,
}

impl VhostControl {
    pub(super) fn new(
        device: Arc<VringControl>,
        tag: &[u8],
    ) -> std::result::Result<Self, NativeMountError> {
        if tag.is_empty() || tag.len() > 36 || tag.contains(&0) {
            return Err(libc::EINVAL.into());
        }
        let allocation = device.work.claim(40, 1, crate::WorkCounters::UNCHARGED)?;
        let mut config = [0; 40];
        config
            .get_mut(..tag.len())
            .ok_or(libc::EINVAL)?
            .copy_from_slice(tag);
        config.get_mut(36..).ok_or(libc::EINVAL)?.copy_from_slice(
            &u32::try_from(QUEUE_COUNT - 1)
                .map_err(|_| libc::EOVERFLOW)?
                .to_le_bytes(),
        );
        Ok(Self {
            device,
            owned: false,
            negotiated: false,
            config,
            _config_reply: allocation,
        })
    }

    fn apply<T>(
        &self,
        operation: impl FnOnce(&VringControl) -> std::result::Result<T, NativeMountError>,
    ) -> Result<T> {
        let result = self.device.check().and_then(|()| operation(&self.device));
        result.map_err(|error| {
            self.device.fail(error);
            Error::ReqHandlerError(std::io::Error::from_raw_os_error(libc::ECANCELED))
        })
    }

    fn active<T>(
        &self,
        operation: impl FnOnce(&VringControl) -> std::result::Result<T, NativeMountError>,
    ) -> Result<T> {
        self.apply(|device| {
            if !self.owned || !self.negotiated {
                return Err(libc::EPROTO.into());
            }
            operation(device)
        })
    }

    fn owner<T>(
        &self,
        operation: impl FnOnce(&VringControl) -> std::result::Result<T, NativeMountError>,
    ) -> Result<T> {
        self.apply(|device| {
            if !self.owned {
                return Err(libc::EPROTO.into());
            }
            operation(device)
        })
    }

    fn unsupported<T>(&self) -> Result<T> {
        self.apply(|_| Err(libc::ENOTSUP.into()))
    }

    fn protocol_features() -> VhostUserProtocolFeatures {
        VhostUserProtocolFeatures::MQ
            | VhostUserProtocolFeatures::REPLY_ACK
            | VhostUserProtocolFeatures::CONFIG
    }
}

impl VhostUserBackendReqHandlerMut for VhostControl {
    fn set_owner(&mut self) -> Result<()> {
        self.apply(|_| {
            if self.owned {
                Err(libc::EPROTO.into())
            } else {
                Ok(())
            }
        })?;
        self.owned = true;
        Ok(())
    }

    fn reset_owner(&mut self) -> Result<()> {
        self.unsupported()
    }
    fn reset_device(&mut self) -> Result<()> {
        self.unsupported()
    }

    fn get_features(&mut self) -> Result<u64> {
        self.apply(|_| Ok(FEATURES))
    }

    fn set_features(&mut self, features: u64) -> Result<()> {
        self.apply(|device| {
            if !self.owned || self.negotiated {
                return Err(libc::EPROTO.into());
            }
            device.set_features(features)
        })?;
        self.negotiated = true;
        Ok(())
    }

    fn get_protocol_features(&mut self) -> Result<VhostUserProtocolFeatures> {
        self.apply(|_| Ok(Self::protocol_features()))
    }

    fn set_protocol_features(&mut self, features: u64) -> Result<()> {
        self.apply(|_| {
            if features & !Self::protocol_features().bits() != 0 {
                return Err(libc::ENOTSUP.into());
            }
            Ok(())
        })
    }

    fn get_queue_num(&mut self) -> Result<u64> {
        self.apply(|_| Ok(QUEUE_COUNT as u64))
    }

    fn set_mem_table(
        &mut self,
        ctx: &[VhostUserMemoryRegion],
        files: Vec<File>,
        postcopy_listening: Option<bool>,
    ) -> Result<Option<Vec<VhostUserMemoryRegionBase>>> {
        self.active(|device| {
            if postcopy_listening == Some(true) {
                return Err(libc::ENOTSUP.into());
            }
            device.set_memory(ctx, files)?;
            Ok(None)
        })
    }

    fn set_vring_num(&mut self, index: u32, num: u32) -> Result<()> {
        self.active(|device| device.queue(index)?.set_size(num))
    }

    fn set_vring_addr(
        &mut self,
        index: u32,
        flags: VhostUserVringAddrFlags,
        descriptor: u64,
        used: u64,
        available: u64,
        _log: u64,
    ) -> Result<()> {
        self.active(|device| {
            if !flags.is_empty() {
                return Err(libc::ENOTSUP.into());
            }
            device.set_addresses(index, descriptor, used, available)
        })
    }

    fn set_vring_base(&mut self, index: u32, base: u32) -> Result<()> {
        self.active(|device| device.queue(index)?.set_base(base))
    }

    fn get_vring_base(&mut self, index: u32) -> Result<VhostUserVringState> {
        self.active(|device| {
            let base = device.queue(index)?.drain()?;
            device.changed(index)?;
            Ok(VhostUserVringState::new(index, u32::from(base)))
        })
    }

    fn set_vring_kick(&mut self, index: u8, fd: Option<File>) -> Result<()> {
        self.active(|device| device.set_kick(u32::from(index), fd))
    }

    fn set_vring_call(&mut self, index: u8, fd: Option<File>) -> Result<()> {
        self.owner(|device| device.set_call(u32::from(index), fd))
    }

    fn set_vring_err(&mut self, index: u8, fd: Option<File>) -> Result<()> {
        self.owner(|device| device.set_error(u32::from(index), fd))
    }

    fn set_vring_enable(&mut self, index: u32, enable: bool) -> Result<()> {
        self.active(|device| device.set_enabled(index, enable))
    }

    fn get_config(
        &mut self,
        offset: u32,
        size: u32,
        flags: VhostUserConfigFlags,
    ) -> Result<Vec<u8>> {
        self.apply(|device| {
            if !flags.is_empty() {
                return Err(libc::EINVAL.into());
            }
            let start = usize::try_from(offset).map_err(|_| libc::EOVERFLOW)?;
            let end = start
                .checked_add(usize::try_from(size).map_err(|_| libc::EOVERFLOW)?)
                .ok_or(libc::EOVERFLOW)?;
            let bytes = self.config.get(start..end).ok_or(libc::EINVAL)?;
            device.work.charge(
                crate::materializer::NativeResourceCredit::Copy.delta(bytes.len() as u64),
            )?;
            Ok(bytes.to_vec())
        })
    }

    fn set_config(
        &mut self,
        _offset: u32,
        _buf: &[u8],
        _flags: VhostUserConfigFlags,
    ) -> Result<()> {
        self.unsupported()
    }
    fn set_gpu_socket(&mut self, _backend: GpuBackend) -> Result<()> {
        self.unsupported()
    }
    fn get_shared_object(&mut self, _uuid: VhostUserSharedMsg) -> Result<File> {
        self.unsupported()
    }
    fn get_inflight_fd(
        &mut self,
        _inflight: &VhostUserInflight,
    ) -> Result<(VhostUserInflight, File)> {
        self.unsupported()
    }
    fn set_inflight_fd(&mut self, _inflight: &VhostUserInflight, _file: File) -> Result<()> {
        self.unsupported()
    }
    fn get_max_mem_slots(&mut self) -> Result<u64> {
        self.unsupported()
    }
    fn add_mem_region(
        &mut self,
        _region: &VhostUserSingleMemoryRegion,
        _fd: File,
        _postcopy_listening: Option<bool>,
    ) -> Result<Option<VhostUserMemoryRegionBase>> {
        self.unsupported()
    }
    fn remove_mem_region(&mut self, _region: &VhostUserSingleMemoryRegion) -> Result<()> {
        self.unsupported()
    }
    fn set_device_state_fd(
        &mut self,
        _direction: VhostTransferStateDirection,
        _phase: VhostTransferStatePhase,
        _fd: File,
    ) -> Result<Option<File>> {
        self.unsupported()
    }
    fn check_device_state(&mut self) -> Result<()> {
        self.unsupported()
    }
    fn get_shmem_config(&mut self) -> Result<VhostUserShMemConfig> {
        self.unsupported()
    }
    fn set_log_base(&mut self, _log: &VhostUserLog, _file: File) -> Result<()> {
        self.unsupported()
    }
}
