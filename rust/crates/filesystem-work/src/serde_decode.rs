//! One funded serde visitor for persisted metadata on every backend.

use crate::performance::WorkCounters;

/// Backend adapter for the original work and allocation owner. Failures remain
/// typed in `MaterializationDecode::failure` while serde unwinds its visitor.
pub trait DecodeWork {
    /// Allocation custody attached to the decoded value.
    type Allocation;
    /// Original error recorded while the serde visitor unwinds.
    type Error;

    /// Admit a charge before performing the effect.
    fn admit(&self, delta: WorkCounters) -> Result<(), Self::Error>;
    /// Record an admitted effect after it completes.
    fn spent(&self, delta: WorkCounters) -> Result<(), Self::Error>;
    /// Account for work that completes without a separate admission step.
    fn charge(&self, delta: WorkCounters) -> Result<(), Self::Error>;
    /// Claim ownership of admitted allocation capacity.
    fn claim(&self, bytes: u64, operations: u64) -> Result<Self::Allocation, Self::Error>;
    /// Grow the original allocation claim.
    fn grow(&self, allocation: &mut Self::Allocation, bytes: u64) -> Result<(), Self::Error>;
    /// Transfer already admitted capacity between original claims.
    fn absorb(
        &self,
        allocation: &mut Self::Allocation,
        incoming: &mut Self::Allocation,
    ) -> Result<(), Self::Error>;
}

/// Admits owned serde data before the destination visitor allocates it.
pub struct MaterializationDecode<'a, W: DecodeWork> {
    work: &'a W,
    allocation: std::cell::RefCell<W::Allocation>,
    failure: std::cell::RefCell<Option<W::Error>>,
}

impl<'a, W: DecodeWork> MaterializationDecode<'a, W> {
    /// Starts a funded decode by claiming custody on this work owner.
    pub fn new(work: &'a W) -> Result<Self, W::Error> {
        let allocation = work.claim(0, 0)?;
        Ok(Self {
            work,
            allocation: std::cell::RefCell::new(allocation),
            failure: std::cell::RefCell::new(None),
        })
    }

    /// Decodes one value with admission before destination allocation.
    pub fn deserialize<'de, T, D>(&self, deserializer: D) -> Result<T, D::Error>
    where
        T: serde::Deserialize<'de>,
        D: serde::Deserializer<'de>,
    {
        T::deserialize(Funded {
            inner: deserializer,
            decode: self,
            kind: DecodeKind::Borrowed,
        })
    }

    /// Takes the original work error, if the visitor stopped on one.
    pub fn take_failure(&self) -> Option<W::Error> {
        self.failure.borrow_mut().take()
    }

    /// Returns the original allocation owner with the destination charge.
    pub fn into_allocation(self) -> W::Allocation {
        self.allocation.into_inner()
    }
}

impl<W: DecodeWork> MaterializationDecode<'_, W> {
    fn error(&self, error: W::Error) -> &'static str {
        *self.failure.borrow_mut() = Some(error);
        "materialization decode failed"
    }

    fn copy<T, E: serde::de::Error>(
        &self,
        bytes: usize,
        effect: impl FnOnce() -> Result<T, E>,
    ) -> Result<T, E> {
        let copied = WorkCounters {
            bytes_copied: bytes as u64,
            ..Default::default()
        };
        self.work
            .admit(copied)
            .map_err(|error| E::custom(self.error(error)))?;
        self.work
            .grow(&mut self.allocation.borrow_mut(), bytes as u64)
            .map_err(|error| E::custom(self.error(error)))?;
        let value = effect()?;
        self.work
            .spent(copied)
            .map_err(|error| E::custom(self.error(error)))?;
        Ok(value)
    }

    /// Grow the original allocation claim.
    fn grow_sequence(&self, width: usize, capacity: &mut usize) -> Result<(), &'static str> {
        let minimum = match width {
            1 => 8,
            2..=1024 => 4,
            _ => 1,
        };
        let next = if *capacity == 0 {
            minimum
        } else {
            capacity
                .checked_mul(2)
                .ok_or("sequence capacity overflow")?
        };
        let bytes = (next - *capacity)
            .checked_mul(width)
            .ok_or("sequence allocation overflow")?;
        self.work
            .grow(&mut self.allocation.borrow_mut(), bytes as u64)
            .map_err(|error| self.error(error))?;
        *capacity = next;
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq)]
enum DecodeKind {
    Borrowed,
    String,
    ByteBuffer,
    Sequence,
    Tree,
    Item,
}

struct Funded<'a, 'b, W: DecodeWork, T> {
    inner: T,
    decode: &'a MaterializationDecode<'b, W>,
    kind: DecodeKind,
}

macro_rules! deserialize {
    ($method:ident) => {
        fn $method<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
            self.inner.$method(visitor)
        }
    };
    ($method:ident, $kind:expr $(, $arg:ident: $type:ty)*) => {
        fn $method<V: serde::de::Visitor<'de>>(self, $($arg: $type,)* visitor: V) -> Result<V::Value, Self::Error> {
            self.inner.$method($($arg,)* Funded { inner: visitor, decode: self.decode, kind: $kind })
        }
    };
}

impl<'de, W: DecodeWork, D: serde::Deserializer<'de>> serde::Deserializer<'de>
    for Funded<'_, '_, W, D>
{
    /// Original error recorded while the serde visitor unwinds.
    type Error = D::Error;
    deserialize!(deserialize_any, DecodeKind::String);
    deserialize!(deserialize_bool);
    deserialize!(deserialize_i8);
    deserialize!(deserialize_i16);
    deserialize!(deserialize_i32);
    deserialize!(deserialize_i64);
    deserialize!(deserialize_i128);
    deserialize!(deserialize_u8);
    deserialize!(deserialize_u16);
    deserialize!(deserialize_u32);
    deserialize!(deserialize_u64);
    deserialize!(deserialize_u128);
    deserialize!(deserialize_f32);
    deserialize!(deserialize_f64);
    deserialize!(deserialize_char);
    deserialize!(deserialize_str);
    deserialize!(deserialize_string, DecodeKind::String);
    deserialize!(deserialize_bytes);
    deserialize!(deserialize_byte_buf, DecodeKind::ByteBuffer);
    deserialize!(deserialize_option, DecodeKind::Borrowed);
    deserialize!(deserialize_unit);
    deserialize!(deserialize_unit_struct, DecodeKind::Borrowed, name: &'static str);
    fn deserialize_newtype_struct<V: serde::de::Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_newtype_struct(
            name,
            Funded {
                inner: visitor,
                decode: self.decode,
                kind: if name == "CoreStateChildSet" {
                    DecodeKind::Tree
                } else {
                    DecodeKind::Borrowed
                },
            },
        )
    }
    fn deserialize_seq<V: serde::de::Visitor<'de>>(
        self,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_seq(Funded {
            inner: visitor,
            decode: self.decode,
            kind: if self.kind == DecodeKind::Tree {
                DecodeKind::Tree
            } else {
                DecodeKind::Sequence
            },
        })
    }
    deserialize!(deserialize_tuple, DecodeKind::Borrowed, length: usize);
    deserialize!(deserialize_tuple_struct, DecodeKind::Borrowed, name: &'static str, length: usize);
    deserialize!(deserialize_map, DecodeKind::Tree);
    deserialize!(deserialize_struct, DecodeKind::Borrowed, name: &'static str, fields: &'static [&'static str]);
    deserialize!(deserialize_enum, DecodeKind::Borrowed, name: &'static str, variants: &'static [&'static str]);
    deserialize!(deserialize_identifier);
    deserialize!(deserialize_ignored_any);
    fn is_human_readable(&self) -> bool {
        self.inner.is_human_readable()
    }
}

macro_rules! visit {
    ($method:ident, $type:ty) => {
        fn $method<E: serde::de::Error>(self, value: $type) -> Result<Self::Value, E> {
            self.inner.$method(value)
        }
    };
}

impl<'de, W: DecodeWork, V: serde::de::Visitor<'de>> serde::de::Visitor<'de>
    for Funded<'_, '_, W, V>
{
    type Value = V::Value;
    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.expecting(formatter)
    }
    visit!(visit_bool, bool);
    visit!(visit_i64, i64);
    visit!(visit_u64, u64);
    visit!(visit_f64, f64);
    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
        if matches!(self.kind, DecodeKind::String | DecodeKind::ByteBuffer) {
            self.decode
                .copy(value.len(), || self.inner.visit_str(value))
        } else {
            self.inner.visit_str(value)
        }
    }
    fn visit_borrowed_str<E: serde::de::Error>(self, value: &'de str) -> Result<Self::Value, E> {
        if matches!(self.kind, DecodeKind::String | DecodeKind::ByteBuffer) {
            self.decode
                .copy(value.len(), || self.inner.visit_borrowed_str(value))
        } else {
            self.inner.visit_borrowed_str(value)
        }
    }
    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
        self.decode
            .work
            .grow(
                &mut self.decode.allocation.borrow_mut(),
                value.capacity() as u64,
            )
            .map_err(|error| E::custom(self.decode.error(error)))?;
        self.inner.visit_string(value)
    }
    fn visit_bytes<E: serde::de::Error>(self, value: &[u8]) -> Result<Self::Value, E> {
        if self.kind == DecodeKind::ByteBuffer {
            self.decode
                .copy(value.len(), || self.inner.visit_bytes(value))
        } else {
            self.inner.visit_bytes(value)
        }
    }
    fn visit_borrowed_bytes<E: serde::de::Error>(self, value: &'de [u8]) -> Result<Self::Value, E> {
        if self.kind == DecodeKind::ByteBuffer {
            self.decode
                .copy(value.len(), || self.inner.visit_borrowed_bytes(value))
        } else {
            self.inner.visit_borrowed_bytes(value)
        }
    }
    fn visit_byte_buf<E: serde::de::Error>(self, value: Vec<u8>) -> Result<Self::Value, E> {
        self.decode
            .work
            .grow(
                &mut self.decode.allocation.borrow_mut(),
                value.capacity() as u64,
            )
            .map_err(|error| E::custom(self.decode.error(error)))?;
        self.inner.visit_byte_buf(value)
    }
    fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        self.inner.visit_none()
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        self.inner.visit_unit()
    }
    fn visit_some<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        self.inner.visit_some(Funded {
            inner: deserializer,
            decode: self.decode,
            kind: DecodeKind::Borrowed,
        })
    }
    fn visit_newtype_struct<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        self.inner.visit_newtype_struct(Funded {
            inner: deserializer,
            decode: self.decode,
            kind: self.kind,
        })
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
        self.inner.visit_seq(MaterializationSequence {
            inner: access,
            decode: self.decode,
            kind: self.kind,
            length: 0,
            capacity: 0,
        })
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
        self.inner.visit_map(MaterializationMap {
            inner: access,
            decode: self.decode,
            kind: self.kind,
            key_layout: std::alloc::Layout::new::<()>(),
        })
    }
    fn visit_enum<A: serde::de::EnumAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
        self.inner.visit_enum(Funded {
            inner: access,
            decode: self.decode,
            kind: DecodeKind::Borrowed,
        })
    }
}

impl<'de, W: DecodeWork, S: serde::de::DeserializeSeed<'de>> serde::de::DeserializeSeed<'de>
    for Funded<'_, '_, W, S>
{
    type Value = S::Value;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        use serde::de::Error as _;
        if self.kind == DecodeKind::Item {
            self.decode
                .work
                .charge(WorkCounters {
                    items_examined: 1,
                    ..Default::default()
                })
                .map_err(|error| D::Error::custom(self.decode.error(error)))?;
        }
        self.inner.deserialize(Funded {
            inner: deserializer,
            decode: self.decode,
            kind: DecodeKind::Borrowed,
        })
    }
}

struct MaterializationSequence<'a, 'b, W: DecodeWork, A> {
    inner: A,
    decode: &'a MaterializationDecode<'b, W>,
    kind: DecodeKind,
    length: usize,
    capacity: usize,
}
impl<'de, W: DecodeWork, A: serde::de::SeqAccess<'de>> serde::de::SeqAccess<'de>
    for MaterializationSequence<'_, '_, W, A>
{
    /// Original error recorded while the serde visitor unwinds.
    type Error = A::Error;
    fn next_element_seed<S: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, Self::Error> {
        use serde::de::Error as _;
        let decode = self.decode;
        let value = self.inner.next_element_seed(Funded {
            inner: seed,
            decode,
            kind: DecodeKind::Item,
        })?;
        if value.is_some() {
            let width = std::mem::size_of::<S::Value>();
            match self.kind {
                DecodeKind::Tree => {
                    let mut entry = decode
                        .work
                        .claim(width as u64, 0)
                        .map_err(|error| Self::Error::custom(decode.error(error)))?;
                    decode
                        .work
                        .absorb(&mut decode.allocation.borrow_mut(), &mut entry)
                        .map_err(|error| Self::Error::custom(decode.error(error)))?;
                }
                DecodeKind::Sequence | DecodeKind::ByteBuffer
                    if width != 0 && self.length == self.capacity =>
                {
                    decode
                        .grow_sequence(width, &mut self.capacity)
                        .map_err(Self::Error::custom)?;
                }
                _ => {}
            }
            self.length += 1;
        }
        Ok(value)
    }
    fn size_hint(&self) -> Option<usize> {
        None
    }
}

struct MaterializationMap<'a, 'b, W: DecodeWork, A> {
    inner: A,
    decode: &'a MaterializationDecode<'b, W>,
    kind: DecodeKind,
    key_layout: std::alloc::Layout,
}
impl<'de, W: DecodeWork, A: serde::de::MapAccess<'de>> serde::de::MapAccess<'de>
    for MaterializationMap<'_, '_, W, A>
{
    /// Original error recorded while the serde visitor unwinds.
    type Error = A::Error;
    fn next_key_seed<S: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<Option<S::Value>, Self::Error> {
        self.key_layout = std::alloc::Layout::new::<S::Value>();
        self.inner.next_key_seed(Funded {
            inner: seed,
            decode: self.decode,
            kind: if self.kind == DecodeKind::Tree {
                DecodeKind::Item
            } else {
                DecodeKind::Borrowed
            },
        })
    }
    fn next_value_seed<S: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<S::Value, Self::Error> {
        use serde::de::Error as _;
        let decode = self.decode;
        let value = self.inner.next_value_seed(Funded {
            inner: seed,
            decode,
            kind: DecodeKind::Borrowed,
        })?;
        if self.kind == DecodeKind::Tree {
            let (layout, _) = self
                .key_layout
                .extend(std::alloc::Layout::new::<S::Value>())
                .map_err(|_| Self::Error::custom("map entry layout overflow"))?;
            let mut entry = decode
                .work
                .claim(layout.pad_to_align().size() as u64, 0)
                .map_err(|error| Self::Error::custom(decode.error(error)))?;
            decode
                .work
                .absorb(&mut decode.allocation.borrow_mut(), &mut entry)
                .map_err(|error| Self::Error::custom(decode.error(error)))?;
        }
        Ok(value)
    }
    fn size_hint(&self) -> Option<usize> {
        None
    }
}

impl<'de, 'a, 'b, W: DecodeWork, A: serde::de::EnumAccess<'de>> serde::de::EnumAccess<'de>
    for Funded<'a, 'b, W, A>
{
    /// Original error recorded while the serde visitor unwinds.
    type Error = A::Error;
    type Variant = Funded<'a, 'b, W, A::Variant>;
    fn variant_seed<S: serde::de::DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> Result<(S::Value, Self::Variant), Self::Error> {
        let (value, inner) = self.inner.variant_seed(Funded {
            inner: seed,
            decode: self.decode,
            kind: DecodeKind::Borrowed,
        })?;
        Ok((
            value,
            Funded {
                inner,
                decode: self.decode,
                kind: DecodeKind::Borrowed,
            },
        ))
    }
}
impl<'de, W: DecodeWork, A: serde::de::VariantAccess<'de>> serde::de::VariantAccess<'de>
    for Funded<'_, '_, W, A>
{
    /// Original error recorded while the serde visitor unwinds.
    type Error = A::Error;
    fn unit_variant(self) -> Result<(), Self::Error> {
        self.inner.unit_variant()
    }
    fn newtype_variant_seed<S: serde::de::DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> Result<S::Value, Self::Error> {
        self.inner.newtype_variant_seed(Funded {
            inner: seed,
            decode: self.decode,
            kind: DecodeKind::Borrowed,
        })
    }
    fn tuple_variant<V: serde::de::Visitor<'de>>(
        self,
        length: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.inner.tuple_variant(
            length,
            Funded {
                inner: visitor,
                decode: self.decode,
                kind: DecodeKind::Borrowed,
            },
        )
    }
    fn struct_variant<V: serde::de::Visitor<'de>>(
        self,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.inner.struct_variant(
            fields,
            Funded {
                inner: visitor,
                decode: self.decode,
                kind: DecodeKind::Borrowed,
            },
        )
    }
}
