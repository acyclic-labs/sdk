/// Admits owned serde data before the destination visitor allocates it.
pub(super) struct MaterializationDecode<'a> {
    pub(super) work: &'a crate::NativeMaterializationWork,
    pub(super) allocation: std::cell::RefCell<crate::NativeAllocation>,
    pub(super) failure: std::cell::RefCell<Option<crate::NativeTreeMaterializationError>>,
}

impl MaterializationDecode<'_> {
    fn error(&self, error: crate::NativeTreeMaterializationError) -> &'static str {
        *self.failure.borrow_mut() = Some(error);
        "materialization decode failed"
    }

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
        self.allocation
            .borrow_mut()
            .grow(bytes as u64)
            .map_err(|error| self.error(error))?;
        *capacity = next;
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq)]
pub(super) enum DecodeKind {
    Borrowed,
    String,
    Sequence,
    Tree,
    Item,
}

pub(super) struct Funded<'a, 'b, T> {
    pub(super) inner: T,
    pub(super) decode: &'a MaterializationDecode<'b>,
    pub(super) kind: DecodeKind,
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

impl<'de, D: serde::Deserializer<'de>> serde::Deserializer<'de> for Funded<'_, '_, D> {
    type Error = D::Error;
    deserialize!(deserialize_any, DecodeKind::Borrowed);
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
    deserialize!(deserialize_byte_buf, DecodeKind::String);
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

impl<'de, V: serde::de::Visitor<'de>> serde::de::Visitor<'de> for Funded<'_, '_, V> {
    type Value = V::Value;
    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.expecting(formatter)
    }
    visit!(visit_bool, bool);
    visit!(visit_i64, i64);
    visit!(visit_u64, u64);
    visit!(visit_f64, f64);
    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
        let copy = crate::WorkCounters {
            bytes_copied: value.len() as u64,
            ..Default::default()
        };
        if self.kind == DecodeKind::String {
            self.decode
                .work
                .admit(copy)
                .map_err(|error| E::custom(self.decode.error(error)))?;
            self.decode
                .allocation
                .borrow_mut()
                .grow(value.len() as u64)
                .map_err(|error| E::custom(self.decode.error(error)))?;
        }
        let output = self.inner.visit_str(value)?;
        if self.kind == DecodeKind::String {
            self.decode
                .work
                .spent(copy)
                .map_err(|error| E::custom(self.decode.error(error)))?;
        }
        Ok(output)
    }
    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
        self.decode
            .allocation
            .borrow_mut()
            .grow(value.capacity() as u64)
            .map_err(|error| E::custom(self.decode.error(error)))?;
        self.inner.visit_string(value)
    }
    visit!(visit_bytes, &[u8]);
    visit!(visit_borrowed_bytes, &'de [u8]);
    visit!(visit_byte_buf, Vec<u8>);
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

impl<'de, S: serde::de::DeserializeSeed<'de>> serde::de::DeserializeSeed<'de>
    for Funded<'_, '_, S>
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
                .charge(crate::WorkCounters {
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

struct MaterializationSequence<'a, 'b, A> {
    inner: A,
    decode: &'a MaterializationDecode<'b>,
    kind: DecodeKind,
    length: usize,
    capacity: usize,
}
impl<'de, A: serde::de::SeqAccess<'de>> serde::de::SeqAccess<'de>
    for MaterializationSequence<'_, '_, A>
{
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
                        .claim(width as u64, 0, crate::WorkCounters::UNCHARGED)
                        .map_err(|error| Self::Error::custom(decode.error(error)))?;
                    decode
                        .allocation
                        .borrow_mut()
                        .absorb(&mut entry)
                        .map_err(|error| Self::Error::custom(decode.error(error)))?;
                }
                DecodeKind::Sequence if width != 0 && self.length == self.capacity => {
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

struct MaterializationMap<'a, 'b, A> {
    inner: A,
    decode: &'a MaterializationDecode<'b>,
    kind: DecodeKind,
    key_layout: std::alloc::Layout,
}
impl<'de, A: serde::de::MapAccess<'de>> serde::de::MapAccess<'de>
    for MaterializationMap<'_, '_, A>
{
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
                .claim(
                    layout.pad_to_align().size() as u64,
                    0,
                    crate::WorkCounters::UNCHARGED,
                )
                .map_err(|error| Self::Error::custom(decode.error(error)))?;
            decode
                .allocation
                .borrow_mut()
                .absorb(&mut entry)
                .map_err(|error| Self::Error::custom(decode.error(error)))?;
        }
        Ok(value)
    }
    fn size_hint(&self) -> Option<usize> {
        None
    }
}

impl<'de, 'a, 'b, A: serde::de::EnumAccess<'de>> serde::de::EnumAccess<'de> for Funded<'a, 'b, A> {
    type Error = A::Error;
    type Variant = Funded<'a, 'b, A::Variant>;
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
impl<'de, A: serde::de::VariantAccess<'de>> serde::de::VariantAccess<'de> for Funded<'_, '_, A> {
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
