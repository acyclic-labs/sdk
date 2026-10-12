/// Tracks serde's owned string copies and sequence capacities before its visitors allocate them.
pub(super) struct MaterializationDecode<'a> {
    pub(super) work: &'a crate::materializer::NativeMaterializationWork,
    pub(super) allocation: std::cell::RefCell<crate::materializer::NativeAllocation>,
    pub(super) failure:
        std::cell::RefCell<Option<crate::materializer::NativeTreeMaterializationError>>,
}

impl MaterializationDecode<'_> {
    pub(super) fn charge(&self, delta: crate::WorkCounters) -> Result<(), &'static str> {
        self.work.charge(delta).map_err(|error| {
            *self.failure.borrow_mut() = Some(error);
            "materialization decode work boundary"
        })
    }

    pub(super) fn admit(&self, delta: crate::WorkCounters) -> Result<(), &'static str> {
        self.work.admit(delta).map_err(|error| {
            *self.failure.borrow_mut() = Some(error);
            "decode work admission failed"
        })
    }

    pub(super) fn spent(&self, delta: crate::WorkCounters) -> Result<(), &'static str> {
        self.work.spent(delta).map_err(|error| {
            *self.failure.borrow_mut() = Some(error);
            "decode work accounting failed"
        })
    }

    pub(super) fn grow(&self, bytes: u64) -> Result<(), &'static str> {
        self.allocation.borrow_mut().grow(bytes).map_err(|error| {
            *self.failure.borrow_mut() = Some(error);
            "materialization decode allocation boundary"
        })
    }
}

pub(super) struct MaterializationDeserializer<'a, 'b, D> {
    pub(super) inner: D,
    pub(super) decode: &'a MaterializationDecode<'b>,
    pub(super) flat_map: bool,
}

pub(super) struct MaterializationVisitor<'a, 'b, V> {
    pub(super) inner: V,
    pub(super) decode: &'a MaterializationDecode<'b>,
    pub(super) owned_string: bool,
    pub(super) owned_sequence: bool,
    pub(super) owned_map: bool,
}

macro_rules! materialization_deserialize {
    ($method:ident, $string:expr, $sequence:expr $(, $argument:ident : $type:ty)*) => {
        fn $method<V: serde::de::Visitor<'de>>(self, $($argument: $type,)* visitor: V) -> Result<V::Value, Self::Error> {
            self.inner.$method($($argument,)* MaterializationVisitor {
                inner: visitor, decode: self.decode,
                owned_string: $string, owned_sequence: $sequence, owned_map: false,
            })
        }
    };
}

impl<'de, D: serde::Deserializer<'de>> serde::Deserializer<'de>
    for MaterializationDeserializer<'_, '_, D>
{
    type Error = D::Error;
    materialization_deserialize!(deserialize_any, false, false);
    materialization_deserialize!(deserialize_bool, false, false);
    materialization_deserialize!(deserialize_i8, false, false);
    materialization_deserialize!(deserialize_i16, false, false);
    materialization_deserialize!(deserialize_i32, false, false);
    materialization_deserialize!(deserialize_i64, false, false);
    materialization_deserialize!(deserialize_i128, false, false);
    materialization_deserialize!(deserialize_u8, false, false);
    materialization_deserialize!(deserialize_u16, false, false);
    materialization_deserialize!(deserialize_u32, false, false);
    materialization_deserialize!(deserialize_u64, false, false);
    materialization_deserialize!(deserialize_u128, false, false);
    materialization_deserialize!(deserialize_f32, false, false);
    materialization_deserialize!(deserialize_f64, false, false);
    materialization_deserialize!(deserialize_char, false, false);
    materialization_deserialize!(deserialize_str, false, false);
    materialization_deserialize!(deserialize_string, true, false);
    materialization_deserialize!(deserialize_bytes, false, false);
    materialization_deserialize!(deserialize_byte_buf, true, false);
    materialization_deserialize!(deserialize_option, false, false);
    materialization_deserialize!(deserialize_unit, false, false);
    materialization_deserialize!(deserialize_unit_struct, false, false, name: &'static str);
    fn deserialize_newtype_struct<V: serde::de::Visitor<'de>>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_newtype_struct(
            name,
            MaterializationVisitor {
                inner: visitor,
                decode: self.decode,
                owned_string: false,
                owned_sequence: false,
                owned_map: name == "CoreStateFlatMap",
            },
        )
    }
    materialization_deserialize!(deserialize_seq, false, true);
    materialization_deserialize!(deserialize_tuple, false, false, length: usize);
    materialization_deserialize!(deserialize_tuple_struct, false, false, name: &'static str, length: usize);
    fn deserialize_map<V: serde::de::Visitor<'de>>(
        self,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_map(MaterializationVisitor {
            inner: visitor,
            decode: self.decode,
            owned_string: false,
            owned_sequence: false,
            owned_map: self.flat_map,
        })
    }
    materialization_deserialize!(deserialize_struct, false, false, name: &'static str, fields: &'static [&'static str]);
    materialization_deserialize!(deserialize_enum, false, false, name: &'static str, variants: &'static [&'static str]);
    materialization_deserialize!(deserialize_identifier, false, false);
    fn deserialize_ignored_any<V: serde::de::Visitor<'de>>(
        self,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.inner.deserialize_ignored_any(visitor)
    }
    fn is_human_readable(&self) -> bool {
        self.inner.is_human_readable()
    }
}

macro_rules! materialization_visit {
    ($method:ident, $type:ty) => {
        fn $method<E: serde::de::Error>(self, value: $type) -> Result<Self::Value, E> {
            self.inner.$method(value)
        }
    };
}

impl<'de, V: serde::de::Visitor<'de>> serde::de::Visitor<'de>
    for MaterializationVisitor<'_, '_, V>
{
    type Value = V::Value;
    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.expecting(formatter)
    }
    materialization_visit!(visit_bool, bool);
    materialization_visit!(visit_i8, i8);
    materialization_visit!(visit_i16, i16);
    materialization_visit!(visit_i32, i32);
    materialization_visit!(visit_i64, i64);
    materialization_visit!(visit_i128, i128);
    materialization_visit!(visit_u8, u8);
    materialization_visit!(visit_u16, u16);
    materialization_visit!(visit_u32, u32);
    materialization_visit!(visit_u64, u64);
    materialization_visit!(visit_u128, u128);
    materialization_visit!(visit_f32, f32);
    materialization_visit!(visit_f64, f64);
    materialization_visit!(visit_char, char);

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
        let copy = crate::WorkCounters {
            bytes_copied: value.len() as u64,
            ..crate::WorkCounters::default()
        };
        if self.owned_string {
            self.decode.admit(copy).map_err(E::custom)?;
            self.decode.grow(value.len() as u64).map_err(E::custom)?;
        }
        let output = self.inner.visit_str(value)?;
        if self.owned_string {
            self.decode.spent(copy).map_err(E::custom)?;
        }
        Ok(output)
    }

    fn visit_borrowed_str<E: serde::de::Error>(self, value: &'de str) -> Result<Self::Value, E> {
        self.visit_str(value)
    }

    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
        self.decode
            .grow(value.capacity() as u64)
            .map_err(E::custom)?;
        self.inner.visit_string(value)
    }

    fn visit_bytes<E: serde::de::Error>(self, value: &[u8]) -> Result<Self::Value, E> {
        self.inner.visit_bytes(value)
    }
    fn visit_borrowed_bytes<E: serde::de::Error>(self, value: &'de [u8]) -> Result<Self::Value, E> {
        self.inner.visit_borrowed_bytes(value)
    }
    fn visit_byte_buf<E: serde::de::Error>(self, value: Vec<u8>) -> Result<Self::Value, E> {
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
        self.inner.visit_some(MaterializationDeserializer {
            inner: deserializer,
            decode: self.decode,
            flat_map: false,
        })
    }
    fn visit_newtype_struct<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        self.inner
            .visit_newtype_struct(MaterializationDeserializer {
                inner: deserializer,
                decode: self.decode,
                flat_map: self.owned_map,
            })
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
        self.inner.visit_seq(MaterializationSequence {
            inner: access,
            decode: self.decode,
            owned: self.owned_sequence,
            length: 0,
            capacity: 0,
        })
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
        self.inner.visit_map(MaterializationMap {
            inner: access,
            decode: self.decode,
            owned: self.owned_map,
            key_layout: std::alloc::Layout::new::<()>(),
            length: 0,
            capacity: 0,
        })
    }
    fn visit_enum<A: serde::de::EnumAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
        self.inner.visit_enum(MaterializationEnum {
            inner: access,
            decode: self.decode,
        })
    }
}

pub(super) struct MaterializationSeed<'a, 'b, S> {
    inner: S,
    decode: &'a MaterializationDecode<'b>,
    item: bool,
}
impl<'de, S: serde::de::DeserializeSeed<'de>> serde::de::DeserializeSeed<'de>
    for MaterializationSeed<'_, '_, S>
{
    type Value = S::Value;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> Result<Self::Value, D::Error> {
        use serde::de::Error as _;
        if self.item {
            self.decode
                .charge(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::default()
                })
                .map_err(D::Error::custom)?;
        }
        self.inner.deserialize(MaterializationDeserializer {
            inner: deserializer,
            decode: self.decode,
            flat_map: false,
        })
    }
}

pub(super) struct MaterializationSequence<'a, 'b, A> {
    pub(super) inner: A,
    pub(super) decode: &'a MaterializationDecode<'b>,
    pub(super) owned: bool,
    pub(super) length: usize,
    pub(super) capacity: usize,
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
        let value = self.inner.next_element_seed(MaterializationSeed {
            inner: seed,
            decode: self.decode,
            item: true,
        })?;
        if value.is_some() {
            let width = std::mem::size_of::<S::Value>();
            if self.owned && width != 0 && self.length == self.capacity {
                // With no size hint serde's Vec visitor starts at zero; these
                // are Vec's actual minimum and amortized growth capacities.
                let minimum = if width == 1 {
                    8
                } else if width <= 1024 {
                    4
                } else {
                    1
                };
                let next = if self.capacity == 0 {
                    minimum
                } else {
                    self.capacity
                        .checked_mul(2)
                        .ok_or_else(|| Self::Error::custom("journal sequence capacity overflow"))?
                };
                let bytes = next
                    .checked_sub(self.capacity)
                    .and_then(|count| count.checked_mul(width))
                    .ok_or_else(|| Self::Error::custom("journal sequence allocation overflow"))?;
                self.decode
                    .grow(bytes as u64)
                    .map_err(Self::Error::custom)?;
                self.capacity = next;
            }
            self.length += 1;
        }
        Ok(value)
    }
    fn size_hint(&self) -> Option<usize> {
        None
    }
}

pub(super) struct MaterializationMap<'a, 'b, A> {
    pub(super) inner: A,
    pub(super) decode: &'a MaterializationDecode<'b>,
    pub(super) owned: bool,
    pub(super) key_layout: std::alloc::Layout,
    pub(super) length: usize,
    pub(super) capacity: usize,
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
        self.inner.next_key_seed(MaterializationSeed {
            inner: seed,
            decode: self.decode,
            item: self.owned,
        })
    }
    fn next_value_seed<S: serde::de::DeserializeSeed<'de>>(
        &mut self,
        seed: S,
    ) -> Result<S::Value, Self::Error> {
        use serde::de::Error as _;
        let value = self.inner.next_value_seed(MaterializationSeed {
            inner: seed,
            decode: self.decode,
            item: false,
        })?;
        if self.owned {
            let value_layout = std::alloc::Layout::new::<S::Value>();
            let alignment = self.key_layout.align().max(value_layout.align());
            let size = self
                .key_layout
                .size()
                .checked_add(value_layout.size())
                .and_then(|bytes| bytes.checked_add(alignment - 1))
                .map(|bytes| bytes & !(alignment - 1))
                .ok_or_else(|| Self::Error::custom("flat map entry layout overflow"))?;
            if size != 0 && self.length == self.capacity {
                let minimum = if size == 1 {
                    8
                } else if size <= 1024 {
                    4
                } else {
                    1
                };
                let next = if self.capacity == 0 {
                    minimum
                } else {
                    self.capacity
                        .checked_mul(2)
                        .ok_or_else(|| Self::Error::custom("flat map capacity overflow"))?
                };
                let bytes = (next - self.capacity)
                    .checked_mul(size)
                    .ok_or_else(|| Self::Error::custom("flat map allocation overflow"))?;
                self.decode
                    .grow(bytes as u64)
                    .map_err(Self::Error::custom)?;
                self.capacity = next;
            }
            self.length += 1;
        }
        Ok(value)
    }
    fn size_hint(&self) -> Option<usize> {
        None
    }
}

pub(super) struct MaterializationEnum<'a, 'b, A> {
    inner: A,
    decode: &'a MaterializationDecode<'b>,
}
impl<'de, 'a, 'b, A: serde::de::EnumAccess<'de>> serde::de::EnumAccess<'de>
    for MaterializationEnum<'a, 'b, A>
{
    type Error = A::Error;
    type Variant = MaterializationVariant<'a, 'b, A::Variant>;
    fn variant_seed<S: serde::de::DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> Result<(S::Value, Self::Variant), Self::Error> {
        let (value, variant) = self.inner.variant_seed(MaterializationSeed {
            inner: seed,
            decode: self.decode,
            item: false,
        })?;
        Ok((
            value,
            MaterializationVariant {
                inner: variant,
                decode: self.decode,
            },
        ))
    }
}

pub(super) struct MaterializationVariant<'a, 'b, A> {
    inner: A,
    decode: &'a MaterializationDecode<'b>,
}

impl<'de, A: serde::de::VariantAccess<'de>> serde::de::VariantAccess<'de>
    for MaterializationVariant<'_, '_, A>
{
    type Error = A::Error;
    fn unit_variant(self) -> Result<(), Self::Error> {
        self.inner.unit_variant()
    }
    fn newtype_variant_seed<S: serde::de::DeserializeSeed<'de>>(
        self,
        seed: S,
    ) -> Result<S::Value, Self::Error> {
        self.inner.newtype_variant_seed(MaterializationSeed {
            inner: seed,
            decode: self.decode,
            item: false,
        })
    }
    fn tuple_variant<V: serde::de::Visitor<'de>>(
        self,
        length: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error> {
        self.inner.tuple_variant(
            length,
            MaterializationVisitor {
                inner: visitor,
                decode: self.decode,
                owned_string: false,
                owned_sequence: false,
                owned_map: false,
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
            MaterializationVisitor {
                inner: visitor,
                decode: self.decode,
                owned_string: false,
                owned_sequence: false,
                owned_map: false,
            },
        )
    }
}
