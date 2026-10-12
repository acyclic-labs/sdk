/// Tracks serde's owned string copies and sequence capacities before its visitors allocate them.
pub(super) struct MaterializationDecode<'a> {
    pub(super) work: &'a crate::materializer::NativeMaterializationWork,
    pub(super) allocation: std::cell::RefCell<crate::materializer::NativeAllocation>,
    pub(super) failure:
        std::cell::RefCell<Option<crate::materializer::NativeTreeMaterializationError>>,
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

pub(super) struct MaterializationDeserializer<'a, 'b, D> {
    pub(super) inner: D,
    pub(super) decode: &'a MaterializationDecode<'b>,
    pub(super) tree_set: bool,
}

pub(super) struct MaterializationVisitor<'a, 'b, V> {
    pub(super) inner: V,
    pub(super) decode: &'a MaterializationDecode<'b>,
    pub(super) owned_string: bool,
    pub(super) owned_sequence: bool,
    pub(super) tree: bool,
}

macro_rules! materialization_deserialize {
    ($method:ident) => {
        fn $method<V: serde::de::Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
            self.inner.$method(visitor)
        }
    };
    ($method:ident, $string:expr, $sequence:expr $(, $argument:ident : $type:ty)*) => {
        fn $method<V: serde::de::Visitor<'de>>(self, $($argument: $type,)* visitor: V) -> Result<V::Value, Self::Error> {
            self.inner.$method($($argument,)* MaterializationVisitor {
                inner: visitor, decode: self.decode,
                owned_string: $string, owned_sequence: $sequence, tree: self.tree_set && $sequence,
            })
        }
    };
}

impl<'de, D: serde::Deserializer<'de>> serde::Deserializer<'de>
    for MaterializationDeserializer<'_, '_, D>
{
    type Error = D::Error;
    materialization_deserialize!(deserialize_any, false, false);
    materialization_deserialize!(deserialize_bool);
    materialization_deserialize!(deserialize_i8);
    materialization_deserialize!(deserialize_i16);
    materialization_deserialize!(deserialize_i32);
    materialization_deserialize!(deserialize_i64);
    materialization_deserialize!(deserialize_i128);
    materialization_deserialize!(deserialize_u8);
    materialization_deserialize!(deserialize_u16);
    materialization_deserialize!(deserialize_u32);
    materialization_deserialize!(deserialize_u64);
    materialization_deserialize!(deserialize_u128);
    materialization_deserialize!(deserialize_f32);
    materialization_deserialize!(deserialize_f64);
    materialization_deserialize!(deserialize_char);
    materialization_deserialize!(deserialize_str);
    materialization_deserialize!(deserialize_string, true, false);
    materialization_deserialize!(deserialize_bytes);
    materialization_deserialize!(deserialize_byte_buf, true, false);
    materialization_deserialize!(deserialize_option, false, false);
    materialization_deserialize!(deserialize_unit);
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
                tree: name == "CoreStateChildSet",
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
            tree: true,
        })
    }
    materialization_deserialize!(deserialize_struct, false, false, name: &'static str, fields: &'static [&'static str]);
    materialization_deserialize!(deserialize_enum, false, false, name: &'static str, variants: &'static [&'static str]);
    materialization_deserialize!(deserialize_identifier);
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
    materialization_visit!(visit_i64, i64);
    materialization_visit!(visit_u64, u64);
    materialization_visit!(visit_f64, f64);

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
        let copy = crate::WorkCounters {
            bytes_copied: value.len() as u64,
            ..crate::WorkCounters::default()
        };
        if self.owned_string {
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
        if self.owned_string {
            self.decode
                .work
                .spent(copy)
                .map_err(|error| E::custom(self.decode.error(error)))?;
        }
        Ok(output)
    }

    fn visit_borrowed_str<E: serde::de::Error>(self, value: &'de str) -> Result<Self::Value, E> {
        self.visit_str(value)
    }

    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
        self.decode
            .allocation
            .borrow_mut()
            .grow(value.capacity() as u64)
            .map_err(|error| E::custom(self.decode.error(error)))?;
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
            tree_set: false,
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
                tree_set: self.tree,
            })
    }
    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
        self.inner.visit_seq(MaterializationSequence {
            inner: access,
            decode: self.decode,
            owned: self.owned_sequence,
            tree: self.tree,
            length: 0,
            capacity: 0,
        })
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
        self.inner.visit_map(MaterializationMap {
            inner: access,
            decode: self.decode,
            owned: self.tree,
            key_layout: std::alloc::Layout::new::<()>(),
        })
    }
    fn visit_enum<A: serde::de::EnumAccess<'de>>(self, access: A) -> Result<Self::Value, A::Error> {
        self.inner.visit_enum(MaterializationVariant {
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
                .work
                .charge(crate::WorkCounters {
                    items_examined: 1,
                    ..crate::WorkCounters::default()
                })
                .map_err(|error| D::Error::custom(self.decode.error(error)))?;
        }
        self.inner.deserialize(MaterializationDeserializer {
            inner: deserializer,
            decode: self.decode,
            tree_set: false,
        })
    }
}

pub(super) struct MaterializationSequence<'a, 'b, A> {
    pub(super) inner: A,
    pub(super) decode: &'a MaterializationDecode<'b>,
    pub(super) owned: bool,
    pub(super) tree: bool,
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
            if self.owned && self.tree {
                let mut entry = self
                    .decode
                    .work
                    .claim(width as u64, 0, crate::WorkCounters::UNCHARGED)
                    .map_err(|error| Self::Error::custom(self.decode.error(error)))?;
                self.decode
                    .allocation
                    .borrow_mut()
                    .absorb(&mut entry)
                    .map_err(|error| Self::Error::custom(self.decode.error(error)))?;
            } else if self.owned && width != 0 && self.length == self.capacity {
                self.decode
                    .grow_sequence(width, &mut self.capacity)
                    .map_err(Self::Error::custom)?;
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
            let (layout, _) = self
                .key_layout
                .extend(value_layout)
                .map_err(|_| Self::Error::custom("map entry layout overflow"))?;
            let mut entry = self
                .decode
                .work
                .claim(
                    layout.pad_to_align().size() as u64,
                    0,
                    crate::WorkCounters::UNCHARGED,
                )
                .map_err(|error| Self::Error::custom(self.decode.error(error)))?;
            self.decode
                .allocation
                .borrow_mut()
                .absorb(&mut entry)
                .map_err(|error| Self::Error::custom(self.decode.error(error)))?;
        }
        Ok(value)
    }
    fn size_hint(&self) -> Option<usize> {
        None
    }
}

impl<'de, 'a, 'b, A: serde::de::EnumAccess<'de>> serde::de::EnumAccess<'de>
    for MaterializationVariant<'a, 'b, A>
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
                tree: false,
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
                tree: false,
            },
        )
    }
}
