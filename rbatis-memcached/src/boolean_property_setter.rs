//! Boolean setter。
//!
//! 对应 Java：`org.mybatis.caches.memcached.BooleanPropertySetter`
//! （位于 `/workspace-github/memcached-cache/src/main/java/org/mybatis/caches/memcached/BooleanPropertySetter.java`）。

#![allow(missing_docs)]

use crate::abstract_property_setter::{PropertySetter, TypedPropertySetter};
use crate::configuration::MemcachedConfiguration;

/// Boolean setter：从 `String` 解析 `bool`。
///
/// 对应 Java: `BooleanPropertySetter#convert(String)`。
pub fn build_boolean_setter(
    property_key: impl Into<String>,
    property_name: impl Into<String>,
    default_value: bool,
    setter: impl Fn(&mut MemcachedConfiguration, bool) + Send + Sync + 'static,
) -> impl PropertySetter {
    TypedPropertySetter::<bool>::new(property_key, property_name, default_value, setter)
}
