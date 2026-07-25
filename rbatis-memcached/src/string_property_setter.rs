//! String setter。
//!
//! 对应 Java：`org.mybatis.caches.memcached.StringPropertySetter`
//! （位于 `/workspace-github/memcached-cache/src/main/java/org/mybatis/caches/memcached/StringPropertySetter.java`）。

#![allow(missing_docs)]

use crate::abstract_property_setter::{PropertySetter, TypedPropertySetter};
use crate::configuration::MemcachedConfiguration;

/// String setter：把字符串原样写入对应字段。
///
/// 对应 Java: `StringPropertySetter#convert(String)`（直接返回）。
pub fn build_string_setter(
    property_key: impl Into<String>,
    property_name: impl Into<String>,
    default_value: String,
    setter: impl Fn(&mut MemcachedConfiguration, String) + Send + Sync + 'static,
) -> impl PropertySetter {
    TypedPropertySetter::<String>::new(property_key, property_name, default_value, setter)
}
