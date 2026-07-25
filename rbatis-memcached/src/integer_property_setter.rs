//! Integer setter。
//!
//! 对应 Java：`org.mybatis.caches.memcached.IntegerPropertySetter`
//! （位于 `/workspace-github/memcached-cache/src/main/java/org/mybatis/caches/memcached/IntegerPropertySetter.java`）。

use crate::abstract_property_setter::{PropertySetter, TypedPropertySetter};
use crate::configuration::MemcachedConfiguration;

/// Integer setter：从 `String` 解析 `i32`。
///
/// 对应 Java: `IntegerPropertySetter#convert(String)`。
pub fn build_integer_setter(
    property_key: impl Into<String>,
    property_name: impl Into<String>,
    default_value: i32,
    setter: impl Fn(&mut MemcachedConfiguration, i32) + Send + Sync + 'static,
) -> impl PropertySetter {
    TypedPropertySetter::<i32>::new(property_key, property_name, default_value, setter)
}
