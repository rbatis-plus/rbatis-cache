//! 时间单位 setter。
//!
//! 对应 Java：`org.mybatis.caches.memcached.TimeUnitSetter`
//! （位于 `/workspace-github/memcached-cache/src/main/java/org/mybatis/caches/memcached/TimeUnitSetter.java`）。
//!
//! Java 侧用 `java.util.concurrent.TimeUnit` 解析时长字符串并写入
//! `MemcachedConfiguration`；本 crate 用 [`std::time::Duration::from_secs`]
//! 承担该语义（最小可用实现）。

#![allow(missing_docs)]

use std::time::Duration;

use crate::abstract_property_setter::{PropertySetter, TypedPropertySetter};
use crate::configuration::MemcachedConfiguration;

/// TimeUnit setter：把整数解析为 [`Duration`]。
///
/// 对应 Java: `TimeUnitSetter#convert(String)` + `TimeUnit#parse`。
pub fn build_time_unit_setter(
    property_key: impl Into<String>,
    property_name: impl Into<String>,
    default_value: Duration,
    setter: impl Fn(&mut MemcachedConfiguration, Duration) + Send + Sync + 'static,
) -> impl PropertySetter {
    let default_secs = default_value.as_secs();
    TypedPropertySetter::<u64>::new(
        property_key,
        property_name,
        default_secs,
        move |cfg, secs| {
            setter(cfg, Duration::from_secs(secs));
        },
    )
}
