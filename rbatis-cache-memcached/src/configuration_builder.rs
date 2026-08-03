//! 配置加载器：从 properties 文件解析出 [`MemcachedConfiguration`]。
//!
//! 对应 Java：`org.mybatis.caches.memcached.MemcachedConfigurationBuilder`
//! （位于 `/workspace-github/memcached-cache/src/main/java/org/mybatis/caches/memcached/MemcachedConfigurationBuilder.java`）。
//!
//! Java 侧用 JavaBean introspection + 各 `*PropertySetter` 把字符串转换
//! 成强类型字段；本 crate 用集中式 [`apply_kv`] 维护相同字段表。

use std::time::Duration;

use crate::configuration::{ConnectionFactoryKind, MemcachedConfiguration};
use crate::inet_socket_address_list_property_setter::parse_address_list;

/// 配置加载器。
///
/// 对应 `MemcachedConfigurationBuilder`（单例）。
pub struct MemcachedConfigurationBuilder;

impl MemcachedConfigurationBuilder {
    /// 读取环境变量 `MEMCACHED_PROPERTIES` 指向的文件；缺省返回默认配置。
    pub fn from_env() -> MemcachedConfiguration {
        match std::env::var("MEMCACHED_PROPERTIES").ok() {
            Some(path) => Self::from_file(&path),
            None => MemcachedConfiguration::default(),
        }
    }

    /// 从指定文件加载。
    pub fn from_file(path: &str) -> MemcachedConfiguration {
        let text = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => return MemcachedConfiguration::default(),
        };
        Self::parse(&text)
    }

    /// 解析 properties 文本。
    pub fn parse(text: &str) -> MemcachedConfiguration {
        let mut config = MemcachedConfiguration::default();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((key, value)) = line.split_once('=') {
                apply_kv(&mut config, key.trim(), value.trim());
            }
        }
        config
    }
}

/// 单条 `key=value` 写入 [`MemcachedConfiguration`]。
///
/// 与 Java 侧 setter 注册表等价的内联实现。
fn apply_kv(config: &mut MemcachedConfiguration, key: &str, value: &str) {
    match key {
        "org.mybatis.caches.memcached.servers" | "memcached.servers" => {
            config.servers = parse_address_list(value);
        }
        "org.mybatis.caches.memcached.connectionfactory" | "memcached.connectionfactory" => {
            config.connection_factory = match value.to_ascii_lowercase().as_str() {
                "binary" => ConnectionFactoryKind::Binary,
                "text" => ConnectionFactoryKind::Text,
                "sasl" => ConnectionFactoryKind::Sasl,
                _ => return,
            };
        }
        "org.mybatis.caches.memcached.keyprefix" | "memcached.keyprefix" => {
            config.key_prefix = value.to_owned();
        }
        "org.mybatis.caches.memcached.expiration" | "memcached.expiration" => {
            if let Ok(secs) = value.parse::<u64>() {
                config.expiration = Duration::from_secs(secs);
            }
        }
        "org.mybatis.caches.memcached.timeout" | "memcached.timeout" => {
            if let Ok(secs) = value.parse::<u64>() {
                config.operation_timeout = Duration::from_secs(secs);
            }
        }
        "org.mybatis.caches.memcached.asyncget" | "memcached.asyncget" => {
            config.async_get = value.eq_ignore_ascii_case("true");
        }
        "org.mybatis.caches.memcached.compression" | "memcached.compression" => {
            config.compression = value.eq_ignore_ascii_case("true");
        }
        "org.mybatis.caches.memcached.sasl" | "memcached.sasl" => {
            config.sasl = value.eq_ignore_ascii_case("true");
        }
        "org.mybatis.caches.memcached.username" | "memcached.username" => {
            config.username = Some(value.to_owned());
        }
        "org.mybatis.caches.memcached.password" | "memcached.password" => {
            config.password = Some(value.to_owned());
        }
        _ => {}
    }
}
