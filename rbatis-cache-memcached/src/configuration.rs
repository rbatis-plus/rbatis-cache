//! Memcached 配置对象。
//!
//! 对应 Java：`org.mybatis.caches.memcached.MemcachedConfiguration`
//! （位于 `/workspace-github/memcached-cache/src/main/java/org/mybatis/caches/memcached/MemcachedConfiguration.java`）。
//!
//! ## 字段映射
//!
//! | Java 字段 | Rust 字段 |
//! |---|---|
//! | `servers` | `servers` |
//! | `connectionfactory` | `connection_factory` |
//! | `keyprefix` | `key_prefix` |
//! | `expiration` | `expiration` |
//! | `timeout` | `operation_timeout` |
//! | `timeoutunit` | （由 setter 隐式决定） |
//! | `asyncget` | `async_get` |
//! | `compression` | `compression` |
//! | `sasl` | `sasl` |
//! | `username` | `username` |
//! | `password` | `password` |

use std::time::Duration;

/// ConnectionFactory 协议选择。
///
/// 对应 Java: spymemcached `ConnectionFactory` 的多种实现。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConnectionFactoryKind {
    /// 二进制协议（默认）。
    #[default]
    Binary,
    /// 文本协议。
    Text,
    /// SASL 认证。
    Sasl,
}

/// Memcached 配置。
#[derive(Debug, Clone)]
pub struct MemcachedConfiguration {
    /// 服务地址列表（host, port）。
    pub servers: Vec<(String, u16)>,
    /// 协议选择。
    pub connection_factory: ConnectionFactoryKind,
    /// key 前缀。
    pub key_prefix: String,
    /// 默认 TTL（秒）。
    pub expiration: Duration,
    /// 操作超时。
    pub operation_timeout: Duration,
    /// 是否启用 async get。
    pub async_get: bool,
    /// 是否启用压缩 transcoder。
    pub compression: bool,
    /// 是否启用 SASL。
    pub sasl: bool,
    /// SASL 用户名。
    pub username: Option<String>,
    /// SASL 密码。
    pub password: Option<String>,
}

impl Default for MemcachedConfiguration {
    fn default() -> Self {
        Self {
            servers: vec![("127.0.0.1".to_owned(), 11211)],
            connection_factory: ConnectionFactoryKind::Binary,
            key_prefix: "rbatis:cache".to_owned(),
            expiration: Duration::from_secs(60 * 60 * 24),
            operation_timeout: Duration::from_secs(1),
            async_get: false,
            compression: false,
            sasl: false,
            username: None,
            password: None,
        }
    }
}
