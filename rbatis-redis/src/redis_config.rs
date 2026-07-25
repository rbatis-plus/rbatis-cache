//! Redis backend 配置。
//!
//! 对应 Java：`org.mybatis.caches.redis.RedisConfig`
//! （位于 `/workspace-github/redis-cache/src/main/java/org/mybatis/caches/redis/RedisConfig.java`）。
//!
//! Java 侧 `RedisConfig` 继承 `JedisPoolConfig`，提供 host/port/timeout/
//! password/database/serializer 等配置项。本 crate 用 struct 表达同样
//! 的配置面，并保留 `key_prefix` / `operation_timeout` 等由
//! `rbatis-cache` SPI 需要的字段。

#![allow(missing_docs)]

use std::time::Duration;

/// Redis 单节点 URL。
///
/// 对应 `RedisConfig#host` + `RedisConfig#port`，合并为 `redis://` URL。
pub type RedisUrl = String;

/// Redis backend 配置。
///
/// ## 字段对照
///
/// | Java 字段 | 本字段 |
/// |---|---|
/// | `host` + `port` | `url`（合并） |
/// | `connectionTimeout` | `connection_timeout` |
/// | `soTimeout` | `operation_timeout` |
/// | `password` | `password` |
/// | `database` | `database` |
/// | `clientName` | `client_name` |
/// | `serializer` | `serializer`（`"jdk"` / `"kryo"`，本 crate 留口但内部固定 MessagePack） |
/// | `ssl` | `ssl` |
#[derive(Debug, Clone)]
pub struct RedisConfig {
    /// 单节点 URL，形如 `redis://127.0.0.1:6379/0`。
    pub url: RedisUrl,
    /// 鉴权密码。
    pub password: Option<String>,
    /// 选定的数据库索引。
    pub database: u8,
    /// 连接客户端名（用于在 `CLIENT LIST` 中可见）。
    pub client_name: Option<String>,
    /// 建连超时。
    pub connection_timeout: Duration,
    /// 单次操作超时（也用作 ReadWriteLock 等价物的熔断阈值）。
    pub operation_timeout: Duration,
    /// 是否启用 SSL。
    pub ssl: bool,
    /// 序列化器名称（"jdk" / "kryo"）。
    pub serializer: String,
}

impl RedisConfig {
    /// 单机默认配置：localhost:6379/0，无密码。
    ///
    /// 对应 `RedisConfigurationBuilder#parse` 拿不到配置项时的回退值。
    pub fn standalone() -> Self {
        Self {
            url: "redis://127.0.0.1:6379/0".to_owned(),
            password: None,
            database: 0,
            client_name: None,
            connection_timeout: Duration::from_secs(2),
            operation_timeout: Duration::from_secs(2),
            ssl: false,
            serializer: "jdk".to_owned(),
        }
    }

    /// 设置 URL。
    pub fn with_url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }

    /// 设置密码。
    pub fn with_password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// 设置数据库索引。
    pub fn with_database(mut self, database: u8) -> Self {
        self.database = database;
        self
    }
}
