//! Redis backend 配置。
//!
//! 对应 Java：`org.mybatis.caches.redis.RedisConfig`
//! （位于 `/workspace-github/redis-cache/src/main/java/org/mybatis/caches/redis/RedisConfig.java`）。
//!
//! Java 侧 `RedisConfig` 继承 `JedisPoolConfig`，提供 host/port/timeout/
//! password/database/serializer 等配置项。本 crate 用 struct 表达同样
//! 的配置面，并保留 `key_prefix` / `operation_timeout` 等由
//! `rbatis-cache` SPI 需要的字段。

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

/// Redis backend 的完整配置（连接配置 + backend 行为配置）。
///
/// Java 侧 `RedisConfig` 通过继承 `JedisPoolConfig` 获得连接池参数；
/// 本 crate 拆为两层：[`RedisConfig`] 承载 Java 同名类的字段面，本
/// 类型承载 backend 行为面（key 前缀、熔断），两者组合使用。
///
/// 熔断参数属于 **Rust 侧增强，无 Java 对应**。
#[derive(Debug, Clone)]
pub struct RedisCacheConfig {
    /// 内部连接配置。
    pub redis: RedisConfig,
    /// 前缀：拼到每条数据 key 与 generation key 之前。
    pub key_prefix: String,
    /// 单次操作超时（覆盖 [`RedisConfig::operation_timeout`]）。
    pub operation_timeout: Duration,
    /// 连续失败次数达到此值打开熔断（Rust 侧增强）。
    pub circuit_failure_threshold: u32,
    /// 熔断冷却时间（Rust 侧增强）。
    pub circuit_cooldown: Duration,
}

impl RedisCacheConfig {
    /// 由 [`RedisConfig`] 派生默认 backend 配置。
    pub fn from_redis(redis: RedisConfig) -> Self {
        Self {
            operation_timeout: redis.operation_timeout,
            redis,
            key_prefix: "rbatis:cache".to_owned(),
            circuit_failure_threshold: 3,
            circuit_cooldown: Duration::from_secs(5),
        }
    }
}
