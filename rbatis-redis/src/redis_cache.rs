//! Redis 分布式 backend。
//!
//! 对应 Java：`org.mybatis.caches.redis.RedisCache`
//! （位于 `/workspace-github/redis-cache/src/main/java/org/mybatis/caches/redis/RedisCache.java`）。
//!
//! | Java 字段 / 方法 | Rust 对应 |
//! |---|---|
//! | `Cache#getId()` | [`RedisCacheBackend::name`] |
//! | `putObject(key, value)` | [`CacheBackend::put`] |
//! | `getObject(key)` | [`CacheBackend::get`] |
//! | `clear()` | [`CacheBackend::bump_generation`]（按 namespace） |
//!
//! ## Rust 侧增强（无 Java 对应）
//!
//! - 字节级 operation timeout 与熔断：Java 侧 `RedisConfig` 透传 Jedis 超时；
//!   本 crate 用 `tokio::time::timeout` + 失败计数做轻量熔断。
//! - `key_prefix` 与 generation 路由在 Rust 侧显式设计，Java 侧由 Jedis
//!   key 字符串拼接。
//!
//! 注意：当前实现是**纯 SPI 实现**，未启用 cluster/sentinel/Pub-Sub
//! 失效广播——后续可按 [`RedisCacheConfig`] 扩展点叠加。

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::future::BoxFuture;
use rbatis_cache::{CacheBackend, CacheError, Result};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client};

use crate::redis_config::RedisCacheConfig;

/// 指标快照。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RedisMetricsSnapshot {
    /// 成功操作次数。
    pub operations: u64,
    /// 错误次数。
    pub errors: u64,
    /// 超时次数。
    pub timeouts: u64,
    /// 熔断器开启次数。
    pub circuit_opens: u64,
    /// generation bump 次数。
    pub invalidations: u64,
}

#[derive(Debug, Default)]
struct RedisMetrics {
    operations: AtomicU64,
    errors: AtomicU64,
    timeouts: AtomicU64,
    circuit_opens: AtomicU64,
    invalidations: AtomicU64,
}

/// Redis backend。
///
/// 对应 Java `RedisCache`（implements `MyBatis Cache`）。
pub struct RedisCacheBackend {
    name: String,
    connection: ConnectionManager,
    config: RedisCacheConfig,
    metrics: RedisMetrics,
    consecutive_failures: AtomicU32,
    circuit_open_until_ms: AtomicU64,
}

impl RedisCacheBackend {
    /// 建连并验证可用性。
    pub async fn connect(name: impl Into<String>, config: RedisCacheConfig) -> Result<Self> {
        let client = Client::open(config.redis.url.as_str())
            .map_err(|error| CacheError::Backend(error.to_string()))?;
        let connection = client
            .get_connection_manager()
            .await
            .map_err(|error| CacheError::Backend(error.to_string()))?;
        Ok(Self {
            name: name.into(),
            connection,
            config,
            metrics: RedisMetrics::default(),
            consecutive_failures: AtomicU32::new(0),
            circuit_open_until_ms: AtomicU64::new(0),
        })
    }

    /// 标识名。
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 当前指标快照。
    pub fn metrics(&self) -> RedisMetricsSnapshot {
        RedisMetricsSnapshot {
            operations: self.metrics.operations.load(Ordering::Relaxed),
            errors: self.metrics.errors.load(Ordering::Relaxed),
            timeouts: self.metrics.timeouts.load(Ordering::Relaxed),
            circuit_opens: self.metrics.circuit_opens.load(Ordering::Relaxed),
            invalidations: self.metrics.invalidations.load(Ordering::Relaxed),
        }
    }

    /// 包装一次操作：超时、错误、熔断。
    async fn run<F, T>(&self, op: F) -> Result<T>
    where
        F: std::future::Future<Output = redis::RedisResult<T>>,
    {
        if self.circuit_open_until_ms.load(Ordering::Acquire) > now_ms() {
            return Err(CacheError::Backend("redis circuit is open".to_owned()));
        }
        match tokio::time::timeout(self.config.operation_timeout, op).await {
            Ok(Ok(value)) => {
                self.consecutive_failures.store(0, Ordering::Release);
                self.metrics.operations.fetch_add(1, Ordering::Relaxed);
                Ok(value)
            }
            Ok(Err(error)) => {
                self.metrics.errors.fetch_add(1, Ordering::Relaxed);
                self.record_failure();
                Err(CacheError::Backend(error.to_string()))
            }
            Err(_) => {
                self.metrics.timeouts.fetch_add(1, Ordering::Relaxed);
                self.record_failure();
                Err(CacheError::Backend("redis operation timed out".to_owned()))
            }
        }
    }

    fn record_failure(&self) {
        let f = self
            .consecutive_failures
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        if f >= self.config.circuit_failure_threshold.max(1) {
            self.circuit_open_until_ms.store(
                now_ms().saturating_add(duration_ms(self.config.circuit_cooldown)),
                Ordering::Release,
            );
            self.metrics.circuit_opens.fetch_add(1, Ordering::Relaxed);
            self.consecutive_failures.store(0, Ordering::Release);
        }
    }

    fn entry_key(&self, digest: &str) -> String {
        format!("{}:entry:{digest}", self.config.key_prefix)
    }

    fn generation_key(&self, namespace: &str) -> String {
        let hash = blake3::hash(namespace.as_bytes()).to_hex();
        format!("{}:generation:{hash}", self.config.key_prefix)
    }
}

impl CacheBackend for RedisCacheBackend {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>>> {
        Box::pin(async move {
            let mut connection = self.connection.clone();
            let entry_key = self.entry_key(key);
            self.run(async move { connection.get(&entry_key).await })
                .await
        })
    }

    fn put<'a>(&'a self, key: &'a str, value: Vec<u8>, ttl: Duration) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let mut connection = self.connection.clone();
            let entry_key = self.entry_key(key);
            let millis = duration_ms(ttl).max(1);
            self.run(async move {
                redis::cmd("PSETEX")
                    .arg(entry_key)
                    .arg(millis)
                    .arg(value)
                    .query_async::<()>(&mut connection)
                    .await
            })
            .await
        })
    }

    fn generation<'a>(&'a self, namespace: &'a str) -> BoxFuture<'a, Result<u64>> {
        Box::pin(async move {
            let mut connection = self.connection.clone();
            let generation_key = self.generation_key(namespace);
            let gen = self
                .run(async move { connection.get::<_, Option<u64>>(&generation_key).await })
                .await?;
            Ok(gen.unwrap_or(0))
        })
    }

    fn bump_generation<'a>(&'a self, namespace: &'a str) -> BoxFuture<'a, Result<u64>> {
        Box::pin(async move {
            let mut connection = self.connection.clone();
            let generation_key = self.generation_key(namespace);
            let gen = self
                .run(async move {
                    redis::cmd("INCR")
                        .arg(&generation_key)
                        .query_async::<u64>(&mut connection)
                        .await
                })
                .await?;
            self.metrics.invalidations.fetch_add(1, Ordering::Relaxed);
            Ok(gen)
        })
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

impl std::fmt::Debug for RedisCacheBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisCacheBackend")
            .field("name", &self.name)
            .field("metrics", &self.metrics())
            .finish_non_exhaustive()
    }
}
