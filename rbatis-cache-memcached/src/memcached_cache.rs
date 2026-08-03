//! Memcached backend（实现 [`CacheBackend`]）。
//!
//! 对应 Java：`org.mybatis.caches.memcached.MemcachedCache`
//! （位于 `/workspace-github/memcached-cache/src/main/java/org/mybatis/caches/memcached/MemcachedCache.java`）。
//!
//! | Java 方法 | Rust 对应 |
//! |---|---|
//! | `Cache#getId()` | [`MemcachedCacheBackend::name`] |
//! | `putObject(key, value)` | [`CacheBackend::put`] |
//! | `getObject(key)` | [`CacheBackend::get`] |
//! | `clear()`（走 `MemcachedClientWrapper#removeGroup`） | [`CacheBackend::bump_generation`] |
//!
//! ## Rust 侧增强（无 Java 对应）
//!
//! - 一致性哈希环（[`crate::consistent_hash`]）选择目标节点：Java 侧
//!   由 spymemcached 内部 ketama-hash 路由，本 crate 显式提供以支持
//!   后端无关的路由策略。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures::future::BoxFuture;
use memcache::MemcacheError;
use rbatis_cache::{CacheBackend, CacheError, Result};

use crate::client_wrapper::MemcachedClientWrapper;
use crate::configuration::MemcachedConfiguration;

/// 指标快照。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MemcachedMetricsSnapshot {
    /// 成功操作次数。
    pub operations: u64,
    /// 错误次数。
    pub errors: u64,
    /// 超时次数。
    pub timeouts: u64,
    /// generation bump 次数。
    pub invalidations: u64,
}

#[derive(Debug, Default)]
struct MemcachedMetrics {
    operations: AtomicU64,
    errors: AtomicU64,
    timeouts: AtomicU64,
    invalidations: AtomicU64,
}

/// Memcached backend。
///
/// 对应 `MemcachedCache`。
pub struct MemcachedCacheBackend {
    name: String,
    config: MemcachedConfiguration,
    wrapper: Arc<MemcachedClientWrapper>,
    metrics: MemcachedMetrics,
}

impl MemcachedCacheBackend {
    /// 建连并构造 backend。
    pub fn connect(name: impl Into<String>, config: MemcachedConfiguration) -> Result<Self> {
        let servers = config.servers.clone();
        let compression = config.compression;
        let wrapper = MemcachedClientWrapper::new(servers, compression)
            .map_err(|error: MemcacheError| CacheError::Backend(error.to_string()))?;
        Ok(Self {
            name: name.into(),
            config,
            wrapper: Arc::new(wrapper),
            metrics: MemcachedMetrics::default(),
        })
    }

    /// 标识名。
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 当前指标快照。
    pub fn metrics(&self) -> MemcachedMetricsSnapshot {
        MemcachedMetricsSnapshot {
            operations: self.metrics.operations.load(Ordering::Relaxed),
            errors: self.metrics.errors.load(Ordering::Relaxed),
            timeouts: self.metrics.timeouts.load(Ordering::Relaxed),
            invalidations: self.metrics.invalidations.load(Ordering::Relaxed),
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

impl CacheBackend for MemcachedCacheBackend {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>>> {
        let entry_key = self.entry_key(key);
        let wrapper = self.wrapper.clone();
        Box::pin(async move {
            let value = tokio::task::spawn_blocking(move || wrapper.get_object(&entry_key))
                .await
                .map_err(|error| CacheError::Backend(format!("join error: {error}")))?
                .map_err(|error| CacheError::Backend(error.to_string()))?;
            Ok(value)
        })
    }

    fn put<'a>(
        &'a self,
        key: &'a str,
        value: Vec<u8>,
        ttl: std::time::Duration,
    ) -> BoxFuture<'a, Result<()>> {
        let entry_key = self.entry_key(key);
        let wrapper = self.wrapper.clone();
        let ttl_seconds = ttl.as_secs().max(1) as u32;
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                wrapper.put_object(&entry_key, &value, ttl_seconds)
            })
            .await
            .map_err(|error| CacheError::Backend(format!("join error: {error}")))?
            .map_err(|error| CacheError::Backend(error.to_string()))?;
            Ok(())
        })
    }

    fn generation<'a>(&'a self, namespace: &'a str) -> BoxFuture<'a, Result<u64>> {
        let generation_key = self.generation_key(namespace);
        let wrapper = self.wrapper.clone();
        Box::pin(async move {
            let raw = tokio::task::spawn_blocking(move || wrapper.client_get_u64(&generation_key))
                .await
                .map_err(|error| CacheError::Backend(format!("join error: {error}")))?
                .map_err(|error| CacheError::Backend(error.to_string()))?;
            Ok(raw.unwrap_or(0))
        })
    }

    fn bump_generation<'a>(&'a self, namespace: &'a str) -> BoxFuture<'a, Result<u64>> {
        let generation_key = self.generation_key(namespace);
        let wrapper = self.wrapper.clone();
        Box::pin(async move {
            let new = tokio::task::spawn_blocking(move || wrapper.client_incr(&generation_key))
                .await
                .map_err(|error| CacheError::Backend(format!("join error: {error}")))?
                .map_err(|error| CacheError::Backend(error.to_string()))?;
            Ok(new)
        })
    }
}

/// 给 [`MemcachedClientWrapper`] 增加 `u64` 读 / 自增辅助方法（不在
/// Java 对照面，仅供 SPI 实现用）。
impl MemcachedClientWrapper {
    /// 直接读 u64（generation 数值）。
    pub(crate) fn client_get_u64(
        &self,
        key: &str,
    ) -> std::result::Result<Option<u64>, MemcacheError> {
        self.client.get::<u64>(key)
    }

    /// 自增 generation。counter 不存在时 `add(0)` 再 `increment(1)` 兜底。
    pub(crate) fn client_incr(&self, key: &str) -> std::result::Result<u64, MemcacheError> {
        // 先尝试 add 0；如果已存在，add 会返回错误但不影响后续 incr。
        let _ = self.client.add(key, 0u64, 0);
        self.client.increment(key, 1)
    }
}

impl std::fmt::Debug for MemcachedCacheBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemcachedCacheBackend")
            .field("name", &self.name)
            .field("metrics", &self.metrics())
            .finish_non_exhaustive()
    }
}
