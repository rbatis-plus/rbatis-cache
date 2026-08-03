//! `rbatis-cache-moka` — 基于 moka 的进程内 backend。
//!
//! Java 对照：`org.mybatis.caches.caffeine.CaffeineCache`
//! （Caffeine 是 moka 的 Java 原型）。
//!
//! ## 与 `rbatis-cache` 内置 `LocalBackend` 的区别
//!
//! | 特性 | `LocalBackend`（DashMap） | `MokaCacheBackend`（moka） |
//! |---|---|---|
//! | 并发模型 | DashMap 分段锁 | moka 无锁 concurrent cache |
//! | 淘汰策略 | 自实现 FIFO/LFU/LRU | moka 内置 TinyLFU |
//! | 异步 get | `BoxFuture` 包装同步读 | 原生 async（`get_with`） |
//! | 过期 | 手动 expires_at + 后台线程 | moka 内置时间轮 |
//! | 适用场景 | 轻量、无额外依赖 | 高并发、需要 TinyLFU |
//!
//! ## 使用示例
//!
//! ```ignore
//! use rbatis_cache_moka::{MokaCacheBackend, MokaCacheConfig};
//!
//! let backend = MokaCacheBackend::new("my-namespace", MokaCacheConfig::default());
//! // 作为 Arc<dyn CacheBackend> 传给 CacheInterceptor
//! ```

#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use moka::future::Cache;
use rbatis_cache::{CacheBackend, Result};

/// moka backend 配置。
#[derive(Debug, Clone)]
pub struct MokaCacheConfig {
    /// 最大条目数（默认 10_000）。
    pub max_capacity: u64,
    /// 条目 TTL（默认 5 分钟）。
    pub ttl: Duration,
    /// 条目 TTI（time-to-idle，最近未访问过期；默认与 TTL 相同）。
    pub tti: Option<Duration>,
}

impl Default for MokaCacheConfig {
    fn default() -> Self {
        Self {
            max_capacity: 10_000,
            ttl: Duration::from_secs(300),
            tti: None,
        }
    }
}

impl MokaCacheConfig {
    /// 设置最大条目数。
    pub fn with_max_capacity(mut self, n: u64) -> Self {
        self.max_capacity = n;
        self
    }

    /// 设置条目 TTL。
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// 设置条目 TTI（time-to-idle）。
    pub fn with_tti(mut self, tti: Duration) -> Self {
        self.tti = Some(tti);
        self
    }
}

/// 基于 moka 的进程内 backend。
///
/// 使用 moka 的 `future::Cache`（TinyLFU 淘汰策略 + 时间轮过期），
/// 适用于高并发场景。generation 通过 `AtomicU64` 实现原子递增。
pub struct MokaCacheBackend {
    /// moka 缓存实例。
    cache: Cache<String, Vec<u8>>,
    /// namespace 前缀（用于 generation key 隔离）。
    namespace: String,
    /// namespace -> generation 原子计数。
    generation: AtomicU64,
    /// 配置（保留用于调试）。
    #[allow(dead_code)]
    config: MokaCacheConfig,
}

impl MokaCacheBackend {
    /// 构造 moka backend。
    pub fn new(namespace: impl Into<String>, config: MokaCacheConfig) -> Self {
        let mut builder = Cache::builder()
            .max_capacity(config.max_capacity)
            .time_to_live(config.ttl);

        if let Some(tti) = config.tti {
            builder = builder.time_to_idle(tti);
        }

        Self {
            cache: builder.build(),
            namespace: namespace.into(),
            generation: AtomicU64::new(0),
            config,
        }
    }

    fn entry_key(&self, digest: &str) -> String {
        format!("{}:entry:{digest}", self.namespace)
    }

    fn generation_key(&self, namespace: &str) -> String {
        let hash = blake3::hash(namespace.as_bytes()).to_hex();
        format!("{}:generation:{hash}", self.namespace)
    }
}

impl CacheBackend for MokaCacheBackend {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>>> {
        Box::pin(async move {
            let storage_key = self.entry_key(key);
            Ok(self.cache.get(&storage_key).await)
        })
    }

    fn put<'a>(
        &'a self,
        key: &'a str,
        value: Vec<u8>,
        _ttl: Duration,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let storage_key = self.entry_key(key);
            // moka 的 TTL 由 cache builder 统一配置，put 时不需要单独设置。
            // 传入的 ttl 参数被忽略（moka 不支持 per-entry TTL）。
            self.cache.insert(storage_key, value).await;
            Ok(())
        })
    }

    fn generation<'a>(&'a self, namespace: &'a str) -> BoxFuture<'a, Result<u64>> {
        Box::pin(async move {
            let _ = namespace;
            // moka backend 使用单一 generation 计数器（与 namespace 参数无关，
            // 因为 moka 是进程内缓存，不跨进程共享）。
            // generation key 同步存入 cache 便于调试。
            let gen_key = self.generation_key(namespace);
            let gen = self
                .cache
                .get(&gen_key)
                .await
                .and_then(|bytes| {
                    let arr: [u8; 8] = bytes.try_into().ok()?;
                    Some(u64::from_le_bytes(arr))
                })
                .unwrap_or(0);
            Ok(gen)
        })
    }

    fn bump_generation<'a>(&'a self, namespace: &'a str) -> BoxFuture<'a, Result<u64>> {
        Box::pin(async move {
            let new = self
                .generation
                .fetch_add(1, Ordering::AcqRel)
                .saturating_add(1);
            // 同步写入 cache 便于 generation() 读取。
            let gen_key = self.generation_key(namespace);
            self.cache.insert(gen_key, new.to_le_bytes().to_vec()).await;
            Ok(new)
        })
    }
}

impl std::fmt::Debug for MokaCacheBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MokaCacheBackend")
            .field("namespace", &self.namespace)
            .field("entry_count", &self.cache.entry_count())
            .field("generation", &self.generation.load(Ordering::Relaxed))
            .field("config", &self.config)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn contract_missing_key_is_none() {
        let backend = MokaCacheBackend::new("test", MokaCacheConfig::default());
        rbatis_cache::testing::assert_missing_key_is_none(&backend).await;
    }

    #[tokio::test]
    async fn contract_put_get_roundtrip() {
        let backend = MokaCacheBackend::new("test", MokaCacheConfig::default());
        rbatis_cache::testing::assert_get_put_roundtrip(&backend).await;
    }

    #[tokio::test]
    async fn contract_generation_atomic() {
        let backend = MokaCacheBackend::new("test", MokaCacheConfig::default());
        rbatis_cache::testing::assert_generation_atomic(&backend).await;
    }

    #[tokio::test]
    async fn contract_ttl_expires() {
        let config = MokaCacheConfig::default()
            .with_ttl(Duration::from_millis(50))
            .with_tti(Duration::from_millis(50));
        let backend = MokaCacheBackend::new("test", config);
        rbatis_cache::testing::assert_ttl_expires(&backend).await;
    }
}
