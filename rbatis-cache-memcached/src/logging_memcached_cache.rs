//! 日志装饰器 backend。
//!
//! 对应 Java：`org.mybatis.caches.memcached.LoggingMemcachedCache`
//! （位于 `/workspace-github/memcached-cache/src/main/java/org/mybatis/caches/memcached/LoggingMemcachedCache.java`）。
//!
//! Java 侧 `LoggingMemcachedCache` 继承 MyBatis 核心包的
//! `org.apache.ibatis.cache.decorators.LoggingCache`——一个记录请求次数、
//! 命中次数与命中率的装饰器。
//!
//! 本 crate 复刻同等语义：包装 [`MemcachedCacheBackend`]，统计请求与
//! 命中并在每次读写时输出 `log::debug!` 日志，命中率日志行为与
//! `LoggingCache#getHitRatio()` 一致。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use log::debug;
use rbatis_cache::{CacheBackend, Result};

use crate::memcached_cache::MemcachedCacheBackend;

/// 日志装饰器。
///
/// 对应 Java: `LoggingMemcachedCache extends LoggingCache`。
///
/// ## 行为
/// - 每次 `get`：requests +1；命中则 hits +1 并记录命中率日志；
/// - 每次 `put`：记录写入日志（key 与字节数）；
/// - 每次 `bump_generation`：记录失效日志。
///
/// 日志输出走 `log` 门面 crate，由上层应用选择具体 logger 实现
/// （与 Java 侧 MyBatis 走自身 LogFactory 的角色相同）。
pub struct LoggingMemcachedCache {
    /// 被装饰的 backend。对应 `LoggingCache` 持有的 `delegate` 字段。
    delegate: Arc<MemcachedCacheBackend>,
    /// 缓存标识（Java 构造参数 `id`）。
    id: String,
    /// 累计请求次数（对应 `LoggingCache#requests`）。
    requests: AtomicU64,
    /// 累计命中次数（对应 `LoggingCache#hits`）。
    hits: AtomicU64,
}

impl LoggingMemcachedCache {
    /// 构造装饰器。对应 Java: `LoggingMemcachedCache(String id)`。
    pub fn new(id: impl Into<String>, delegate: Arc<MemcachedCacheBackend>) -> Self {
        Self {
            delegate,
            id: id.into(),
            requests: AtomicU64::new(0),
            hits: AtomicU64::new(0),
        }
    }

    /// 当前命中率。对应 `LoggingCache#getHitRatio()`。
    pub fn hit_ratio(&self) -> f64 {
        let requests = self.requests.load(Ordering::Relaxed);
        if requests == 0 {
            return 0.0;
        }
        self.hits.load(Ordering::Relaxed) as f64 / requests as f64
    }

    /// 记录一次 get 的结果并输出命中率日志。
    ///
    /// 对应 `LoggingCache#getObject(Object key)` 中
    /// `requests++ / if (value != null) hits++` 的统计段。
    fn record_request(&self, hit: bool) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        if hit {
            self.hits.fetch_add(1, Ordering::Relaxed);
        }
        // MyBatis LoggingCache 的日志形如
        // "Cache Hit Ratio [com.foo.Mapper]: 0.5"
        debug!("Cache Hit Ratio [{}]: {:.4}", self.id, self.hit_ratio());
    }
}

impl CacheBackend for LoggingMemcachedCache {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>>> {
        Box::pin(async move {
            let value = self.delegate.get(key).await?;
            // 命中判定与日志（对应 LoggingCache#getObject 装饰段）
            self.record_request(value.is_some());
            Ok(value)
        })
    }

    fn put<'a>(&'a self, key: &'a str, value: Vec<u8>, ttl: Duration) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // 写入日志（对应 LoggingCache#putObject 的装饰段）
            debug!(
                "Cache [{}] put: {} bytes (ttl {:?})",
                self.id,
                value.len(),
                ttl
            );
            self.delegate.put(key, value, ttl).await
        })
    }

    fn generation<'a>(&'a self, namespace: &'a str) -> BoxFuture<'a, Result<u64>> {
        // 读操作不打日志，直接透传（Java 侧 LoggingCache 不覆盖此类方法）
        self.delegate.generation(namespace)
    }

    fn bump_generation<'a>(&'a self, namespace: &'a str) -> BoxFuture<'a, Result<u64>> {
        Box::pin(async move {
            // 失效日志（对应 LoggingCache#clear 的装饰段）
            debug!("Cache [{}] invalidate namespace: {}", self.id, namespace);
            self.delegate.bump_generation(namespace).await
        })
    }
}

impl std::fmt::Debug for LoggingMemcachedCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoggingMemcachedCache")
            .field("id", &self.id)
            .field("hit_ratio", &self.hit_ratio())
            .finish_non_exhaustive()
    }
}
