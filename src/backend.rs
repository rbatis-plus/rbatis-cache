//! 缓存 backend 抽象 + 失效策略 + 策略参数。
//!
//! 对应 Java 包 `org.apache.ibatis.cache.Cache`：MyBatis 的 `Cache` 接口
//! 是阻塞同步的；本 crate 的 [`CacheBackend`] 是异步（基于 `BoxFuture`），
//! 同时显式分出 [`CacheBackend::generation`] / [`CacheBackend::bump_generation`]
//! 让 generation 失效机制在所有 backend 上保持原子一致。
//!
//! ## 设计要点
//! - `get` / `put` 都返回 MessagePack envelope 字节，由上层 [`CacheInterceptor`](crate::CacheInterceptor)
//!   负责解码与新鲜度判定，backend 不感知 envelope 含义。
//! - generation 操作必须满足原子递增（`bump_generation` 返回新值）。backend
//!   可以选用 Redis `INCR`、Memcached `incr`、本地 `DashMap + AtomicU64` 等。
//! - trait 方法使用 `BoxFuture` 是为了让 backend 类型成为 dyn-compatible，
//!   从而 `Arc<dyn CacheBackend>` 可直接作为 [`CacheInterceptor`](crate::CacheInterceptor) 的字段。

#![allow(missing_docs)]

use std::time::Duration;

use futures::future::BoxFuture;

use crate::Result;

/// Generation-based 失效策略。
///
/// 对应 MyBatis 中 `Cache#clear` 整体失效的两种粒度。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidationStrategy {
    /// 数据库 commit 后 bump 整个命名空间 generation（默认，保守）。
    NamespaceGeneration,
    /// 预留：parser 抽取的关系级 generation（用于未来的精细失效）。
    TableGeneration,
}

/// 缓存策略：TTL、大小限制、失效粒度。
#[derive(Debug, Clone)]
pub struct CachePolicy {
    /// 条目过期时间（写时作为 envelope 的 expires_at_ms）。
    pub ttl: Duration,
    /// 单条 payload 上限（超过则不写 backend，但 loader 仍返回结果）。
    pub max_value_size: usize,
    /// 失效策略。
    pub invalidation: InvalidationStrategy,
}

impl Default for CachePolicy {
    fn default() -> Self {
        Self {
            ttl: Duration::from_mins(5),
            max_value_size: 1024 * 1024,
            invalidation: InvalidationStrategy::NamespaceGeneration,
        }
    }
}

/// Backend SPI。
///
/// 实现方必须保证：
/// 1. `get` 返回的字节可被 [`crate::envelope::CacheEnvelope::decode`] 解码。
/// 2. `bump_generation` 是原子的，并返回 bump 后的新值。
/// 3. 任何方法失败时**不**抛底层 client 异常，而是转为 [`CacheError::Backend`](crate::CacheError::Backend)。
///
/// Java 对照：`org.apache.ibatis.cache.Cache`（同步版）。
pub trait CacheBackend: Send + Sync + 'static {
    /// 取出由 [`CacheKey::digest`](crate::key::CacheKey::digest) 索引的 envelope 字节。
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>>>;

    /// 写入 envelope 字节，并按 TTL 设置 backend 端过期（若 backend 支持）。
    fn put<'a>(&'a self, key: &'a str, value: Vec<u8>, ttl: Duration) -> BoxFuture<'a, Result<()>>;

    /// 读取 namespace 的当前 generation（缺失视为 0）。
    fn generation<'a>(&'a self, namespace: &'a str) -> BoxFuture<'a, Result<u64>>;

    /// 原子递增 namespace generation 并返回新值。
    ///
    /// 对应 MyBatis `Cache#clear` 的精细版本——只让 bump 之后
    /// 的查询发现 generation 已变，从而自动 miss。
    fn bump_generation<'a>(&'a self, namespace: &'a str) -> BoxFuture<'a, Result<u64>>;
}
