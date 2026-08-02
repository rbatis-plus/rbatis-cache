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

use std::time::Duration;

use futures::future::BoxFuture;

use crate::policy::{CacheFailureMode, TransactionCacheMode, UseCacheFilter};
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

/// 缓存策略：TTL、大小限制、失效粒度与执行器集成扩展。
#[derive(Debug, Clone)]
pub struct CachePolicy {
    /// 条目过期时间（写时作为 envelope 的 expires_at_ms）。
    pub ttl: Duration,
    /// 单条 payload 上限（超过则不写 backend，但 loader 仍返回结果）。
    pub max_value_size: usize,
    /// 失效策略。
    pub invalidation: InvalidationStrategy,
    /// 空结果（`Null` / 空数组）是否缓存；`false` 时只进 L1 不进 L2。
    pub cache_null: bool,
    /// 空结果的独立 TTL（防穿透；`None` 时使用 [`Self::ttl`]）。
    pub null_ttl: Option<Duration>,
    /// backend 故障时的行为（默认 fail-open）。
    pub failure_mode: CacheFailureMode,
    /// 缓存与事务的交互模式（默认 Bypass）。
    pub transaction_mode: TransactionCacheMode,
    /// 按语句过滤谓词（默认缓存所有可解析 SELECT）。
    pub use_cache_filter: Option<UseCacheFilter>,
    /// 是否启用 singleflight 防击穿（默认开启）。
    pub blocking: bool,
    /// 每 executor 会话 L1 最大条目数（默认 256，超出驱逐最旧条目）。
    pub l1_max_entries: usize,
}

impl Default for CachePolicy {
    fn default() -> Self {
        Self {
            ttl: Duration::from_mins(5),
            max_value_size: 1024 * 1024,
            invalidation: InvalidationStrategy::NamespaceGeneration,
            cache_null: true,
            null_ttl: Some(Duration::from_secs(10)),
            failure_mode: CacheFailureMode::FailOpen,
            transaction_mode: TransactionCacheMode::Bypass,
            use_cache_filter: None,
            blocking: true,
            l1_max_entries: 256,
        }
    }
}

impl CachePolicy {
    /// 设置条目 TTL。
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// 空结果是否进入 L2（`null_ttl` 生效的前提是 `cache_null = true`）。
    pub fn with_cache_null(mut self, v: bool) -> Self {
        self.cache_null = v;
        self
    }

    /// 设置空结果的独立 TTL。
    pub fn with_null_ttl(mut self, ttl: Duration) -> Self {
        self.null_ttl = Some(ttl);
        self
    }

    /// 设置 backend 故障模式为 fail-closed（向调用方传播错误）。
    pub fn with_failure_closed(mut self) -> Self {
        self.failure_mode = CacheFailureMode::FailClosed;
        self
    }

    /// 设置事务模式为 `Defer`（MyBatis `TransactionalCache` 语义）。
    pub fn with_transaction_defer(mut self) -> Self {
        self.transaction_mode = TransactionCacheMode::Defer;
        self
    }

    /// 按语句谓词过滤（返回 `true` 的 SQL 才参与缓存）。
    pub fn with_use_cache_filter(mut self, filter: UseCacheFilter) -> Self {
        self.use_cache_filter = Some(filter);
        self
    }

    /// 关闭 singleflight。
    pub fn without_blocking(mut self) -> Self {
        self.blocking = false;
        self
    }

    /// 设置每 executor 会话 L1 最大条目数。
    pub fn with_l1_max_entries(mut self, n: usize) -> Self {
        self.l1_max_entries = n;
        self
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
