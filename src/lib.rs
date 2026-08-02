//! `rbatis-cache` — RBatis 二级缓存 SPI 与保守拦截语义。
//!
//! 本 crate 是缓存生态的核心抽象，对应 Java 包 `org.mybatis.caches.*`：
//!
//! | Java 适配器 | Rust 实现 | 模块 |
//! |---|---|---|
//! | `org.mybatis.caches.caffeine.CaffeineCache` | `rbatis-moka` | `rbatis-moka/src/moka_cache.rs` |
//! | `org.mybatis.caches.redis.RedisCache` | `rbatis-redis` | `rbatis-redis/src/redis_cache.rs` |
//! | `org.mybatis.caches.memcached.MemcachedCache` | `rbatis-memcached` | `rbatis-memcached/src/memcached_cache.rs` |
//!
//! 自身 (`rbatis-cache`) 只承担：
//!
//! 1. [`CacheBackend`] SPI — backend 必须实现 4 个方法。
//! 2. [`CacheInterceptor`] — 解析/旁路/singleflight/loader 的统一拦截。
//! 3. [`CacheKey`] — BLAKE3 + 长度前缀化的隔离边界摘要。
//! 4. [`CacheEnvelope`] — MessagePack 线缆格式。
//! 5. [`CacheMetrics`] — 拦截器内部原子计数。
//! 6. [`testing`] — 各 backend 复用的契约测试 harness（feature `testing`）。
//!
//! ## 已落实不变量
//!
//! - 仅缓存解析后的 `SELECT` 单语句（非事务）。
//! - BLAKE3 key 隔离 `version + data_source + driver + tenant + namespace +
//!   statement_id + generation + canonical_sql + parameters`。
//! - MessagePack envelope + parser 抽取的 `table_tags`。
//! - 通过 generation bump 实现 namespace 级无扫描失效。
//! - 每个 key 上的 singleflight 防雪崩。
//! - backend 错误全部 fail-open 到 loader 并可观测。
//!
//! 这是一份 alpha 契约。RBatis 执行器集成（通过 `rbatis::intercept::Intercept`）
//! 与分布式 backend 在各自 crate / 仓库中开发。

#![forbid(unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]

// ---------------------------------------------------------------------------
// 模块声明（顺序固定，避免内层模块对外层产生循环依赖）
// ---------------------------------------------------------------------------

mod backend;
mod envelope;
mod error;
mod interceptor;
mod key;
mod l1;
mod listener;
mod local_backend;
mod metrics;
mod plugin;
mod policy;
mod rbatis_intercept;
mod singleflight;
mod sql;
mod transactional;

#[cfg(feature = "testing")]
pub mod testing;

// ---------------------------------------------------------------------------
// 公共 API 重导出
// ---------------------------------------------------------------------------

pub use backend::{CacheBackend, CachePolicy, InvalidationStrategy};
pub use envelope::CacheEnvelope;
pub use error::CacheError;
pub use interceptor::{CacheInterceptor, CacheRequest};
pub use key::{CacheKey, CacheKeyInput};
pub use l1::L1Cache;
pub use listener::CacheTransactionListener;
pub use local_backend::{EvictionStrategy, LocalBackend, LocalBackendConfig};
pub use metrics::{CacheMetrics, CacheMetricsSnapshot};
pub use plugin::RbatisCacheExt;
pub use policy::{CacheFailureMode, TransactionCacheMode, UseCacheFilter};
pub use rbatis_intercept::RbatisCacheInterceptor;
pub use singleflight::{LoadRole, LoadState, SingleFlight};
pub use sql::{SqlMetadata, StatementKind};
pub use transactional::TransactionalCacheBuffer;

/// 缓存层统一返回类型别名。
pub type CacheResult<T> = std::result::Result<T, CacheError>;

/// 内部别名（保持向后兼容旧文档/示例）。
pub type Result<T> = CacheResult<T>;
