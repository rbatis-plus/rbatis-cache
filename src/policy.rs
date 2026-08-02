//! 缓存策略扩展：失败模式、事务模式与按语句过滤谓词。
//!
//! 这些类型由 [`CachePolicy`](crate::CachePolicy) 引用，为执行器集成层
//! （[`RbatisCacheInterceptor`](crate::RbatisCacheInterceptor)）提供：
//!
//! - `FailOpen`（默认）/ `FailClosed`：backend 故障时降级为 miss 还是
//!   向调用方传播错误。
//! - `Bypass`（默认）/ `Defer`：事务内查询完全绕过缓存，还是像 MyBatis
//!   `TransactionalCache` 一样把事务内结果缓冲、commit 时冲刷。
//! - [`UseCacheFilter`]：按 SQL 谓词逐语句决定是否可缓存。

use std::fmt;
use std::sync::Arc;

/// 缓存后端故障时的行为。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CacheFailureMode {
    /// 继续执行（当作 miss），仅记录日志。默认。
    #[default]
    FailOpen,
    /// 把后端错误传播给调用方（查询失败）。
    FailClosed,
}

/// 缓存与事务的交互模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransactionCacheMode {
    /// 事务内查询完全绕过缓存（不读不写）；事务内 DML 立即失效 L1。
    /// 更简单，命中率更低。默认。
    #[default]
    Bypass,
    /// 事务内查询读 L2 缓存，但写入缓冲到事务缓冲；
    /// commit 时冲刷到 L2，rollback 时丢弃。
    /// 对应 MyBatis `TransactionalCache` 语义，命中率更高。
    Defer,
}

/// 按语句判定是否可缓存的谓词。
///
/// 返回 `true` 表示该 SQL 参与缓存。默认缓存所有可解析的 SELECT；
/// 用户可用 `CachePolicy::with_use_cache_filter` 覆盖（例如排除
/// `SELECT ... FOR UPDATE` 或指定 Mapper 语句）。
///
/// 用新类型包装以提供 `Debug`（`dyn Fn` 无法实现 `Debug`）。
pub struct UseCacheFilter(Arc<dyn Fn(&str) -> bool + Send + Sync>);

impl UseCacheFilter {
    /// 包装一个谓词闭包。
    pub fn new(f: impl Fn(&str) -> bool + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }

    /// 判定该 SQL 是否参与缓存。
    pub fn check(&self, sql: &str) -> bool {
        (self.0)(sql)
    }
}

impl Clone for UseCacheFilter {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl fmt::Debug for UseCacheFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<use_cache_filter>")
    }
}
