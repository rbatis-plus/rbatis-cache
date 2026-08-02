//! 缓存操作的运行时指标。
//!
//! 对应 MyBatis 缓存统计：Java 侧没有内置 metrics 体系，通常由上层
//! (Spring Cache / Micrometer) 注入；本 crate 提供一组无锁原子计数，
//! 与 [`CacheInterceptor`](crate::CacheInterceptor) 紧耦合，任何 backend
//! 都自动继承。

use std::sync::atomic::{AtomicU64, Ordering};

/// 指标快照，可安全地在任意线程之间传递。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheMetricsSnapshot {
    /// 命中次数。
    pub hits: u64,
    /// 未命中次数（数据库 loader 被调用）。
    pub misses: u64,
    /// 策略或解析层旁路次数（事务内 / 非 SELECT）。
    pub bypasses: u64,
    /// backend 错误次数（已被 fail-open 隐藏）。
    pub backend_errors: u64,
    /// 真实数据库加载次数。
    pub loads: u64,
    /// generation bump 次数。
    pub invalidations: u64,
}

/// 拦截器内部原子计数器集合。
#[derive(Debug, Default)]
pub struct CacheMetrics {
    hits: AtomicU64,
    misses: AtomicU64,
    bypasses: AtomicU64,
    backend_errors: AtomicU64,
    loads: AtomicU64,
    invalidations: AtomicU64,
}

impl CacheMetrics {
    /// 构造全新计数器。
    pub const fn new() -> Self {
        Self {
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            bypasses: AtomicU64::new(0),
            backend_errors: AtomicU64::new(0),
            loads: AtomicU64::new(0),
            invalidations: AtomicU64::new(0),
        }
    }

    /// 获取一致性足够好的快照（仅供观察，非事务性读）。
    pub fn snapshot(&self) -> CacheMetricsSnapshot {
        CacheMetricsSnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            bypasses: self.bypasses.load(Ordering::Relaxed),
            backend_errors: self.backend_errors.load(Ordering::Relaxed),
            loads: self.loads.load(Ordering::Relaxed),
            invalidations: self.invalidations.load(Ordering::Relaxed),
        }
    }

    /// 内部：命中 +1。
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// 内部：未命中 +1。
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }

    /// 内部：旁路 +1。
    pub fn record_bypass(&self) {
        self.bypasses.fetch_add(1, Ordering::Relaxed);
    }

    /// 内部：backend 错误 +1。
    pub fn record_backend_error(&self) {
        self.backend_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// 内部：数据库加载 +1。
    pub fn record_load(&self) {
        self.loads.fetch_add(1, Ordering::Relaxed);
    }

    /// 内部：generation 失效 +1。
    pub fn record_invalidation(&self) {
        self.invalidations.fetch_add(1, Ordering::Relaxed);
    }
}
