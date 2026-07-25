//! 保守的 fail-open 缓存拦截器 + per-key singleflight。
//!
//! 对应 MyBatis 的 `CachingExecutor` 装饰链：MyBatis 用装饰器模式
//! 把多个 `Cache` 串成装饰链；本 crate 用 [`CacheInterceptor::get_or_load`]
//! 单方法承担"解析 SQL → 旁路判定 → 查缓存 → singleflight → loader → 写
//! envelope"整条流水。
//!
//! ## 单 flight 实现
//! Java 侧 MyBatis 没有内置 stampede 保护。本 crate 用 `DashMap<String,
//! Arc<Mutex<()>>>` + `Arc::strong_count == 2` 启发式清理条目：follower
//! 拿到 leader 的同一把锁，等 leader 完成后通过 re-check 命中缓存。

#![allow(missing_docs)]

use std::future::Future;
use std::sync::Arc;

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use tokio::sync::Mutex;

use crate::backend::{CacheBackend, CachePolicy};
use crate::envelope::CacheEnvelope;
use crate::key::{CacheKey, CacheKeyInput};
use crate::metrics::CacheMetrics;
use crate::sql::{SqlMetadata, StatementKind};
use crate::Result;

/// 单次缓存拦截请求。
///
/// 对应 MyBatis 中 `Executor` 准备好的"原始 SQL + 参数 + 事务状态"上下文。
#[derive(Debug, Clone, Copy)]
pub struct CacheRequest<'a> {
    /// 全部隔离边界（参见 [`CacheKeyInput`]）。
    pub key: CacheKeyInput<'a>,
    /// 当前是否在数据库事务内。
    pub in_transaction: bool,
}

/// 拦截器。
pub struct CacheInterceptor<B> {
    backend: Arc<B>,
    policy: CachePolicy,
    metrics: Arc<CacheMetrics>,
    /// 单 flight 表：digest -> 共享锁。
    flights: DashMap<String, Arc<Mutex<()>>>,
}

impl<B> CacheInterceptor<B>
where
    B: CacheBackend,
{
    /// 构造一个拦截器。
    ///
    /// 对应 `CachingExecutor(Cache cache)` 的简化版本——本 crate 不要求
    /// backend 与 metric 一一对应，由 [`Arc`] 共享即可。
    pub fn new(backend: Arc<B>, policy: CachePolicy) -> Self {
        Self {
            backend,
            policy,
            metrics: Arc::new(CacheMetrics::new()),
            flights: DashMap::new(),
        }
    }

    /// 共享指标句柄。
    pub fn metrics(&self) -> Arc<CacheMetrics> {
        Arc::clone(&self.metrics)
    }

    /// 主入口：拿一条请求 + loader 闭包，返回载荷字节。
    ///
    /// 对应 MyBatis 中 `CachingExecutor#query` 的执行路径——但本方法内
    /// 显式把"解析/旁路/cache/singleflight/loader/写缓存"全部串成一个调用，
    /// 简化上游对接。
    pub async fn get_or_load<F, Fut>(&self, request: CacheRequest<'_>, loader: F) -> Result<Vec<u8>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<u8>>>,
    {
        // 第一层保护：解析失败立刻旁路。
        let Ok(metadata) = SqlMetadata::parse(request.key.sql) else {
            return self.bypass(loader).await;
        };
        // 第二层保护：事务内或非 SELECT 一律旁路（保守）。
        if request.in_transaction || metadata.kind != StatementKind::Select {
            return self.bypass(loader).await;
        }

        // 第三层：backend 故障直接回源（fail-open）。
        let Ok(generation) = self.backend.generation(request.key.namespace).await else {
            self.metrics.record_backend_error();
            return self.load(loader).await;
        };

        let key = match CacheKey::build(request.key, generation) {
            Ok(key) => key,
            Err(_) => return self.load(loader).await,
        };

        // 缓存命中。
        if let Some(payload) = self.cached(&key).await {
            self.metrics.record_hit();
            return Ok(payload);
        }
        self.metrics.record_miss();

        // singleflight：拿同一 key 的锁，二次确认仍 miss 后独占执行 loader。
        let flight = self
            .flights
            .entry(key.digest().to_owned())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let guard = flight.lock().await;
        if let Some(payload) = self.cached(&key).await {
            self.metrics.record_hit();
            drop(guard);
            self.release_flight(key.digest(), &flight);
            return Ok(payload);
        }

        // 真正加载。
        let payload = self.load(loader).await?;

        // 写 backend（受 max_value_size 限制）。
        if payload.len() <= self.policy.max_value_size {
            let envelope = CacheEnvelope::new(&key, payload.clone(), self.policy.ttl).encode()?;
            if self
                .backend
                .put(key.digest(), envelope, self.policy.ttl)
                .await
                .is_err()
            {
                self.metrics.record_backend_error();
            }
        }
        drop(guard);
        self.release_flight(key.digest(), &flight);
        Ok(payload)
    }

    /// Commit 成功后由上层调用。
    pub async fn invalidate_after_commit(&self, namespace: &str) -> Result<u64> {
        let generation = self.backend.bump_generation(namespace).await?;
        self.metrics.record_invalidation();
        Ok(generation)
    }

    /// 取出 envelope 字节并解码；返回 None 表示 miss（不存在 / 已过期 /
    /// backend 错误 / 解码失败）。
    async fn cached(&self, key: &CacheKey) -> Option<Vec<u8>> {
        let Ok(bytes) = self.backend.get(key.digest()).await else {
            self.metrics.record_backend_error();
            return None;
        };
        let bytes = bytes?;
        CacheEnvelope::decode(&bytes)
            .ok()
            .filter(|envelope| envelope.is_fresh(key.generation()))
            .map(|envelope| envelope.payload)
    }

    /// 旁路路径：metrics +1 后直接调用 loader。
    async fn bypass<F, Fut>(&self, loader: F) -> Result<Vec<u8>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<u8>>>,
    {
        self.metrics.record_bypass();
        self.load(loader).await
    }

    /// loader 调用 + loads 计数。
    async fn load<F, Fut>(&self, loader: F) -> Result<Vec<u8>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<u8>>>,
    {
        self.metrics.record_load();
        loader().await
    }

    /// 清理已无共享者的 singleflight 条目。
    ///
    /// 采用 `Arc::strong_count == 2` 启发式：`flights` 表自身持 1 个 Arc，
    /// 调用方持 1 个 Arc 时无并发 follower，再删也不会误清。
    fn release_flight(&self, key: &str, flight: &Arc<Mutex<()>>) {
        let occupied = match self.flights.entry(key.to_owned()) {
            Entry::Occupied(entry) => entry,
            Entry::Vacant(_) => return,
        };
        if Arc::ptr_eq(occupied.get(), flight) && Arc::strong_count(occupied.get()) == 2 {
            occupied.remove();
        }
    }
}

/// 单 flight 用的 `Arc<Mutex<()>>` 别名（暴露给 backend / 上层装饰用）。
#[allow(dead_code)]
pub type FlightGuard = Arc<Mutex<()>>;
