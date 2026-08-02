//! 执行器集成层 — 把缓存挂进 rbatis 的拦截器链。
//!
//! [`RbatisCacheInterceptor`] 实现上游 `rbatis::intercept::Intercept`
//! 的 `before` / `after` 两段式钩子（对应 MyBatis `CachingExecutor`
//! 装饰链的查询路径）：
//!
//! - `before`（Query）：SQL 可缓存判定（单 SELECT、非 FOR UPDATE/SHARE、
//!   可选 use_cache_filter）→ L1 查 → L2 查 → 命中短路 `Action::Return`；
//!   miss 时 singleflight 选举 leader 后放行 DB 执行。
//! - `after`（Query）：singleflight 收尾唤醒 → L1 回填（会话一致性）→
//!   L2 回填（`cache_null` / `null_ttl` / `max_value_size` 约束）；
//!   Defer 事务模式写入事务缓冲。
//! - `after`（Exec）：`rows_affected > 0` 视为 DML → 清 L1；事务外直接
//!   bump namespace generation，事务内延迟到 commit（监听器负责）。
//!
//! 事务感知用 `Executor::name()`（`std::any::type_name`）字符串匹配，
//! 与 fork 的 `infer_executor_type` 等价，但完全在本 crate 内实现，
//! 不依赖 rbatis 的任何内部类型。

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use rbatis::executor::Executor;
use rbatis::intercept::{Action, Intercept, ResultType};
use rbatis::rbdc::db::ExecResult;
use rbatis::Error;
use rbs::Value;

use crate::envelope::CacheEnvelope;
use crate::key::{CacheKey, CacheKeyInput};
use crate::l1::L1Cache;
use crate::listener::CacheTransactionListener;
use crate::metrics::CacheMetrics;
use crate::policy::{CacheFailureMode, TransactionCacheMode};
use crate::singleflight::{LoadRole, SingleFlight, FOLLOWER_WAIT_TIMEOUT};
use crate::sql::SqlMetadata;
use crate::transactional::TransactionalCacheBuffer;
use crate::{CacheBackend, CachePolicy};

/// follower 最多等待轮数；超过后降级自行发查询（leader 长时间不完成时）。
const MAX_SINGLEFLIGHT_ATTEMPTS: usize = 3;

/// 把 rbatis 拦截器链中的缓存实现。
///
/// 构造后通过 [`crate::plugin::RbatisCacheExt::install_cache`] 注册到
/// [`rbatis::RBatis`]；Defer 事务模式还需把 [`RbatisCacheInterceptor::listener`]
/// 一并注册为事务监听器（`install_cache` 的第二参数）。
pub struct RbatisCacheInterceptor<B> {
    /// 命名空间（generation 失效与 L2 键隔离的边界）。
    namespace: String,
    /// 缓存后端。
    backend: Arc<B>,
    /// 完整策略（含执行器集成扩展字段）。
    policy: CachePolicy,
    /// 每 executor 会话 L1。
    l1: Arc<L1Cache>,
    /// 跨 before/after 的防击穿。
    singleflight: Arc<SingleFlight>,
    /// 事务缓冲表（Defer 模式；与监听器共享）。
    tx_buffers: Arc<DashMap<i64, Arc<TransactionalCacheBuffer>>>,
    /// 拦截器级指标。
    metrics: Arc<CacheMetrics>,
}

impl<B> std::fmt::Debug for RbatisCacheInterceptor<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RbatisCacheInterceptor")
            .field("namespace", &self.namespace)
            .field("l1_entries", &self.l1.len())
            .field("singleflight_inflight", &self.singleflight.len())
            .finish_non_exhaustive()
    }
}

impl<B> RbatisCacheInterceptor<B>
where
    B: CacheBackend,
{
    /// 构造拦截器。`namespace` 是缓存隔离与 generation 失效的边界
    /// （对应 MyBatis `Cache#id`）。
    pub fn new(namespace: impl Into<String>, backend: Arc<B>, policy: CachePolicy) -> Self {
        let namespace = namespace.into();
        let l1_max_entries = policy.l1_max_entries;
        let l1_ttl = policy.ttl;
        Self {
            namespace,
            backend,
            policy,
            l1: Arc::new(L1Cache::new(l1_max_entries, l1_ttl)),
            singleflight: Arc::new(SingleFlight::new()),
            tx_buffers: Arc::new(DashMap::new()),
            metrics: Arc::new(CacheMetrics::new()),
        }
    }

    /// 共享的指标句柄。
    pub fn metrics(&self) -> Arc<CacheMetrics> {
        Arc::clone(&self.metrics)
    }

    /// 共享的事务缓冲表（供 [`CacheTransactionListener`] 使用）。
    pub fn tx_buffers_clone(&self) -> Arc<DashMap<i64, Arc<TransactionalCacheBuffer>>> {
        Arc::clone(&self.tx_buffers)
    }

    /// 构建缓存事务监听器（Begin 建缓冲 / Commit 冲刷 / Rollback 丢弃）。
    pub fn listener(&self) -> CacheTransactionListener {
        CacheTransactionListener::new(
            self.backend.clone(),
            self.namespace.clone(),
            self.policy.ttl,
            self.tx_buffers_clone(),
            self.policy.transaction_mode == TransactionCacheMode::Defer,
        )
    }

    /// 当前是否在数据库事务内（字符串匹配，等价 fork 的 infer_executor_type）。
    fn is_in_transaction(rb: &dyn Executor) -> bool {
        rb.name().contains("RBatisTxExecutor")
    }

    /// 该 SQL 是否允许进入缓存。
    fn is_cacheable(&self, sql: &str) -> bool {
        let Ok(metadata) = SqlMetadata::parse(sql) else {
            return false;
        };
        if !metadata.is_cacheable() {
            return false;
        }
        if let Some(filter) = &self.policy.use_cache_filter {
            return filter.check(sql);
        }
        true
    }

    /// 构造键输入（namespace / driver / 参数编码统一在此收敛，保证
    /// before 与 after 产生一致的 digest）。
    fn build_key_input<'a>(
        namespace: &'a str,
        driver: &'a str,
        sql: &'a str,
        parameters: &'a [u8],
    ) -> CacheKeyInput<'a> {
        CacheKeyInput {
            version: "1",
            data_source: "default",
            driver,
            tenant: None,
            namespace,
            statement_id: "",
            sql,
            parameters,
        }
    }

    /// 读取当前 generation 并构造键；backend 故障按 failure_mode 处理。
    async fn resolve_key(
        &self,
        key_input: CacheKeyInput<'_>,
    ) -> Result<CacheKey, Error> {
        match self.backend.generation(key_input.namespace).await {
            Ok(generation) => CacheKey::build(key_input, generation)
                .map_err(|e| Error::from(format!("[rbatis-cache] {e}"))),
            Err(e) => match self.policy.failure_mode {
                CacheFailureMode::FailOpen => {
                    self.metrics.record_backend_error();
                    Err(Error::from(format!("[rbatis-cache] generation fail-open: {e}")))
                }
                CacheFailureMode::FailClosed => Err(Error::from(format!(
                    "[rbatis-cache] generation fail-closed: {e}"
                ))),
            },
        }
    }

    /// L1 + L2 联合查找；命中返回并把 L2 值提升到 L1。
    async fn lookup_value(
        &self,
        executor_id: i64,
        key: &CacheKey,
    ) -> Result<Option<Value>, Error> {
        // L1
        if let Some(v) = self.l1.get(executor_id, key.digest()) {
            self.metrics.record_hit();
            return Ok(Some((*v).clone()));
        }
        // L2
        let payload = match self.backend.get(key.digest()).await {
            Ok(Some(bytes)) => match CacheEnvelope::decode(&bytes) {
                Ok(envelope) if envelope.is_fresh(key.generation()) => envelope.payload,
                _ => {
                    self.metrics.record_miss();
                    return Ok(None);
                }
            },
            Ok(None) => {
                self.metrics.record_miss();
                return Ok(None);
            }
            Err(e) => {
                self.metrics.record_backend_error();
                match self.policy.failure_mode {
                    CacheFailureMode::FailOpen => return Ok(None),
                    CacheFailureMode::FailClosed => {
                        return Err(Error::from(format!("[rbatis-cache] L2 get fail-closed: {e}")))
                    }
                }
            }
        };
        match rmp_serde::from_slice::<Value>(&payload) {
            Ok(v) => {
                self.metrics.record_hit();
                self.l1.put(executor_id, key.digest(), Arc::new(v.clone()));
                Ok(Some(v))
            }
            Err(e) => {
                log::warn!("[rbatis-cache] L2 payload decode fail: {e}");
                Ok(None)
            }
        }
    }

    /// 回填 L2（受 max_value_size 与 failure_mode 约束）。
    async fn store_value(&self, key: &CacheKey, value: &Value, ttl: std::time::Duration) -> Result<(), Error> {
        let payload = rmp_serde::to_vec_named(value)
            .map_err(|e| Error::from(format!("[rbatis-cache] encode: {e}")))?;
        if payload.len() > self.policy.max_value_size {
            log::debug!(
                "[rbatis-cache] skip L2: value exceeds max_value_size ({} bytes)",
                self.policy.max_value_size
            );
            return Ok(());
        }
        let envelope = CacheEnvelope::new(key, payload, ttl)
            .encode()
            .map_err(|e| Error::from(format!("[rbatis-cache] envelope: {e}")))?;
        match self.backend.put(key.digest(), envelope, ttl).await {
            Ok(()) => Ok(()),
            Err(e) => {
                self.metrics.record_backend_error();
                match self.policy.failure_mode {
                    CacheFailureMode::FailOpen => {
                        log::warn!("[rbatis-cache] L2 set fail-open: {e}");
                        Ok(())
                    }
                    CacheFailureMode::FailClosed => Err(Error::from(format!(
                        "[rbatis-cache] L2 set fail-closed: {e}"
                    ))),
                }
            }
        }
    }
}

#[async_trait]
impl<B> Intercept for RbatisCacheInterceptor<B>
where
    B: CacheBackend,
{
    async fn before(
        &self,
        task_id: i64,
        rb: &dyn Executor,
        sql: &mut String,
        args: &mut Vec<Value>,
        result: ResultType<&mut Result<ExecResult, Error>, &mut Result<Value, Error>>,
    ) -> Result<Action, Error> {
        let ResultType::Query(query_result) = result else {
            return Ok(Action::Next);
        };

        if !self.is_cacheable(sql) {
            return Ok(Action::Next);
        }

        let executor_id = rb.id();
        let in_tx = Self::is_in_transaction(rb);

        // 事务 Defer 模式：先查事务缓冲（事务内已写过的条目）。
        if in_tx && self.policy.transaction_mode == TransactionCacheMode::Defer {
            if let Some(buf) = self.tx_buffers.get(&task_id) {
                let parameters = args_to_bytes(args);
                let driver = rb.driver_type().unwrap_or("unknown");
                let key_input = Self::build_key_input(&self.namespace, driver, sql, &parameters);
                if let Ok(key) = CacheKey::build(key_input, 0) {
                    if let Some(v) = buf.pending_get(key.digest()) {
                        *query_result = Ok((*v).clone());
                        return Ok(Action::Return);
                    }
                }
            }
        }
        // 事务 Bypass 模式：事务内一律不读缓存。
        if in_tx && self.policy.transaction_mode == TransactionCacheMode::Bypass {
            return Ok(Action::Next);
        }

        let parameters = args_to_bytes(args);
        let driver = rb.driver_type().unwrap_or("unknown");
        let key_input = Self::build_key_input(&self.namespace, driver, sql, &parameters);
        let key = match self.resolve_key(key_input).await {
            Ok(k) => k,
            // generation 故障（fail-open）：放行 DB，本次不缓存。
            Err(_) if self.policy.failure_mode == CacheFailureMode::FailOpen => {
                return Ok(Action::Next)
            }
            Err(e) => return Err(e),
        };
        let digest = key.digest().to_owned();

        // L1 + L2 查找（命中短路）。
        if let Some(v) = self.lookup_value(executor_id, &key).await? {
            *query_result = Ok(v);
            return Ok(Action::Return);
        }

        // singleflight：选举 leader 走 DB；follower 等 leader 的 after 完成后
        // re-check 缓存（最多 MAX_SINGLEFLIGHT_ATTEMPTS 轮，防 leader 反复失败）。
        if self.policy.blocking {
            let mut attempts = 0usize;
            loop {
                match self.singleflight.try_begin_load(&digest) {
                    LoadRole::Leader => break,
                    LoadRole::Follower(state) => {
                        attempts += 1;
                        if attempts >= MAX_SINGLEFLIGHT_ATTEMPTS {
                            break; // 降级：自行发查询
                        }
                        state.wait(FOLLOWER_WAIT_TIMEOUT).await;
                        let parameters = args_to_bytes(args);
                        let driver = rb.driver_type().unwrap_or("unknown");
                        let key_input =
                            Self::build_key_input(&self.namespace, driver, sql, &parameters);
                        let key = match self.resolve_key(key_input).await {
                            Ok(k) => k,
                            Err(_) => return Ok(Action::Next),
                        };
                        if let Some(v) = self.lookup_value(executor_id, &key).await? {
                            *query_result = Ok(v);
                            return Ok(Action::Return);
                        }
                    }
                }
            }
        }

        Ok(Action::Next)
    }

    async fn after(
        &self,
        task_id: i64,
        rb: &dyn Executor,
        sql: &mut String,
        args: &mut Vec<Value>,
        result: ResultType<&mut Result<ExecResult, Error>, &mut Result<Value, Error>>,
    ) -> Result<Action, Error> {
        let executor_id = rb.id();
        let in_tx = Self::is_in_transaction(rb);

        match result {
            ResultType::Query(query_result) => {
                if !self.is_cacheable(sql) {
                    return Ok(Action::Next);
                }
                let parameters = args_to_bytes(args);
                let driver = rb.driver_type().unwrap_or("unknown");
                let key_input = Self::build_key_input(&self.namespace, driver, sql, &parameters);
                let key = match self.resolve_key(key_input).await {
                    Ok(k) => k,
                    Err(_) => return Ok(Action::Next),
                };
                let digest = key.digest().to_owned();

                // 唤醒 singleflight follower（无论成败）。
                self.singleflight
                    .complete_load(&digest, query_result.is_ok());

                let Ok(value) = query_result.as_ref() else {
                    return Ok(Action::Next);
                };

                // L1 始终回填（会话一致性）。
                self.l1.put(executor_id, &digest, Arc::new(value.clone()));

                // 空结果不进 L2（可选）。
                if !self.policy.cache_null && is_empty(value) {
                    return Ok(Action::Next);
                }
                let ttl = if is_empty(value) {
                    self.policy.null_ttl.unwrap_or(self.policy.ttl)
                } else {
                    self.policy.ttl
                };

                // Defer 事务模式：写入事务缓冲（commit 时冲刷）。
                if in_tx && self.policy.transaction_mode == TransactionCacheMode::Defer {
                    if let Some(buf) = self.tx_buffers.get(&task_id) {
                        buf.put(key, Arc::new(value.clone()));
                        return Ok(Action::Next);
                    }
                }

                // L2 回填。
                self.store_value(&key, value, ttl).await?;
            }
            ResultType::Exec(exec_result) => {
                let Ok(er) = exec_result.as_ref() else {
                    return Ok(Action::Next);
                };
                if er.rows_affected > 0 {
                    // DML：清 L1（本 executor 会话立即一致）。
                    self.l1.clear_for_executor(executor_id);
                    if in_tx {
                        // 事务内不立即失效 L2；Defer 标记 namespace 由
                        // commit 时冲刷（bump），Bypass 由 commit 监听器 bump。
                        if self.policy.transaction_mode == TransactionCacheMode::Defer {
                            if let Some(buf) = self.tx_buffers.get(&task_id) {
                                buf.clear_namespace(&self.namespace);
                            }
                        }
                        log::debug!(
                            "[rbatis-cache] DML in tx ({} rows), L2 invalidation deferred to commit",
                            er.rows_affected
                        );
                    } else {
                        match self.backend.bump_generation(&self.namespace).await {
                            Ok(_) => self.metrics.record_invalidation(),
                            Err(e) => {
                                self.metrics.record_backend_error();
                                match self.policy.failure_mode {
                                    CacheFailureMode::FailOpen => {
                                        log::warn!("[rbatis-cache] invalidate fail-open: {e}");
                                    }
                                    CacheFailureMode::FailClosed => {
                                        return Err(Error::from(format!(
                                            "[rbatis-cache] invalidate fail-closed: {e}"
                                        )))
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(Action::Next)
    }
}

/// 空结果判定（Null / 空数组）。
fn is_empty(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::Array(arr) => arr.is_empty(),
        _ => false,
    }
}

/// 参数编码：与 payload 同用 MessagePack（named 模式），保证键与值一致。
fn args_to_bytes(args: &[Value]) -> Vec<u8> {
    rmp_serde::to_vec_named(args).unwrap_or_default()
}
