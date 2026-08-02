//! 事务缓存监听器 — 把缓存一致性绑定到事务生命周期。
//!
//! 实现上游 `rbatis::plugin::transaction::TransactionListener`（fix 分支
//! 提供的 hook）：
//!
//! - `Begin`：Defer 模式创建该事务的 [`TransactionalCacheBuffer`]；
//! - `CommitSuccess`：Defer 冲刷缓冲（+ bump 事务内 DML 标记的
//!   namespace）；Bypass 模式 bump namespace（让 post-commit 读看到新数据）；
//! - `Rollback` / `CommitFailed` / `RollbackFailed`：丢弃缓冲（Bypass
//!   模式下事务内从未写 L2，无需回滚）。
//!
//! 监听器错误只记日志，绝不改变事务结果（上游契约保证）。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use rbatis::plugin::transaction::{TransactionEvent, TransactionEventType, TransactionListener};
use rbatis::Error;

use crate::transactional::TransactionalCacheBuffer;
use crate::CacheBackend;

/// 缓存事务监听器。
pub struct CacheTransactionListener {
    backend: Arc<dyn CacheBackend>,
    namespace: String,
    /// 冲刷缓冲时使用的 TTL。
    ttl: Duration,
    /// 与执行器集成层共享的 tx_id → 缓冲表。
    pub tx_buffers: Arc<DashMap<i64, Arc<TransactionalCacheBuffer>>>,
    defer_mode: bool,
}

impl std::fmt::Debug for CacheTransactionListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheTransactionListener")
            .field("namespace", &self.namespace)
            .field("defer_mode", &self.defer_mode)
            .finish_non_exhaustive()
    }
}

impl CacheTransactionListener {
    /// 构造监听器（`defer_mode` 决定 Begin 时是否创建事务缓冲）。
    pub fn new(
        backend: Arc<dyn CacheBackend>,
        namespace: impl Into<String>,
        ttl: Duration,
        tx_buffers: Arc<DashMap<i64, Arc<TransactionalCacheBuffer>>>,
        defer_mode: bool,
    ) -> Self {
        Self {
            backend,
            namespace: namespace.into(),
            ttl,
            tx_buffers,
            defer_mode,
        }
    }
}

#[async_trait]
impl TransactionListener for CacheTransactionListener {
    async fn on_event(&self, event: &TransactionEvent) -> Result<(), Error> {
        match event.event_type {
            TransactionEventType::Begin => {
                if self.defer_mode {
                    self.tx_buffers.insert(
                        event.tx_id,
                        Arc::new(TransactionalCacheBuffer::new(self.ttl)),
                    );
                    log::debug!(
                        "[rbatis-cache] Begin tx={} (defer buffer created)",
                        event.tx_id
                    );
                }
            }
            TransactionEventType::CommitSuccess => {
                if self.defer_mode {
                    if let Some((_, buf)) = self.tx_buffers.remove(&event.tx_id) {
                        log::debug!(
                            "[rbatis-cache] Commit tx={}, flushing {} pending entries",
                            event.tx_id,
                            buf.pending_count()
                        );
                        if let Err(e) = buf.flush_to(&self.backend).await {
                            log::warn!("[rbatis-cache] flush fail-open: {e}");
                        }
                    }
                } else {
                    log::debug!(
                        "[rbatis-cache] Commit tx={}, bumping namespace '{}'",
                        event.tx_id,
                        self.namespace
                    );
                    match self.backend.bump_generation(&self.namespace).await {
                        Ok(_) => {}
                        Err(e) => log::warn!("[rbatis-cache] bump fail-open: {e}"),
                    }
                }
            }
            TransactionEventType::CommitFailed => {
                // 视同 rollback：丢弃缓冲，L2 未被事务污染。
                self.tx_buffers.remove(&event.tx_id);
            }
            TransactionEventType::Rollback => {
                if let Some((_, buf)) = self.tx_buffers.remove(&event.tx_id) {
                    buf.discard();
                }
                // Bypass 模式：事务内未写 L2，缓存保持原样。
            }
            TransactionEventType::RollbackFailed => {
                self.tx_buffers.remove(&event.tx_id);
            }
        }
        Ok(())
    }
}
