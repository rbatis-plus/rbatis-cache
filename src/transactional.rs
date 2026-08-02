//! 事务缓冲 — 把缓存写入推迟到 commit。
//!
//! 对应 MyBatis `TransactionalCache` 语义：
//! - 事务内查询**读** L2 缓存（miss 的结果**缓冲**在这里，不立即写 L2）；
//! - commit 时把缓冲条目冲刷到共享 backend（`flush_to`）；
//! - rollback 时丢弃（`discard`）。
//!
//! 每事务一个缓冲实例，由 [`CacheTransactionListener`](crate::CacheTransactionListener)
//! 在 `Begin` 时创建、commit/rollback 时冲刷/丢弃。

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use rbs::Value;

use crate::key::CacheKey;
use crate::Result;

/// 每事务的缓存写缓冲。
#[derive(Debug)]
pub struct TransactionalCacheBuffer {
    /// digest -> (key, value)。冲刷时按 key 编码 envelope 写入 backend。
    pending_add: DashMap<String, (CacheKey, Arc<Value>)>,
    /// commit 时需要 bump 的 namespace 集合（事务内 DML 标记）。
    clear_namespaces: DashMap<String, ()>,
    /// 冲刷时使用的 TTL。
    ttl: Duration,
}

impl TransactionalCacheBuffer {
    /// 构造事务缓冲（`ttl` 为 commit 冲刷时使用的 TTL）。
    pub fn new(ttl: Duration) -> Self {
        Self {
            pending_add: DashMap::new(),
            clear_namespaces: DashMap::new(),
            ttl,
        }
    }

    /// 缓冲一个待写条目（key 必须已包含正确的 generation）。
    pub fn put(&self, key: CacheKey, value: Arc<Value>) {
        self.pending_add
            .insert(key.digest().to_owned(), (key, value));
    }

    /// 按 digest 查找缓冲中的条目（事务内 Defer 模式读路径）。
    pub fn pending_get(&self, digest: &str) -> Option<Arc<Value>> {
        self.pending_add.get(digest).map(|e| e.1.clone())
    }

    /// 标记一个 namespace 需要在 commit 时 bump。
    pub fn clear_namespace(&self, namespace: &str) {
        self.clear_namespaces.insert(namespace.to_owned(), ());
    }

    /// Commit 成功：先 bump 全部标记的 namespace，再冲刷缓冲条目。
    pub async fn flush_to(&self, backend: &Arc<dyn crate::CacheBackend>) -> Result<()> {
        for ns in &self.clear_namespaces {
            let _ = backend.bump_generation(ns.key()).await?;
        }
        for entry in &self.pending_add {
            let (key, value) = entry.value();
            let payload = rmp_serde::to_vec_named(&**value)
                .map_err(|e| crate::CacheError::Codec(e.to_string()))?;
            let envelope = crate::envelope::CacheEnvelope::new(key, payload, self.ttl).encode()?;
            backend.put(key.digest(), envelope, self.ttl).await?;
        }
        Ok(())
    }

    /// Rollback：丢弃全部缓冲与标记。
    pub fn discard(&self) {
        self.pending_add.clear();
        self.clear_namespaces.clear();
    }

    /// 待写条目数。
    pub fn pending_count(&self) -> usize {
        self.pending_add.len()
    }
}
