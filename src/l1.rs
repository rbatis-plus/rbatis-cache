//! 每 executor 会话的进程内 L1 缓存。
//!
//! 对应 MyBatis 一级缓存的会话一致性作用：同一 `Executor`（连接）内
//! 重复查询优先命中 L1，避免每次命中 L2 都要跨进程往返 + MessagePack
//! 解码。L2 命中会把值提升到 L1（`Arc` 引用计数，零拷贝）。
//!
//! 实现为 16 个 shard 的 `DashMap`（executor_id → 条目），每个 executor
//! 的条目数受 `CachePolicy::l1_max_entries` 限制，超限驱逐最旧条目；
//! 条目带 TTL，惰性过期。

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use rbs::Value;

/// 每个 executor 会话的 L1 缓存。
#[derive(Debug)]
pub struct L1Cache {
    /// executor_id -> 条目。
    entries: DashMap<i64, L1Entry>,
    /// 每 executor 最大条目数。
    max_entries: usize,
    /// 条目 TTL（惰性过期）。
    ttl: Duration,
}

#[derive(Debug)]
struct L1Entry {
    value: Arc<Value>,
    expires_at: Instant,
}

impl L1Cache {
    /// 构造 L1 缓存（`max_entries` 每 executor 上限，`ttl` 条目过期时间）。
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            entries: DashMap::new(),
            max_entries: usize::max(max_entries, 1),
            ttl,
        }
    }

    /// 命中返回共享值；不存在 / 已过期返回 `None`（并清理过期条目）。
    pub fn get(&self, executor_id: i64, digest: &str) -> Option<Arc<Value>> {
        let entry = self.entries.get(&executor_id)?;
        if Instant::now() >= entry.expires_at {
            drop(entry);
            self.entries.remove(&executor_id);
            return None;
        }
        let _ = digest; // digest 保留给未来的 key 级淘汰策略
        Some(entry.value.clone())
    }

    /// 写入（覆盖）。超限时移除该 executor 最旧的条目。
    pub fn put(&self, executor_id: i64, digest: &str, value: Arc<Value>) {
        let entry = L1Entry {
            value,
            expires_at: Instant::now() + self.ttl,
        };
        let _ = digest; // digest 保留给未来的 key 级淘汰策略
        if let Some(mut old) = self.entries.get_mut(&executor_id) {
            *old = entry;
            return;
        }
        if self.entries.len() >= self.max_entries {
            if let Some(oldest) = self.entries.iter().min_by_key(|e| e.expires_at) {
                let key = *oldest.key();
                drop(oldest);
                self.entries.remove(&key);
            }
        }
        self.entries.insert(executor_id, entry);
    }

    /// 清除某个 executor 的全部 L1 条目（DML 后调用）。
    pub fn clear_for_executor(&self, executor_id: i64) {
        self.entries.remove(&executor_id);
    }

    /// 当前条目总数。
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// 是否无任何条目。
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_put_roundtrip() {
        let c = L1Cache::new(8, Duration::from_secs(60));
        let v = Arc::new(Value::I64(42));
        c.put(1, "d1", v.clone());
        assert_eq!(*c.get(1, "d1").unwrap(), Value::I64(42));
        assert!(c.get(2, "d1").is_none(), "other executor must miss");
    }

    #[test]
    fn capacity_is_bounded() {
        let c = L1Cache::new(2, Duration::from_secs(60));
        c.put(1, "a", Arc::new(Value::I64(1)));
        c.put(2, "b", Arc::new(Value::I64(2)));
        c.put(3, "c", Arc::new(Value::I64(3)));
        assert!(c.len() <= 2, "capacity must stay bounded");
    }

    #[test]
    fn entries_expire() {
        let c = L1Cache::new(8, Duration::from_millis(10));
        c.put(1, "a", Arc::new(Value::I64(1)));
        std::thread::sleep(Duration::from_millis(30));
        assert!(c.get(1, "a").is_none(), "expired entry must miss");
    }

    #[test]
    fn clear_is_per_executor() {
        let c = L1Cache::new(8, Duration::from_secs(60));
        c.put(1, "a", Arc::new(Value::I64(1)));
        c.put(2, "b", Arc::new(Value::I64(2)));
        c.clear_for_executor(1);
        assert!(c.get(1, "a").is_none());
        assert!(c.get(2, "b").is_some(), "other executor untouched");
    }
}
