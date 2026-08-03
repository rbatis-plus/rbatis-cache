//! FIFO（先进先出）淘汰策略。
//!
//! 对应 Hutool `cn.hutool.cache.FIFOCache`：按插入顺序驱逐最旧的条目。
//! 不关心访问频率或最近访问时间，只看 `insert_seq`。

use dashmap::DashMap;

use super::Entry;

/// FIFO 淘汰策略：驱逐最早写入的条目。
#[derive(Debug, Clone, Copy, Default)]
pub struct FifoPolicy;

impl FifoPolicy {
    /// 选出 `insert_seq` 最小（最早插入）的条目 key。
    pub(crate) fn pick_victim(entries: &DashMap<String, Entry>) -> Option<String> {
        entries
            .iter()
            .min_by_key(|e| e.insert_seq)
            .map(|e| e.key().clone())
    }
}
