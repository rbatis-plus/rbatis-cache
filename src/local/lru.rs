//! LRU（最近最少使用）淘汰策略。
//!
//! 对应 Hutool `cn.hutool.cache.LRUCache`：驱逐最久未被访问的条目。
//! 通过 `last_access_seq` 判断（越小越久未访问）。

use dashmap::DashMap;

use super::Entry;

/// LRU 淘汰策略：驱逐最久未被访问的条目。
#[derive(Debug, Clone, Copy, Default)]
pub struct LruPolicy;

impl LruPolicy {
    /// 选出 `last_access_seq` 最小（最久未访问）的条目 key。
    pub(crate) fn pick_victim(entries: &DashMap<String, Entry>) -> Option<String> {
        entries
            .iter()
            .min_by_key(|e| e.last_access_seq)
            .map(|e| e.key().clone())
    }
}
