//! LFU（最不经常使用）淘汰策略。
//!
//! 对应 Hutool `cn.hutool.cache.LFUCache`：驱逐访问次数最少的条目；
//! 同次数时优先驱逐最早插入的。
//!
//! 满时先对所有条目减去最小访问次数（公平计数器），保证新条目（0 次）
//! 不会被老条目（高次数）长期排挤。这是 Hutool `LFUCache.pruneCache`
//! 的语义。

use dashmap::DashMap;

use super::Entry;

/// LFU 淘汰策略：驱逐访问次数最少、同次数最旧的条目。
#[derive(Debug, Clone, Copy, Default)]
pub struct LfuPolicy;

impl LfuPolicy {
    /// Hutool `LFUCache.pruneCache` 语义：先对所有条目减去最小访问次数
    /// （公平计数器），再驱逐访问最少且最旧的条目。
    pub(crate) fn pick_victim(entries: &DashMap<String, Entry>) -> Option<String> {
        // 第一步：减去最小访问次数（Hutool 公平计数器）。
        let min_access = entries.iter().map(|e| e.accesses).min();
        if let Some(min_access) = min_access {
            for mut e in entries.iter_mut() {
                e.accesses = e.accesses.saturating_sub(min_access);
            }
        }
        // 第二步：驱逐访问最少且最旧的条目。
        entries
            .iter()
            .min_by_key(|e| (e.accesses, e.insert_seq))
            .map(|e| e.key().clone())
    }
}
