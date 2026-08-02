//! 进程内 byte-level [`CacheBackend`]。
//!
//! 对应 Java：`org.mybatis.caches.caffeine.CaffeineCache`
//! （位于 `/workspace-github/caffeine-cache/src/main/java/org/mybatis/caches/caffeine/CaffeineCache.java`）。
//!
//! Java 侧 Caffeine 适配器只暴露 `getObject/putObject/removeObject/clear/getSize`，
//! 不感知 generation——其实现等价于一个简单的内存 map。
//!
//! 本 crate 的 [`LocalBackend`] 在 SPI 上**比 Java 对位更严格**：实现完整
//! 4 个 `CacheBackend` 方法（含 `bump_generation`），与 [`crate::CacheInterceptor`]
//! 协作支持 generation 失效。底层使用 [`dashmap`]（ConcurrentHashMap 等价）
//! + 可选的后台清理线程。
//!
//! ## Rust 侧增强（无 Java 对应）
//!
//! - TTL 由 [`crate::envelope::CacheEnvelope::is_fresh`] 判定（已在
//!   interceptor 层处理）；backend 自身存储过期时间戳，按 lazy 或
//!   后台线程（`cleanup_interval`）清理。
//! - generation 原子计数由 `DashMap<String, AtomicU64>` 承担。
//! - 大小上限驱逐：配置 `max_entries` 后启用，策略见 [`EvictionStrategy`]：
//!   FIFO/LFU/LRU 对应 Hutool `cn.hutool.cache` 的 `FIFOCache` /
//!   `LFUCache` / `LRUCache`（LFU 含 Hutool 的"满时减最小访问次数"
//!   公平计数器语义）。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use futures::future::BoxFuture;

use crate::backend::CacheBackend;
use crate::Result;

/// 内部条目。
#[derive(Clone)]
struct Entry {
    /// 原始 envelope 字节。
    bytes: Vec<u8>,
    /// 过期时间（Unix epoch ms）。
    expires_at_ms: u64,
    /// 全局插入序号（FIFO/LFU 驱逐依据）。
    insert_seq: u64,
    /// 最近访问序号（LRU 驱逐依据；越大越新，单调递增避免时间粒度问题）。
    last_access_seq: u64,
    /// 累计访问次数（LFU 驱逐依据）。
    accesses: u64,
}

/// 大小上限驱逐策略（对应 Hutool `cn.hutool.cache` 的 FIFO/LFU/LRU）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionStrategy {
    /// 驱逐最早写入的条目（Hutool `FIFOCache`）。
    Fifo,
    /// 驱逐访问次数最少、同次数最旧的条目（Hutool `LFUCache`；
    /// 满时先对所有条目减去最小访问次数，保证新条目公平参与竞争）。
    Lfu,
    /// 驱逐最久未被访问的条目（Hutool `LRUCache`）。
    Lru,
}

/// 进程内 backend 配置。
#[derive(Debug, Clone)]
pub struct LocalBackendConfig {
    /// 定期清理过期条目的间隔。`Some(interval)` 时启动后台清理线程
    /// （backend 被 drop 时自动停止）；`None` 禁用（仅 get 时 lazy 清理）。
    pub cleanup_interval: Option<Duration>,
    /// 条目数上限；`Some(n)` 时启用驱逐，`None` 表示不限制（默认）。
    pub max_entries: Option<usize>,
    /// 驱逐策略（仅 `max_entries` 为 `Some` 时生效）。
    pub eviction: EvictionStrategy,
}

impl Default for LocalBackendConfig {
    fn default() -> Self {
        Self {
            cleanup_interval: Some(Duration::from_secs(30)),
            max_entries: None,
            eviction: EvictionStrategy::Fifo,
        }
    }
}

impl LocalBackendConfig {
    /// 启用大小上限 + FIFO 驱逐（Hutool `FIFOCache` 语义）。
    pub fn with_max_entries_fifo(mut self, n: usize) -> Self {
        self.max_entries = Some(n);
        self.eviction = EvictionStrategy::Fifo;
        self
    }

    /// 启用大小上限 + LFU 驱逐（Hutool `LFUCache` 语义）。
    pub fn with_max_entries_lfu(mut self, n: usize) -> Self {
        self.max_entries = Some(n);
        self.eviction = EvictionStrategy::Lfu;
        self
    }

    /// 启用大小上限 + LRU 驱逐（Hutool `LRUCache` 语义）。
    pub fn with_max_entries_lru(mut self, n: usize) -> Self {
        self.max_entries = Some(n);
        self.eviction = EvictionStrategy::Lru;
        self
    }

    /// 设置后台清理间隔（`None` 关闭后台清理）。
    pub fn with_cleanup_interval(mut self, interval: Option<Duration>) -> Self {
        self.cleanup_interval = interval;
        self
    }
}

/// 进程内 backend。
pub struct LocalBackend {
    /// data key -> entry（Arc 共享给后台清理线程；DashMap::clone 是深拷贝，
    /// 不能用来共享）。
    entries: Arc<DashMap<String, Entry>>,
    /// namespace -> generation
    generations: DashMap<String, Arc<AtomicU64>>,
    /// 全局插入序号（FIFO/LRU/LFU 驱逐）。
    seq: AtomicU64,
    config: LocalBackendConfig,
    /// 后台清理线程停止信号（Drop 时发送并 join）。
    prune_stop: Option<mpsc::Sender<()>>,
    prune_worker: Option<JoinHandle<()>>,
}

impl LocalBackend {
    /// 用默认配置构造。
    pub fn new() -> Self {
        Self::with_config(LocalBackendConfig::default())
    }

    /// 用给定配置构造。`cleanup_interval` 为 `Some` 时启动后台清理线程。
    pub fn with_config(config: LocalBackendConfig) -> Self {
        let entries: Arc<DashMap<String, Entry>> = Arc::new(DashMap::new());
        let (prune_stop, prune_worker) = match config.cleanup_interval {
            Some(interval) if !interval.is_zero() => {
                let entries_for_thread = Arc::clone(&entries);
                let (tx, rx) = mpsc::channel::<()>();
                let worker = thread::spawn(move || {
                    while rx.recv_timeout(interval).is_err() {
                        let now = now_ms();
                        entries_for_thread
                            .retain(|_, e| e.expires_at_ms > now);
                    }
                });
                (Some(tx), Some(worker))
            }
            _ => (None, None),
        };
        Self {
            entries,
            generations: DashMap::new(),
            seq: AtomicU64::new(0),
            config,
            prune_stop,
            prune_worker,
        }
    }

    /// 当前条目数（含可能过期的）。
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// 命名空间数（generation 已注册数）。
    pub fn namespace_count(&self) -> usize {
        self.generations.len()
    }

    fn entry_key(digest: &str) -> String {
        format!("entry:{digest}")
    }

    fn generation_key(namespace: &str) -> String {
        let hash = blake3::hash(namespace.as_bytes()).to_hex();
        format!("generation:{hash}")
    }

    /// 读 generation（缺失视为 0）。
    fn read_generation(&self, namespace: &str) -> u64 {
        self.generations
            .get(namespace)
            .map(|a| a.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    /// 原子递增 generation。
    fn bump(&self, namespace: &str) -> u64 {
        let entry = self
            .generations
            .entry(namespace.to_owned())
            .or_insert_with(|| Arc::new(AtomicU64::new(0)));
        entry.fetch_add(1, Ordering::AcqRel).saturating_add(1)
    }

    /// put 前调用：先清过期条目，仍超上限则按策略驱逐。
    fn evict_if_needed(&self, now: u64) {
        let Some(max) = self.config.max_entries else {
            return;
        };
        if self.entries.len() < max {
            return;
        }
        // 先回收已过期条目。
        self.entries.retain(|_, e| e.expires_at_ms > now);
        // 仍超限则按策略驱逐，直到低于上限。
        while self.entries.len() >= max {
            let victim = self.victim_key();
            match victim {
                Some(k) => {
                    self.entries.remove(&k);
                }
                None => break,
            }
        }
    }

    /// 按驱逐策略选出受害者。
    ///
    /// LFU 参考 Hutool `LFUCache.pruneCache`：先对所有条目减去最小访问次数
    /// （公平计数器），再驱逐访问最少且最旧的条目。
    fn victim_key(&self) -> Option<String> {
        match self.config.eviction {
            EvictionStrategy::Fifo => self
                .entries
                .iter()
                .min_by_key(|e| e.insert_seq)
                .map(|e| e.key().clone()),
            EvictionStrategy::Lfu => {
                let min_access = self.entries.iter().map(|e| e.accesses).min();
                if let Some(min_access) = min_access {
                    for mut e in self.entries.iter_mut() {
                        e.accesses = e.accesses.saturating_sub(min_access);
                    }
                }
                self.entries
                    .iter()
                    .min_by_key(|e| (e.accesses, e.insert_seq))
                    .map(|e| e.key().clone())
            }
            EvictionStrategy::Lru => self
                .entries
                .iter()
                .min_by_key(|e| e.last_access_seq)
                .map(|e| e.key().clone()),
        }
    }
}

impl Drop for LocalBackend {
    fn drop(&mut self) {
        if let Some(stop) = self.prune_stop.take() {
            let _ = stop.send(());
        }
        if let Some(worker) = self.prune_worker.take() {
            let _ = worker.join();
        }
    }
}

impl Default for LocalBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl CacheBackend for LocalBackend {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>>> {
        Box::pin(async move {
            let storage_key = Self::entry_key(key);
            let now = now_ms();
            let value = self.entries.get(&storage_key).map(|e| {
                if e.expires_at_ms > now {
                    Some(e.bytes.clone())
                } else {
                    None
                }
            });
            // lazy cleanup
            if let Some(entry) = self.entries.get(&storage_key) {
                if entry.expires_at_ms <= now {
                    drop(entry);
                    self.entries
                        .remove_if(&storage_key, |_, e| e.expires_at_ms <= now);
                }
            }
            // LRU/LFU：命中时刷新最近访问序号与访问次数。
            if let Some(mut e) = self.entries.get_mut(&storage_key) {
                e.last_access_seq = self.seq.fetch_add(1, Ordering::Relaxed);
                e.accesses = e.accesses.saturating_add(1);
            }
            Ok(value.flatten())
        })
    }

    fn put<'a>(&'a self, key: &'a str, value: Vec<u8>, ttl: Duration) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let storage_key = Self::entry_key(key);
            let ttl = if ttl.is_zero() {
                Duration::from_secs(60)
            } else {
                ttl
            };
            let now = now_ms();
            self.evict_if_needed(now);
            let seq = self.seq.fetch_add(1, Ordering::Relaxed);
            let entry = Entry {
                bytes: value,
                expires_at_ms: now.saturating_add(ttl.as_millis() as u64),
                insert_seq: seq,
                last_access_seq: seq,
                accesses: 0,
            };
            self.entries.insert(storage_key, entry);
            Ok(())
        })
    }

    fn generation<'a>(&'a self, namespace: &'a str) -> BoxFuture<'a, Result<u64>> {
        Box::pin(async move { Ok(self.read_generation(namespace)) })
    }

    fn bump_generation<'a>(&'a self, namespace: &'a str) -> BoxFuture<'a, Result<u64>> {
        Box::pin(async move {
            let new = self.bump(namespace);
            // 在 entries 里同步存一份，便于调试观察 generation 增长。
            let storage_key = Self::generation_key(namespace);
            let now = now_ms();
            self.evict_if_needed(now);
            let seq = self.seq.fetch_add(1, Ordering::Relaxed);
            self.entries.insert(
                storage_key,
                Entry {
                    bytes: new.to_le_bytes().to_vec(),
                    expires_at_ms: u64::MAX,
                    insert_seq: seq,
                    last_access_seq: seq,
                    accesses: 0,
                },
            );
            Ok(new)
        })
    }
}

impl std::fmt::Debug for LocalBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalBackend")
            .field("entries", &self.entries.len())
            .field("namespaces", &self.generations.len())
            .field("seq", &self.seq)
            .field("prune_active", &self.prune_worker.is_some())
            .field("prune_stop_signal", &self.prune_stop.is_some())
            .field("config", &self.config)
            .finish()
    }
}

/// Unix epoch 毫秒。
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;

    fn put(b: &LocalBackend, key: &str, payload: u8) {
        b.put(key, vec![payload], Duration::from_secs(60)).now_or_never().unwrap().unwrap();
    }

    fn get(b: &LocalBackend, key: &str) -> Option<u8> {
        b.get(key).now_or_never().unwrap().unwrap().map(|v| v[0])
    }

    #[test]
    fn no_limit_keeps_all() {
        let b = LocalBackend::new();
        for i in 0..10 {
            put(&b, &format!("k{i}"), i);
        }
        assert_eq!(b.entry_count(), 10);
        assert_eq!(get(&b, "k0"), Some(0));
    }

    #[test]
    fn fifo_evicts_oldest_inserted() {
        let b = LocalBackend::with_config(
            LocalBackendConfig::default().with_max_entries_fifo(3),
        );
        put(&b, "a", 1);
        put(&b, "b", 2);
        put(&b, "c", 3);
        put(&b, "d", 4); // 驱逐最早写入的 "a"
        assert_eq!(b.entry_count(), 3);
        assert_eq!(get(&b, "a"), None, "FIFO must evict the oldest inserted");
        assert_eq!(get(&b, "b"), Some(2));
        assert_eq!(get(&b, "d"), Some(4));
    }

    #[test]
    fn lru_evicts_least_recently_used() {
        let b = LocalBackend::with_config(
            LocalBackendConfig::default().with_max_entries_lru(3),
        );
        put(&b, "a", 1);
        put(&b, "b", 2);
        put(&b, "c", 3);
        // 访问 "a" 使其成为最近使用；再插入 "d" 应驱逐 "b"（最久未访问）。
        let _ = get(&b, "a");
        put(&b, "d", 4);
        assert_eq!(b.entry_count(), 3);
        assert_eq!(get(&b, "b"), None, "LRU must evict the least recently used");
        assert_eq!(get(&b, "a"), Some(1));
        assert_eq!(get(&b, "c"), Some(3));
        assert_eq!(get(&b, "d"), Some(4));
    }

    #[test]
    fn lfu_evicts_least_frequently_used() {
        let b = LocalBackend::with_config(
            LocalBackendConfig::default().with_max_entries_lfu(3),
        );
        put(&b, "a", 1);
        put(&b, "b", 2);
        put(&b, "c", 3);
        // 高频访问 "a" 与 "c"，"b" 保持低频；插入 "d" 应驱逐 "b"。
        let _ = get(&b, "a");
        let _ = get(&b, "a");
        let _ = get(&b, "c");
        put(&b, "d", 4);
        assert_eq!(b.entry_count(), 3);
        assert_eq!(get(&b, "b"), None, "LFU must evict the least frequently used");
        assert_eq!(get(&b, "a"), Some(1));
        assert_eq!(get(&b, "c"), Some(3));
        assert_eq!(get(&b, "d"), Some(4));
    }

    #[test]
    fn lfu_fairness_subtracts_min_access() {
        // Hutool LFUCache.pruneCache 语义：满时先对所有条目减去最小访问次数，
        // 保证新条目（0 次）不会被老条目（高次数）长期排挤。
        let b = LocalBackend::with_config(
            LocalBackendConfig::default().with_max_entries_lfu(2),
        );
        put(&b, "a", 1);
        put(&b, "b", 2);
        let _ = get(&b, "a");
        let _ = get(&b, "a");
        // 满（2/2）：插入 "c" 触发驱逐。减最小次数(0)后 a=2,b=0 → 驱逐 b。
        put(&b, "c", 3);
        assert_eq!(get(&b, "b"), None, "min-access subtraction must still evict the lowest");
        assert_eq!(get(&b, "a"), Some(1));
        assert_eq!(get(&b, "c"), Some(3));
    }

    #[test]
    fn background_cleanup_removes_expired() {
        // 短 TTL + 短清理间隔：过期条目由后台线程回收。
        let b = LocalBackend::with_config(
            LocalBackendConfig::default()
                .with_cleanup_interval(Some(Duration::from_millis(30))),
        );
        b.put("k", vec![1], Duration::from_millis(20))
            .now_or_never()
            .unwrap()
            .unwrap();
        assert_eq!(b.entry_count(), 1);
        std::thread::sleep(Duration::from_millis(120));
        assert_eq!(b.entry_count(), 0, "background prune must remove expired entries");
    }

    #[test]
    fn background_cleanup_stops_on_drop() {
        // Drop 后后台线程停止：再 sleep 不应 panic / 泄漏（句柄 join 验证）。
        let b = LocalBackend::with_config(
            LocalBackendConfig::default().with_cleanup_interval(Some(Duration::from_millis(10))),
        );
        put(&b, "k", 1);
        drop(b);
        std::thread::sleep(Duration::from_millis(40));
    }
}

