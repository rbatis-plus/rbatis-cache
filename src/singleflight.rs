//! 跨拦截器钩子的 per-key singleflight。
//!
//! 执行器集成层是两段式（`before` 查缓存 → 放行 DB → `after` 回填），
//! 同一 key 的并发 miss 必须等"leader"的 `after` 写完缓存后才能命中。
//! 本实现用 `DashMap<digest, LoadState>` 选举 leader：
//!
//! - `try_begin_load` 返回 [`LoadRole::Leader`] 的请求继续走 DB；
//! - [`LoadRole::Follower`] 阻塞在 `Notify` 上等待 leader 的
//!   `complete_load`（无论成败都会唤醒），唤醒后由调用方 re-check 缓存。
//!
//! 对应 MyBatis `BlockingCache` 的防击穿语义，但用通知而非轮询。

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::{Mutex, Notify};

/// follower 单次等待上限；超时后调用方可自行降级发查询。
pub const FOLLOWER_WAIT_TIMEOUT: Duration = Duration::from_secs(5);

/// 一次加载的共享状态。
#[derive(Debug)]
pub struct LoadState {
    state: Mutex<LoadValue>,
    notify: Notify,
}

#[derive(Debug)]
enum LoadValue {
    Loading,
    Done,
    Failed,
}

/// `try_begin_load` 的选举结果。
#[derive(Debug)]
pub enum LoadRole {
    /// 本请求当选 leader，继续走数据库加载（由 `after` 完成回填）。
    Leader,
    /// 本请求为 follower，等待 leader 完成后 re-check 缓存。
    Follower(Arc<LoadState>),
}

/// 跨钩子 singleflight。
#[derive(Debug, Default)]
pub struct SingleFlight {
    map: DashMap<String, Arc<LoadState>>,
}

impl SingleFlight {
    /// 构造空的 singleflight 表。
    pub fn new() -> Self {
        Self::default()
    }

    /// 尝试选举为 leader。同 digest 已有加载中状态时返回 follower。
    pub fn try_begin_load(&self, digest: &str) -> LoadRole {
        match self.map.entry(digest.to_owned()) {
            dashmap::mapref::entry::Entry::Vacant(v) => {
                // 直接以 Loading 初始化（async 上下文中禁止 blocking_lock）。
                let state = Arc::new(LoadState {
                    state: Mutex::new(LoadValue::Loading),
                    notify: Notify::new(),
                });
                v.insert(Arc::clone(&state));
                LoadRole::Leader
            }
            dashmap::mapref::entry::Entry::Occupied(o) => LoadRole::Follower(Arc::clone(o.get())),
        }
    }

    /// 结束一次加载（无论成败都调用），唤醒全部 follower。
    /// 仅在 leader 的 `after` 钩子中调用；对非 leader 是 no-op。
    pub fn complete_load(&self, digest: &str, success: bool) {
        let Some(state) = self.map.get(digest) else {
            return;
        };
        let new_state = if success {
            LoadValue::Done
        } else {
            LoadValue::Failed
        };
        if let Ok(mut s) = state.state.try_lock() {
            *s = new_state;
        }
        state.notify.notify_waiters();
        // 状态已终止，移除条目（follower 已持有 Arc 引用不受影响）。
        drop(state);
        self.map.remove(digest);
    }

    /// 当前进行中的加载数（诊断用）。
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// 是否无进行中的加载。
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl LoadState {
    /// 等待 leader 完成（超时返回）。
    pub async fn wait(&self, timeout: Duration) {
        // 双检查：状态已非 Loading 则无需等待。
        {
            let s = self.state.lock().await;
            if !matches!(*s, LoadValue::Loading) {
                return;
            }
        }
        let _ = tokio::time::timeout(timeout, self.notify.notified()).await;
    }

    /// leader 成功完成（供集成层在 `after` 中标记，payload 由 re-check 获取）。
    pub async fn mark_done(&self) {
        *self.state.lock().await = LoadValue::Done;
        self.notify.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_is_leader_second_is_follower() {
        let sf = SingleFlight::new();
        let digest = "abc";
        assert!(matches!(sf.try_begin_load(digest), LoadRole::Leader));
        assert!(matches!(sf.try_begin_load(digest), LoadRole::Follower(_)));
        // 完成后条目被清理，后续请求重新成为 leader。
        sf.complete_load(digest, true);
        assert!(matches!(sf.try_begin_load(digest), LoadRole::Leader));
    }

    #[tokio::test]
    async fn wait_releases_on_complete() {
        let state = Arc::new(LoadState {
            state: Mutex::new(LoadValue::Loading),
            notify: Notify::new(),
        });
        let state2 = Arc::clone(&state);
        let done = Arc::new(Notify::new());
        let done2 = Arc::clone(&done);
        tokio::spawn(async move {
            state2.wait(Duration::from_secs(5)).await;
            done2.notify_one();
        });
        *state.state.lock().await = LoadValue::Done;
        state.notify.notify_waiters();
        tokio::time::timeout(Duration::from_secs(1), done.notified())
            .await
            .expect("wait must be released by complete");
    }
}
