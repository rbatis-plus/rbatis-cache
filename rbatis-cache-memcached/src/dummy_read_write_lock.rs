//! 空读写锁。
//!
//! 对应 Java：`org.mybatis.caches.memcached.DummyReadWriteLock`
//! （位于 `/workspace-github/memcached-cache/src/main/java/org/mybatis/caches/memcached/DummyReadWriteLock.java`）。

use std::sync::Mutex;

/// 空读写锁：read 与 write 都通过同一个 [`Mutex`] 串行化，语义上等价于
/// "不保护任何并发"。
pub struct DummyReadWriteLock {
    state: Mutex<()>,
}

impl DummyReadWriteLock {
    /// 构造新锁。
    pub fn new() -> Self {
        Self {
            state: Mutex::new(()),
        }
    }

    /// 获取读锁（Java: `ReentrantReadWriteLock#readLock()`）。
    pub fn read_lock(&self) -> std::sync::MutexGuard<'_, ()> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// 获取写锁（Java: `ReentrantReadWriteLock#writeLock()`）。
    pub fn write_lock(&self) -> std::sync::MutexGuard<'_, ()> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Default for DummyReadWriteLock {
    fn default() -> Self {
        Self::new()
    }
}
