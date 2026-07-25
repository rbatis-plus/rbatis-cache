//! 空读写锁。
//!
//! 对应 Java：`org.mybatis.caches.redis.DummyReadWriteLock`
//! （位于 `/workspace-github/redis-cache/src/main/java/org/mybatis/caches/redis/DummyReadWriteLock.java`）。
//!
//! Java 侧 MyBatis 的 `Cache#getReadWriteLock()` 约定可返回空锁——本
//! crate 不暴露该约定，但保留同源类型以便上层装饰器在跨语言移植时
//! 仍能找到对等件。

#![allow(missing_docs)]

use std::sync::Mutex;

/// 空读写锁：read 与 write 都通过同一个 [`Mutex`] 串行化，语义上等价于
/// "不保护任何并发"。
///
/// 对应 `DummyReadWriteLock#readLock() / writeLock()`。
pub struct DummyReadWriteLock {
    /// 内部状态锁，仅做占位。
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
