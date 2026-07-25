//! 日志装饰器 backend。
//!
//! 对应 Java：`org.mybatis.caches.memcached.LoggingMemcachedCache`
//! （位于 `/workspace-github/memcached-cache/src/main/java/org/mybatis/caches/memcached/LoggingMemcachedCache.java`）。
//!
//! Java 侧 `LoggingMemcachedCache` 是装饰器，在每个方法里打日志；本
//! crate 由于 [`CacheBackend`] 已用 `tracing/log` 可观测，不再做日志
//! 装饰器——保留同名类型作为占位以便跨语言对照。

#![allow(missing_docs)]

/// 日志装饰器：占位实现。
///
/// 对应 `LoggingMemcachedCache#LoggingMemcachedCache(MemcachedCache)`。
/// 本 crate 中无对应增强（无 Java 对应）：保留为单元结构体。
pub struct LoggingMemcachedCache;

impl LoggingMemcachedCache {
    /// 构造装饰器。
    pub fn new() -> Self {
        Self
    }
}

impl Default for LoggingMemcachedCache {
    fn default() -> Self {
        Self::new()
    }
}
