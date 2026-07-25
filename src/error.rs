//! 缓存契约的错误模型。
//!
//! 对应 Java 包 `org.mybatis.caches.*` 中各适配器的异常体系：Java 侧用
//! 自定义异常类（`RedisCacheException`、`MemcachedException` 等），本
//! crate 用统一枚举表达所有 backend 在 SPI 边界上抛出的错误类别。
//!
//! 所有 backend 必须把内部错误统一映射到这四种变体之一，绝不向上
//! 泄漏底层 client 库的细节（Redis 错误码、Memcached 协议错误等）。

#![allow(missing_docs)]

use std::fmt;

/// 缓存层统一错误类型。
///
/// ## 变体语义
///
/// - `Sql`：sqlparser 解析失败 / 无法安全分类（保守缓存策略选择放行）。
/// - `Codec`：`CacheEnvelope` 的 MessagePack 编解码失败（数据被篡改或版本不兼容）。
/// - `Backend`：底层 backend（Redis / Memcached / Moka）操作失败。
/// - `Loader`：调用方提供的数据库加载闭包失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheError {
    /// SQL 解析失败或无法安全分类。
    Sql(String),
    /// `CacheEnvelope`（MessagePack）编解码失败。
    Codec(String),
    /// 后端 backend 操作失败。
    Backend(String),
    /// 调用方数据库 loader 失败。
    Loader(String),
}

impl fmt::Display for CacheError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CacheError::Sql(message) => write!(formatter, "cache SQL parse failed: {message}"),
            CacheError::Codec(message) => {
                write!(formatter, "cache envelope codec failed: {message}")
            }
            CacheError::Backend(message) => write!(formatter, "cache backend failed: {message}"),
            CacheError::Loader(message) => write!(formatter, "cache loader failed: {message}"),
        }
    }
}

impl std::error::Error for CacheError {}
