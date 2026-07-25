//! 缓存键构造。
//!
//! 对应 Java 包 `org.mybatis.cache.CacheKey`：MyBatis 把缓存键分解为
//! `id + statementId + Sql + parameterMappings + environmentId + ...`，
//! 本 crate 用 [`CacheKeyInput`] 显式枚举所有隔离边界，避免不同数据源、
//! 租户、命名空间下的缓存互相命中。
//!
//! 摘要算法：所有分量长度前缀化后写入 BLAKE3。
//! 与 Caffeine / Redis 适配器同款做法，长度前缀保证 `("ab","c")` 与
//! `("a","bc")` 永不冲突。

#![allow(missing_docs)]

use std::collections::BTreeSet;

use blake3::Hasher;

use crate::sql::SqlMetadata;
use crate::Result;

/// 构造缓存键时所需的全部隔离边界。
///
/// 任何一项变化都必须使最终 digest 改变，否则不同租户/数据源/参数
/// 的查询结果会互相污染。
#[derive(Debug, Clone, Copy)]
pub struct CacheKeyInput<'a> {
    /// 缓存协议版本号（用于跨版本兼容）。
    pub version: &'a str,
    /// 逻辑数据源名（多数据源隔离）。
    pub data_source: &'a str,
    /// 数据库驱动标识（如 `"mysql"`/`"sqlite"`）。
    pub driver: &'a str,
    /// 可选租户边界（多租户隔离）。
    pub tenant: Option<&'a str>,
    /// Mapper / 应用层命名空间（对应 MyBatis cache id）。
    pub namespace: &'a str,
    /// 语句 ID（对应 MyBatis `MappedStatement#getId()` 的本地部分）。
    pub statement_id: &'a str,
    /// 实际送往数据库的 SQL。
    pub sql: &'a str,
    /// 已规范化的参数编码（由调用方决定编码格式，例如 MessagePack）。
    pub parameters: &'a [u8],
}

/// BLAKE3 摘要 + 命名空间/版本元数据。
///
/// digest 是 32 字节 BLAKE3 输出十六进制串：足以对抗意外碰撞，
/// 且因 `update_component` 长度前缀化处理，拼接歧义被消除。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheKey {
    digest: String,
    namespace: String,
    generation: u64,
    table_tags: BTreeSet<String>,
}

impl CacheKey {
    /// 从所有隔离边界构建缓存键。
    ///
    /// 对应 `org.apache.ibatis.cache.CacheKey#update*` 的全部字段
    /// 在本 crate 的一次性等价实现。
    pub fn build(input: CacheKeyInput<'_>, generation: u64) -> Result<Self> {
        let metadata = SqlMetadata::parse(input.sql)?;
        let mut hasher = blake3::Hasher::new();
        // 顺序遍历：版本 -> 数据源 -> 驱动 -> 租户(默认 "-") -> 命名空间 -> 语句ID -> canonical SQL
        for component in [
            input.version,
            input.data_source,
            input.driver,
            input.tenant.unwrap_or("-"),
            input.namespace,
            input.statement_id,
            &metadata.canonical_sql,
        ] {
            update_component(&mut hasher, component.as_bytes());
        }
        // generation 进入哈希，使得 generation bump 后的同一 SQL 立刻无法命中旧条目。
        update_component(&mut hasher, &generation.to_le_bytes());
        // 参数编码：调用方自由选择（推荐 MessagePack + 类型签名），长度前缀保证拼接无歧义。
        update_component(&mut hasher, input.parameters);
        Ok(Self {
            digest: hasher.finalize().to_hex().to_string(),
            namespace: input.namespace.to_owned(),
            generation,
            table_tags: metadata.table_tags,
        })
    }

    /// 后端安全的摘要字符串（十六进制 BLAKE3）。
    pub fn digest(&self) -> &str {
        &self.digest
    }

    /// 命名空间（用于 generation 路由）。
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// 构造该键时采用的 generation。
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// 解析器抽取的关系名集合（用于表级失效或诊断）。
    pub fn table_tags(&self) -> &BTreeSet<String> {
        &self.table_tags
    }
}

/// 把 `bytes` 长度前缀化地写入哈希。
///
/// 长度前缀使两个不同位置不同长度的字节序列永不会产生相同的中间态。
/// 即使长度字段被截断（如 8 字节 u64 溢出）也只会被哈希到不同 digest，
/// 而不会与别的输入碰撞。
fn update_component(hasher: &mut Hasher, bytes: &[u8]) {
    let len = u64::try_from(bytes.len()).unwrap_or(u64::MAX);
    hasher.update(&len.to_le_bytes());
    hasher.update(bytes);
}
