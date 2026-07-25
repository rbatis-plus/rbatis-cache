//! SQL 解析与分类。
//!
//! 对应 Java 包 `org.mybatis.caches.*` 各适配器对 SQL 的预处理：
//! MyBatis 主流程负责 `Executor.isCached(...)` 判断，本 crate 在 SPI 层面
//! 提供保守的双重保险——只有 `Statement::Query` 单语句才允许进入缓存。
//!
//! 关系（表名）抽取用于构造失效标签 `table_tags`，与 MyBatis 的 table
//! flush 概念对齐；当配置 `InvalidationStrategy::TableGeneration` 时，
//! 任何对该表的写入都能精确失效相关条目。

#![allow(missing_docs)]

use std::collections::BTreeSet;
use std::ops::ControlFlow;

use sqlparser::ast::{visit_relations, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::error::CacheError;
use crate::Result;

/// 语句类别。`Select` 进入缓存；`Other` 一律放行。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementKind {
    /// 解析得到恰好一个 SELECT 查询。
    Select,
    /// 写操作、DDL、多语句或无法解析。
    Other,
}

/// 规范化后的 SQL 元数据：canonical_sql / table_tags / kind。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlMetadata {
    /// 解析器渲染的稳定 SQL（参数占位符已规范化）。
    pub canonical_sql: String,
    /// 解析器访问到的全部关系名（小写、去重、排序）。
    pub table_tags: BTreeSet<String>,
    /// 语句分类。
    pub kind: StatementKind,
}

impl SqlMetadata {
    /// 使用 `sqlparser` 解析 SQL。
    ///
    /// 对应 `mybatis-redis` / `mybatis-memcached` 中类似的概念性入口：Java
    /// 侧由 MyBatis `Executor` 在调用缓存前完成分类，本 crate 自行解析以
    /// 保证 backend 无关。
    pub fn parse(sql: &str) -> Result<Self> {
        let statements = Parser::parse_sql(&GenericDialect {}, sql)
            .map_err(|error| CacheError::Sql(error.to_string()))?;
        let kind = if statements.len() == 1 && matches!(statements[0], Statement::Query(_)) {
            StatementKind::Select
        } else {
            StatementKind::Other
        };
        let mut table_tags = BTreeSet::new();
        // 用 sqlparser 的访问者遍历所有关系（FROM / JOIN 子句等）。
        let _: ControlFlow<()> = visit_relations(&statements, |relation| {
            table_tags.insert(relation.to_string().to_ascii_lowercase());
            ControlFlow::Continue(())
        });
        Ok(Self {
            // 多个语句以 "; " 连接；单语句则与原文一致（解析器规范化后输出）。
            canonical_sql: statements
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; "),
            table_tags,
            kind,
        })
    }
}
