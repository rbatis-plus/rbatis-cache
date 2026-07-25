//! 一致性哈希环。
//!
//! ## Rust 侧增强（无 Java 对应）
//!
//! Java 侧由 spymemcached 内部 ketama-hash 路由，无需应用层关心。本
//! crate 在 [`MemcachedCacheBackend`] 之外显式提供一个简单一致性哈希
//! 实现，便于上层做"指定路由" / 多节点探活等扩展。
//!
//! 实现：使用 BLAKE3 在 `(node, replica)` 元组上做 64 位哈希，按值
//! 排序插入 `BTreeMap`，查询时取首个 >= 查询 key 的点。

#![allow(missing_docs)]

use std::collections::BTreeMap;

/// 一致性哈希环。
#[derive(Debug, Clone)]
pub struct ConsistentHashRing {
    /// 虚拟点 hash -> 节点下标。
    points: BTreeMap<u64, usize>,
    /// 节点数。
    node_count: usize,
}

impl ConsistentHashRing {
    /// 构造：每个节点放 `virtual_nodes` 个虚拟点。
    ///
    /// 对应 ketama 算法的常见默认（160 虚拟点）。
    pub fn new(node_count: usize, virtual_nodes: u16) -> Self {
        let mut points = BTreeMap::new();
        for node in 0..node_count {
            for replica in 0..virtual_nodes {
                let key = format!("node:{node}:replica:{replica}");
                let hash = hash(&key);
                points.insert(hash, node);
            }
        }
        Self { points, node_count }
    }

    /// 选节点：取首个 >= hash 的点；否则环绕到首点。
    pub fn node_for(&self, key: &str) -> usize {
        let point = hash(key);
        self.points
            .range(point..)
            .next()
            .or_else(|| self.points.first_key_value())
            .map_or(0, |(_, node)| *node)
    }

    /// 节点数。
    pub fn node_count(&self) -> usize {
        self.node_count
    }
}

fn hash(input: &str) -> u64 {
    let bytes = blake3::hash(input.as_bytes());
    let bytes = bytes.as_bytes();
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(buf)
}
