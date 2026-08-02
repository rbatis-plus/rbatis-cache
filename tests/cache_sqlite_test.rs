//! 执行器集成层在**真实 sqlite 数据库**上的端到端验证。
//!
//! 与 `cache_test.rs`（计数 MockDriver）互补：这里走完整 rbdc-sqlite
//! 驱动 + 真实 SQL 执行，确认缓存命中/失效/事务行为在真实数据上正确，
//! 不污染数据（每次用独立的内存库）。

#![allow(mismatched_lifetime_syntaxes)]

use rbatis::rbdc::rt::block_on;
use rbatis::RBatis;
use rbdc_sqlite::SqliteDriver;
use rbs::Value;
use std::sync::Arc;
use std::time::Duration;

use rbatis_cache::{
    CachePolicy, LocalBackend, RbatisCacheExt, RbatisCacheInterceptor,
};

/// 建表 + 安装缓存，返回 (RBatis, 拦截器指标句柄)。
fn setup() -> (RBatis, Arc<rbatis_cache::CacheMetrics>) {
    let rb = RBatis::new();
    let cache = RbatisCacheInterceptor::new(
        "sqlite_ns",
        Arc::new(LocalBackend::new()),
        CachePolicy::default().with_ttl(Duration::from_secs(60)),
    );
    let metrics = cache.metrics();
    let listener = cache.listener();
    rb.install_cache(Arc::new(cache), Some(Arc::new(listener)));
    let rb_clone = rb.clone();
    block_on(async move {
        rb_clone
            .link(SqliteDriver {}, "sqlite://:memory:")
            .await
            .unwrap();
        rb_clone
            .exec(
                "CREATE TABLE IF NOT EXISTS cache_item (id INTEGER PRIMARY KEY, name TEXT)",
                vec![],
            )
            .await
            .unwrap();
        for i in 0..3 {
            rb_clone
                .exec(
                    "INSERT INTO cache_item (id, name) VALUES (?, ?)",
                    vec![Value::I32(i), Value::String(format!("item-{i}"))],
                )
                .await
                .unwrap();
        }
    });
    (rb, metrics)
}

#[test]
fn real_query_hits_cache_second_time() {
    let (rb, metrics) = setup();
    block_on(async move {
        let v1 = rb
            .query(
                "SELECT name FROM cache_item WHERE id = ?",
                vec![Value::I32(0)],
            )
            .await
            .unwrap();
        assert!(v1.to_string().contains("item-0"), "real data: {v1}");

        // 第二次查询必须命中缓存（L2 回填后），数据一致。
        let v2 = rb
            .query(
                "SELECT name FROM cache_item WHERE id = ?",
                vec![Value::I32(0)],
            )
            .await
            .unwrap();
        assert_eq!(v1, v2, "cached value must equal first result");
    });
    let m = metrics.snapshot();
    assert_eq!(m.misses, 1, "first query misses, got {m:?}");
    assert!(m.hits >= 1, "second query must hit cache, got {m:?}");
}

#[test]
fn real_dml_invalidates_cache() {
    let (rb, metrics) = setup();
    block_on(async move {
        // 预热缓存
        let _ = rb
            .query(
                "SELECT name FROM cache_item WHERE id = ?",
                vec![Value::I32(1)],
            )
            .await
            .unwrap();
        let m = metrics.snapshot();
        assert_eq!(m.misses, 1, "warm-up must miss, got {m:?}");

        // DML（真实执行）：必须 bump generation 使缓存失效
        rb.exec(
            "UPDATE cache_item SET name = ? WHERE id = ?",
            vec![Value::String("updated-1".to_string()), Value::I32(1)],
        )
        .await
        .unwrap();

        // 再次查询：缓存已失效，走 DB 拿到新数据
        let v = rb
            .query(
                "SELECT name FROM cache_item WHERE id = ?",
                vec![Value::I32(1)],
            )
            .await
            .unwrap();
        assert!(v.to_string().contains("updated-1"), "fresh data: {v}");
        let m = metrics.snapshot();
        assert_eq!(m.misses, 2, "DML must invalidate cache, got {m:?}");
    });
}

#[test]
fn real_tx_bypass_then_commit_invalidates() {
    let (rb, metrics) = setup();
    block_on(async move {
        // 预热缓存
        let _ = rb
            .query(
                "SELECT name FROM cache_item WHERE id = ?",
                vec![Value::I32(2)],
            )
            .await
            .unwrap();

        // 事务内 DML（Bypass 模式：事务内查询不读缓存；commit 后 bump）
        let tx = rb.acquire_begin().await.unwrap();
        tx.exec(
            "UPDATE cache_item SET name = ? WHERE id = ?",
            vec![Value::String("tx-updated".to_string()), Value::I32(2)],
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();

        // 提交后查询：缓存必须已失效，拿到新数据
        let v = rb
            .query(
                "SELECT name FROM cache_item WHERE id = ?",
                vec![Value::I32(2)],
            )
            .await
            .unwrap();
        assert!(v.to_string().contains("tx-updated"), "post-commit fresh: {v}");
        let m = metrics.snapshot();
        assert_eq!(m.misses, 2, "commit must invalidate cache, got {m:?}");
    });
}
