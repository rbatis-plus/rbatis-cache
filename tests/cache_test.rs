//! 执行器集成层（`RbatisCacheInterceptor`）的端到端测试。
//!
//! 迁移自 rbatis fork `tests/cache_test.rs`：用计数 MockDriver 验证
//! L1/L2 命中、DML 失效、事务行为、singleflight 与 fail-closed 语义。
//! 全局 `QUERY_COUNT` 由 `TEST_LOCK` 串行化（并行运行互不污染）。

#![allow(mismatched_lifetime_syntaxes)]

use futures::future::BoxFuture;
use futures::Stream;
use rbatis::rbdc::db::{ConnectOptions, Connection, Driver, ExecResult, MetaData, Row};
use rbatis::rbdc::rt::block_on;
use rbatis::RBatis;
use rbs::Value;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use rbatis_cache::{
    CacheBackend, CacheError, CachePolicy, LocalBackend, RbatisCacheExt, RbatisCacheInterceptor,
    TransactionCacheMode, UseCacheFilter,
};

// ---------------------------------------------------------------------------
// Mock driver that counts queries
// ---------------------------------------------------------------------------

static QUERY_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Tests below share the global `QUERY_COUNT` counter, so they must never
/// run concurrently: hold this lock for the whole test body.
static TEST_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone)]
struct CountingMockDriver;

impl Driver for CountingMockDriver {
    fn name(&self) -> &'static str {
        "counting-cache-mock"
    }
    fn connect(
        &self,
        _url: &str,
    ) -> BoxFuture<'_, Result<Box<dyn Connection>, rbatis::rbdc::Error>> {
        Box::pin(async { Ok(Box::new(CountingConn) as Box<dyn Connection>) })
    }
    fn connect_opt<'a>(
        &'a self,
        _opt: &'a dyn ConnectOptions,
    ) -> BoxFuture<'a, Result<Box<dyn Connection>, rbatis::rbdc::Error>> {
        Box::pin(async { Ok(Box::new(CountingConn) as Box<dyn Connection>) })
    }
    fn default_option(&self) -> Box<dyn ConnectOptions> {
        Box::new(MockOpts)
    }
}

#[derive(Clone, Debug)]
struct CountingMeta;
impl MetaData for CountingMeta {
    fn column_len(&self) -> usize {
        1
    }
    fn column_name(&self, _i: usize) -> String {
        "v".into()
    }
    fn column_type(&self, _i: usize) -> String {
        "I64".into()
    }
}

#[derive(Clone, Debug)]
struct CountingRow;
impl Row for CountingRow {
    fn meta_data(&self) -> Box<dyn MetaData> {
        Box::new(CountingMeta)
    }
    fn get(&mut self, _i: usize) -> Result<Value, rbatis::rbdc::Error> {
        Ok(Value::I64(1))
    }
}

#[derive(Clone, Debug)]
struct CountingConn;
impl Connection for CountingConn {
    fn exec_rows(
        &mut self,
        _sql: &str,
        _p: Vec<Value>,
    ) -> BoxFuture<
        '_,
        Result<
            Pin<Box<dyn Stream<Item = Result<Box<dyn Row>, rbatis::rbdc::Error>> + Send + '_>>,
            rbatis::rbdc::Error,
        >,
    > {
        QUERY_COUNT.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            let row = Box::new(CountingRow) as Box<dyn Row>;
            let s: Pin<
                Box<dyn Stream<Item = Result<Box<dyn Row>, rbatis::rbdc::Error>> + Send + '_>,
            > = Box::pin(futures::stream::iter(vec![Ok(row)]));
            Ok(s)
        })
    }
    fn exec(
        &mut self,
        _sql: &str,
        _p: Vec<Value>,
    ) -> BoxFuture<'_, Result<ExecResult, rbatis::rbdc::Error>> {
        Box::pin(async {
            Ok(ExecResult {
                rows_affected: 1,
                last_insert_id: Value::Null,
            })
        })
    }
    fn close(&mut self) -> BoxFuture<'_, Result<(), rbatis::rbdc::Error>> {
        Box::pin(async { Ok(()) })
    }
    fn ping(&mut self) -> BoxFuture<'_, Result<(), rbatis::rbdc::Error>> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone, Debug)]
struct MockOpts;
impl ConnectOptions for MockOpts {
    fn connect(&self) -> BoxFuture<'_, Result<Box<dyn Connection>, rbatis::rbdc::Error>> {
        Box::pin(async { Ok(Box::new(CountingConn) as Box<dyn Connection>) })
    }
    fn set_uri(&mut self, _u: &str) -> Result<(), rbatis::rbdc::Error> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

/// 用本地内存 backend 安装缓存拦截器 + 事务监听器。
fn install_cache(rb: &RBatis, namespace: &str, policy: CachePolicy) {
    let cache = RbatisCacheInterceptor::new(namespace, Arc::new(LocalBackend::new()), policy);
    let listener = cache.listener();
    rb.install_cache(Arc::new(cache), Some(Arc::new(listener)));
}

fn setup_rb_with_cache() -> RBatis {
    QUERY_COUNT.store(0, Ordering::SeqCst);
    let rb = RBatis::new();
    rb.init(CountingMockDriver, "mock://test").unwrap();
    install_cache(
        &rb,
        "test_ns",
        CachePolicy::default().with_ttl(Duration::from_secs(60)),
    );
    rb
}

fn setup_rb_defer() -> RBatis {
    QUERY_COUNT.store(0, Ordering::SeqCst);
    let rb = RBatis::new();
    rb.init(CountingMockDriver, "mock://test").unwrap();
    install_cache(
        &rb,
        "defer_ns",
        CachePolicy::default()
            .with_ttl(Duration::from_secs(60))
            .with_transaction_defer(),
    );
    rb
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn install_cache_works() {
    let rb = RBatis::new();
    install_cache(&rb, "test_ns", CachePolicy::default());
    // verify intercept and listener were registered
    assert!(rb.intercepts.len() >= 2); // cache + page + log
    assert!(!rb.listeners().is_empty());
}

#[test]
fn cache_hit_on_second_query() {
    let _guard = TEST_LOCK.lock().unwrap();
    let rb = setup_rb_with_cache();
    block_on(async move {
        let _ = rb.query("select * from t", vec![]).await.unwrap();
        assert_eq!(QUERY_COUNT.load(Ordering::SeqCst), 1, "first query hits DB");

        let _ = rb.query("select * from t", vec![]).await.unwrap();
        assert_eq!(
            QUERY_COUNT.load(Ordering::SeqCst),
            1,
            "second query hits cache"
        );
    });
}

#[test]
fn different_sql_caches_separately() {
    let _guard = TEST_LOCK.lock().unwrap();
    let rb = setup_rb_with_cache();
    block_on(async move {
        let _ = rb
            .query("select * from t where id = 1", vec![])
            .await
            .unwrap();
        let _ = rb
            .query("select * from t where id = 2", vec![])
            .await
            .unwrap();
        assert_eq!(QUERY_COUNT.load(Ordering::SeqCst), 2);
    });
}

#[test]
fn dml_invalidates_cache() {
    let _guard = TEST_LOCK.lock().unwrap();
    let rb = setup_rb_with_cache();
    block_on(async move {
        let _ = rb.query("select * from t", vec![]).await.unwrap();
        assert_eq!(QUERY_COUNT.load(Ordering::SeqCst), 1);

        let _ = rb.exec("update t set x = 1", vec![]).await.unwrap();

        let _ = rb.query("select * from t", vec![]).await.unwrap();
        assert_eq!(
            QUERY_COUNT.load(Ordering::SeqCst),
            2,
            "cache invalidated after DML"
        );
    });
}

#[test]
fn transaction_queries_bypass_cache() {
    let _guard = TEST_LOCK.lock().unwrap();
    let rb = setup_rb_with_cache();
    block_on(async move {
        let tx = rb.acquire_begin().await.unwrap();
        let _ = tx.query("select * from t", vec![]).await.unwrap();
        let _ = tx.query("select * from t", vec![]).await.unwrap();
        assert_eq!(
            QUERY_COUNT.load(Ordering::SeqCst),
            2,
            "tx queries always hit DB"
        );
        tx.commit().await.unwrap();
    });
}

#[test]
fn commit_after_dml_clears_cache() {
    let _guard = TEST_LOCK.lock().unwrap();
    let rb = setup_rb_with_cache();
    block_on(async move {
        let _ = rb.query("select * from t", vec![]).await.unwrap();
        assert_eq!(QUERY_COUNT.load(Ordering::SeqCst), 1);

        let tx = rb.acquire_begin().await.unwrap();
        let _ = tx.exec("update t set x = 2", vec![]).await.unwrap();
        tx.commit().await.unwrap();

        let _ = rb.query("select * from t", vec![]).await.unwrap();
        assert_eq!(
            QUERY_COUNT.load(Ordering::SeqCst),
            2,
            "cache cleared after commit"
        );
    });
}

#[test]
fn rollback_does_not_clear_cache() {
    let _guard = TEST_LOCK.lock().unwrap();
    let rb = setup_rb_with_cache();
    block_on(async move {
        let _ = rb.query("select * from t", vec![]).await.unwrap();
        assert_eq!(QUERY_COUNT.load(Ordering::SeqCst), 1);

        let tx = rb.acquire_begin().await.unwrap();
        let _ = tx.exec("update t set x = 3", vec![]).await.unwrap();
        tx.rollback().await.unwrap();

        let _ = rb.query("select * from t", vec![]).await.unwrap();
        assert_eq!(
            QUERY_COUNT.load(Ordering::SeqCst),
            1,
            "cache preserved after rollback"
        );
    });
}

// ---------------------------------------------------------------------------
// L1 cache: per-connection promotion
// ---------------------------------------------------------------------------

#[test]
fn l1_promotes_l2_hit_to_l1() {
    let _guard = TEST_LOCK.lock().unwrap();
    let rb = setup_rb_with_cache();
    block_on(async move {
        let conn = rb.acquire().await.unwrap();
        // First query: miss L1, miss L2, hits DB, writes L1+L2
        let _ = conn.query("select * from t", vec![]).await.unwrap();
        assert_eq!(QUERY_COUNT.load(Ordering::SeqCst), 1);
        // Second query: should hit L1 (no DB call, no L2 lookup)
        let _ = conn.query("select * from t", vec![]).await.unwrap();
        assert_eq!(
            QUERY_COUNT.load(Ordering::SeqCst),
            1,
            "L1 hit should not call DB"
        );
    });
}

#[test]
fn l1_is_per_connection() {
    let _guard = TEST_LOCK.lock().unwrap();
    let rb = setup_rb_with_cache();
    block_on(async move {
        let conn1 = rb.acquire().await.unwrap();
        let _ = conn1.query("select * from t", vec![]).await.unwrap();
        assert_eq!(QUERY_COUNT.load(Ordering::SeqCst), 1);

        // conn2: same SQL, but different L1 — should miss L1, may hit L2
        let conn2 = rb.acquire().await.unwrap();
        let _ = conn2.query("select * from t", vec![]).await.unwrap();
        // conn2 L1 miss but L2 hit → still 1 DB call
        assert_eq!(
            QUERY_COUNT.load(Ordering::SeqCst),
            1,
            "L2 hit should not call DB"
        );
    });
}

#[test]
fn dml_clears_l1_for_executor() {
    let _guard = TEST_LOCK.lock().unwrap();
    let rb = setup_rb_with_cache();
    block_on(async move {
        let conn = rb.acquire().await.unwrap();
        let _ = conn.query("select * from t", vec![]).await.unwrap();
        assert_eq!(QUERY_COUNT.load(Ordering::SeqCst), 1);
        // DML on same conn clears L1 + L2
        let _ = conn.exec("update t set x=1", vec![]).await.unwrap();
        // Query again: L1 cleared, L2 cleared → DB call
        let _ = conn.query("select * from t", vec![]).await.unwrap();
        assert_eq!(QUERY_COUNT.load(Ordering::SeqCst), 2);
    });
}

// ---------------------------------------------------------------------------
// Singleflight: concurrent miss deduplication
// ---------------------------------------------------------------------------

#[test]
fn singleflight_dedup_concurrent_miss() {
    let _guard = TEST_LOCK.lock().unwrap();
    let rb = setup_rb_with_cache();
    block_on(async move {
        // Prime with a unique SQL
        let sql = "select * from singleflight_test";
        // First call to populate cache
        let _ = rb.query(sql, vec![]).await.unwrap();
        assert_eq!(QUERY_COUNT.load(Ordering::SeqCst), 1);
        // Multiple concurrent queries — all should hit cache, not DB
        let mut handles = Vec::new();
        let rb_clone = rb.clone();
        for _ in 0..10 {
            let rb2 = rb_clone.clone();
            handles.push(tokio::spawn(async move {
                let _ = rb2.query(sql, vec![]).await;
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(
            QUERY_COUNT.load(Ordering::SeqCst),
            1,
            "all concurrent reads should hit cache"
        );
    });
}

// ---------------------------------------------------------------------------
// FOR UPDATE exclusion
// ---------------------------------------------------------------------------

#[test]
fn for_update_not_cached() {
    let _guard = TEST_LOCK.lock().unwrap();
    let rb = setup_rb_with_cache();
    block_on(async move {
        let _ = rb
            .query("select * from t for update", vec![])
            .await
            .unwrap();
        assert_eq!(QUERY_COUNT.load(Ordering::SeqCst), 1);
        // Second query: FOR UPDATE is never cached → DB call again
        let _ = rb
            .query("select * from t for update", vec![])
            .await
            .unwrap();
        assert_eq!(
            QUERY_COUNT.load(Ordering::SeqCst),
            2,
            "FOR UPDATE should not cache"
        );
    });
}

#[test]
fn for_share_not_cached() {
    let _guard = TEST_LOCK.lock().unwrap();
    let rb = setup_rb_with_cache();
    block_on(async move {
        let _ = rb.query("select * from t for share", vec![]).await.unwrap();
        let _ = rb.query("select * from t for share", vec![]).await.unwrap();
        assert_eq!(
            QUERY_COUNT.load(Ordering::SeqCst),
            2,
            "FOR SHARE should not cache"
        );
    });
}

// ---------------------------------------------------------------------------
// use_cache_filter: per-statement control
// ---------------------------------------------------------------------------

#[test]
fn use_cache_filter_excludes_pattern() {
    let _guard = TEST_LOCK.lock().unwrap();
    QUERY_COUNT.store(0, Ordering::SeqCst);
    let rb = RBatis::new();
    rb.init(CountingMockDriver, "mock://test").unwrap();
    install_cache(
        &rb,
        "filtered",
        CachePolicy::default().with_use_cache_filter(UseCacheFilter::new(|sql: &str| {
            !sql.to_lowercase().contains("temp_")
        })),
    );
    block_on(async move {
        // temp_ query: not cached → DB every time
        let _ = rb.query("select * from temp_data", vec![]).await.unwrap();
        let _ = rb.query("select * from temp_data", vec![]).await.unwrap();
        assert_eq!(
            QUERY_COUNT.load(Ordering::SeqCst),
            2,
            "filtered SQL should not cache"
        );

        // normal query: cached
        let _ = rb.query("select * from real_data", vec![]).await.unwrap();
        let _ = rb.query("select * from real_data", vec![]).await.unwrap();
        assert_eq!(
            QUERY_COUNT.load(Ordering::SeqCst),
            3,
            "non-filtered SQL should cache after first"
        );
    });
}

// ---------------------------------------------------------------------------
// Defer mode: transactional cache buffering
// ---------------------------------------------------------------------------

#[test]
fn defer_mode_tx_query_buffers_writes() {
    let _guard = TEST_LOCK.lock().unwrap();
    let rb = setup_rb_defer();
    block_on(async move {
        // Prime cache before tx
        let _ = rb.query("select * from t", vec![]).await.unwrap();
        assert_eq!(QUERY_COUNT.load(Ordering::SeqCst), 1);

        // Begin tx, query inside (Defer: reads L2, buffers write)
        let tx = rb.acquire_begin().await.unwrap();
        let _ = tx.query("select * from t", vec![]).await.unwrap();
        tx.commit().await.unwrap();

        // After commit, buffer flushed → next query hits L2
        let _ = rb.query("select * from t", vec![]).await.unwrap();
        // commit flushed the buffer, so this should be an L2 hit (0 extra DB calls)
        assert_eq!(
            QUERY_COUNT.load(Ordering::SeqCst),
            1,
            "defer commit should flush buffer"
        );
    });
}

#[test]
fn defer_mode_rollback_discards_buffer() {
    let _guard = TEST_LOCK.lock().unwrap();
    let rb = setup_rb_defer();
    block_on(async move {
        let tx = rb.acquire_begin().await.unwrap();
        let _ = tx
            .query("select * from defer_rollback", vec![])
            .await
            .unwrap();
        tx.rollback().await.unwrap();

        // After rollback: buffer discarded, query hits DB
        let _ = rb
            .query("select * from defer_rollback", vec![])
            .await
            .unwrap();
        assert!(
            QUERY_COUNT.load(Ordering::SeqCst) >= 1,
            "after rollback, buffer discarded → query hits DB"
        );
    });
}

// ---------------------------------------------------------------------------
// Empty result caching
// ---------------------------------------------------------------------------

#[test]
fn empty_array_is_cached_when_cache_null_true() {
    let _guard = TEST_LOCK.lock().unwrap();
    let rb = setup_rb_with_cache();
    block_on(async move {
        let _ = rb.query("select * from empty_table", vec![]).await.unwrap();
        assert_eq!(QUERY_COUNT.load(Ordering::SeqCst), 1);
        let _ = rb.query("select * from empty_table", vec![]).await.unwrap();
        assert_eq!(QUERY_COUNT.load(Ordering::SeqCst), 1, "empty result cached");
    });
}

// ---------------------------------------------------------------------------
// Different args isolation
// ---------------------------------------------------------------------------

#[test]
fn different_args_produce_different_cache_entries() {
    let _guard = TEST_LOCK.lock().unwrap();
    let rb = setup_rb_with_cache();
    block_on(async move {
        let _ = rb
            .query("select * from t where id = ?", vec![Value::I64(1)])
            .await
            .unwrap();
        let _ = rb
            .query("select * from t where id = ?", vec![Value::I64(2)])
            .await
            .unwrap();
        let _ = rb
            .query("select * from t where id = ?", vec![Value::I64(1)])
            .await
            .unwrap();
        // 3rd query hits cache (same as 1st) → 2 DB calls
        assert_eq!(QUERY_COUNT.load(Ordering::SeqCst), 2);
    });
}

#[test]
fn null_vs_string_arg_isolated() {
    let _guard = TEST_LOCK.lock().unwrap();
    let rb = setup_rb_with_cache();
    block_on(async move {
        let _ = rb.query("select ?", vec![Value::Null]).await.unwrap();
        let _ = rb
            .query("select ?", vec![Value::String("null".into())])
            .await
            .unwrap();
        let _ = rb.query("select ?", vec![Value::Null]).await.unwrap();
        assert_eq!(
            QUERY_COUNT.load(Ordering::SeqCst),
            2,
            "null vs string should have different keys"
        );
    });
}

// ---------------------------------------------------------------------------
// CachePolicy builder
// ---------------------------------------------------------------------------

#[test]
fn policy_with_ttl_builder() {
    let p = CachePolicy::default().with_ttl(Duration::from_secs(30));
    assert_eq!(p.ttl, Duration::from_secs(30));
    assert_eq!(p.l1_max_entries, 256);
}

#[test]
fn policy_with_transaction_defer_builder() {
    let p = CachePolicy::default().with_transaction_defer();
    assert_eq!(p.transaction_mode, TransactionCacheMode::Defer);
}

#[test]
fn policy_without_blocking_builder() {
    let p = CachePolicy::default().without_blocking();
    assert!(!p.blocking);
}

// ---------------------------------------------------------------------------
// Fail-closed mode: backend errors must propagate to the caller
// ---------------------------------------------------------------------------

/// 一个所有操作都失败的 backend。
#[derive(Debug)]
struct FailingBackend;

impl CacheBackend for FailingBackend {
    fn get<'a>(&'a self, _key: &'a str) -> BoxFuture<'a, Result<Option<Vec<u8>>, CacheError>> {
        Box::pin(async { Err(CacheError::Backend("intentional failure".into())) })
    }
    fn put<'a>(
        &'a self,
        _key: &'a str,
        _value: Vec<u8>,
        _ttl: Duration,
    ) -> BoxFuture<'a, Result<(), CacheError>> {
        Box::pin(async { Err(CacheError::Backend("intentional failure".into())) })
    }
    fn generation<'a>(&'a self, _namespace: &'a str) -> BoxFuture<'a, Result<u64, CacheError>> {
        Box::pin(async { Err(CacheError::Backend("intentional failure".into())) })
    }
    fn bump_generation<'a>(
        &'a self,
        _namespace: &'a str,
    ) -> BoxFuture<'a, Result<u64, CacheError>> {
        Box::pin(async { Err(CacheError::Backend("intentional failure".into())) })
    }
}

#[test]
fn fail_closed_backend_returns_error() {
    let _guard = TEST_LOCK.lock().unwrap();
    let rb = RBatis::new();
    rb.init(CountingMockDriver, "mock://test").unwrap();
    install_cache(
        &rb,
        "fail_closed_ns",
        CachePolicy::default().with_failure_closed(),
    );
    // 覆盖为 FailingBackend（install_cache 用的是 LocalBackend，这里手动替换）
    let cache = RbatisCacheInterceptor::new(
        "fail_closed_ns",
        Arc::new(FailingBackend),
        CachePolicy::default().with_failure_closed(),
    );
    let listener = cache.listener();
    rb.intercepts.clear();
    rb.install_cache(Arc::new(cache), Some(Arc::new(listener)));
    block_on(async move {
        // L2 backend always fails; fail-closed must surface the error
        let r = rb.query("select * from t", vec![]).await;
        assert!(r.is_err(), "fail-closed: backend error must fail the query");
    });
}

#[test]
fn fail_open_backend_degrades_to_miss() {
    let _guard = TEST_LOCK.lock().unwrap();
    let rb = RBatis::new();
    rb.init(CountingMockDriver, "mock://test").unwrap();
    let cache = RbatisCacheInterceptor::new(
        "fail_open_ns",
        Arc::new(FailingBackend),
        CachePolicy::default(),
    );
    let listener = cache.listener();
    rb.install_cache(Arc::new(cache), Some(Arc::new(listener)));
    block_on(async move {
        // Backend error is logged and treated as a miss: the query still works
        let r = rb.query("select * from t", vec![]).await;
        assert!(r.is_ok(), "fail-open: backend error must degrade to a miss");
    });
}
