//! Executable cache safety and concurrency contract.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use futures::future::BoxFuture;
use rbatis_cache::{
    CacheBackend, CacheError, CacheInterceptor, CacheKey, CacheKeyInput, CachePolicy, CacheRequest,
    CacheResult, SqlMetadata, StatementKind,
};

#[derive(Default)]
struct MemoryBackend {
    values: DashMap<String, Vec<u8>>,
    generations: DashMap<String, u64>,
    fail: AtomicBool,
}

impl CacheBackend for MemoryBackend {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, CacheResult<Option<Vec<u8>>>> {
        Box::pin(async move {
            self.available()?;
            Ok(self.values.get(key).map(|value| value.clone()))
        })
    }

    fn put<'a>(
        &'a self,
        key: &'a str,
        value: Vec<u8>,
        _ttl: Duration,
    ) -> BoxFuture<'a, CacheResult<()>> {
        Box::pin(async move {
            self.available()?;
            self.values.insert(key.to_owned(), value);
            Ok(())
        })
    }

    fn generation<'a>(&'a self, namespace: &'a str) -> BoxFuture<'a, CacheResult<u64>> {
        Box::pin(async move {
            self.available()?;
            Ok(self.generations.get(namespace).map_or(0, |value| *value))
        })
    }

    fn bump_generation<'a>(&'a self, namespace: &'a str) -> BoxFuture<'a, CacheResult<u64>> {
        Box::pin(async move {
            self.available()?;
            let mut generation = self.generations.entry(namespace.to_owned()).or_insert(0);
            *generation = generation.saturating_add(1);
            Ok(*generation)
        })
    }
}

impl MemoryBackend {
    fn available(&self) -> CacheResult<()> {
        if self.fail.load(Ordering::Relaxed) {
            Err(CacheError::Backend("unavailable".to_owned()))
        } else {
            Ok(())
        }
    }
}

fn key_input() -> CacheKeyInput<'static> {
    CacheKeyInput {
        version: "v1",
        data_source: "primary",
        driver: "sqlite",
        tenant: Some("tenant-a"),
        namespace: "order.mapper",
        statement_id: "find_by_id",
        sql: "select o.id from orders o join customers c on c.id = o.customer_id where o.id = ?",
        parameters: b"[42]",
    }
}

#[test]
fn parser_extracts_relations_and_key_isolates_every_boundary() {
    let metadata = SqlMetadata::parse(key_input().sql).unwrap();
    assert_eq!(metadata.kind, StatementKind::Select);
    assert_eq!(
        metadata.table_tags.into_iter().collect::<Vec<_>>(),
        ["customers", "orders"]
    );

    let first = CacheKey::build(key_input(), 1).unwrap();
    let mut isolated = key_input();
    isolated.tenant = Some("tenant-b");
    let second = CacheKey::build(isolated, 1).unwrap();
    let next_generation = CacheKey::build(key_input(), 2).unwrap();
    assert_ne!(first.digest(), second.digest());
    assert_ne!(first.digest(), next_generation.digest());
}

#[tokio::test]
async fn transaction_reads_bypass_cache() {
    let backend = Arc::new(MemoryBackend::default());
    let cache = CacheInterceptor::new(backend, CachePolicy::default());
    let loads = AtomicU64::new(0);
    for _ in 0..2 {
        let value = cache
            .get_or_load(
                CacheRequest {
                    key: key_input(),
                    in_transaction: true,
                },
                || async {
                    loads.fetch_add(1, Ordering::Relaxed);
                    Ok(b"database".to_vec())
                },
            )
            .await
            .unwrap();
        assert_eq!(value, b"database");
    }
    assert_eq!(loads.load(Ordering::Relaxed), 2);
    assert_eq!(cache.metrics().snapshot().bypasses, 2);
}

#[tokio::test]
async fn generation_invalidation_forces_one_new_load() {
    let backend = Arc::new(MemoryBackend::default());
    let cache = CacheInterceptor::new(backend, CachePolicy::default());
    let loads = AtomicU64::new(0);
    for expected in [1_u64, 1] {
        let value = cache
            .get_or_load(
                CacheRequest {
                    key: key_input(),
                    in_transaction: false,
                },
                || async {
                    let loaded = loads.fetch_add(1, Ordering::Relaxed) + 1;
                    Ok(loaded.to_le_bytes().to_vec())
                },
            )
            .await
            .unwrap();
        assert_eq!(u64::from_le_bytes(value.try_into().unwrap()), expected);
    }
    cache
        .invalidate_after_commit(key_input().namespace)
        .await
        .unwrap();
    let value = cache
        .get_or_load(
            CacheRequest {
                key: key_input(),
                in_transaction: false,
            },
            || async {
                let loaded = loads.fetch_add(1, Ordering::Relaxed) + 1;
                Ok(loaded.to_le_bytes().to_vec())
            },
        )
        .await
        .unwrap();
    assert_eq!(u64::from_le_bytes(value.try_into().unwrap()), 2);
}

#[tokio::test]
async fn concurrent_misses_are_singleflighted() {
    let cache = Arc::new(CacheInterceptor::new(
        Arc::new(MemoryBackend::default()),
        CachePolicy::default(),
    ));
    let loads = Arc::new(AtomicU64::new(0));
    let mut tasks = Vec::new();
    for _ in 0..16 {
        let cache = Arc::clone(&cache);
        let loads = Arc::clone(&loads);
        tasks.push(tokio::spawn(async move {
            cache
                .get_or_load(
                    CacheRequest {
                        key: key_input(),
                        in_transaction: false,
                    },
                    || async move {
                        loads.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        Ok(b"one-load".to_vec())
                    },
                )
                .await
                .unwrap()
        }));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), b"one-load");
    }
    assert_eq!(loads.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn backend_failure_is_fail_open_and_observable() {
    let backend = Arc::new(MemoryBackend::default());
    backend.fail.store(true, Ordering::Relaxed);
    let cache = CacheInterceptor::new(backend, CachePolicy::default());
    let value = cache
        .get_or_load(
            CacheRequest {
                key: key_input(),
                in_transaction: false,
            },
            || async { Ok(b"database-still-works".to_vec()) },
        )
        .await
        .unwrap();
    assert_eq!(value, b"database-still-works");
    assert_eq!(cache.metrics().snapshot().backend_errors, 1);
}
