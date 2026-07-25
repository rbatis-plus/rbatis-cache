//! 契约测试 harness：每个 backend 复用同一组标准断言。
//!
//! ## Java 对照
//! Java 侧 MyBatis 适配器没有统一契约测试，每个仓库自带集成测试
//! （`RedisCacheTest` / `MemcachedCacheTest` 等），用真实服务运行。
//!
//! 本 crate 提供一组与 backend 无关的 trait-level 契约断言，**不要求
//! 任何外部服务**——backend 实现方只需把自己的 client 实例包成
//! `Arc<dyn CacheBackend>`，再交给以下四个 async 测试函数即可。
//!
//! ## 用法（在 backend crate 的 dev-dependency 里）
//! ```ignore
//! rbatis-cache = { path = "../..", version = "0.1", features = ["testing"] }
//! ```
//!
//! ```ignore
//! #[tokio::test]
//! async fn contract_misses_for_unknown_key() {
//!     rbatis_cache::testing::assert_missing_key_is_none(&*backend).await;
//! }
//! ```

#![allow(missing_docs)]
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;

use crate::CacheBackend;

/// 标准 backend 测试后端工厂签名。
///
/// backend 测试函数对 backend 的具体类型无知，只依赖 `Arc<dyn CacheBackend>`。
pub type DynBackend = Arc<dyn CacheBackend>;

/// 把任意 backend 转成 `Arc<dyn CacheBackend>`。
///
/// 实现方在测试里调用：
/// `let backend = rbatis_cache::testing::dyn_backend(my_backend);`
pub fn dyn_backend<B: CacheBackend + 'static>(backend: B) -> DynBackend {
    Arc::new(backend)
}

/// **契约 1**：未写入的 key 必须返回 `Ok(None)`，不得报错。
pub async fn assert_missing_key_is_none(backend: &dyn CacheBackend) {
    let result = backend.get("non-existent-digest").await;
    assert!(
        matches!(result, Ok(None)),
        "expected Ok(None) for missing key, got {result:?}",
    );
}

/// **契约 2**：put 后立即 get 应当取回相同字节。
///
/// 覆盖写入应替换而非追加。
pub async fn assert_get_put_roundtrip(backend: &dyn CacheBackend) {
    let payload = b"sample-envelope-bytes".to_vec();
    backend
        .put(
            "contract-key-roundtrip",
            payload.clone(),
            Duration::from_mins(1),
        )
        .await
        .expect("put must succeed");
    let read_back = backend
        .get("contract-key-roundtrip")
        .await
        .expect("get must succeed");
    assert_eq!(read_back.as_deref(), Some(payload.as_slice()));

    // 覆盖写
    let payload2 = b"updated".to_vec();
    backend
        .put(
            "contract-key-roundtrip",
            payload2.clone(),
            Duration::from_mins(1),
        )
        .await
        .expect("second put must succeed");
    let read_back2 = backend
        .get("contract-key-roundtrip")
        .await
        .expect("second get must succeed");
    assert_eq!(read_back2.as_deref(), Some(payload2.as_slice()));
}

/// **契约 3**：generation 必须原子递增，并等于调用次数。
///
/// 后端必须保证：并发 N 次 bump 后 generation == N（单调性 + 原子性）。
pub async fn assert_generation_atomic(backend: &dyn CacheBackend) {
    let namespace = "contract-namespace";
    let before = backend
        .generation(namespace)
        .await
        .expect("generation read");

    let mut handles = Vec::new();
    for _ in 0..32 {
        handles.push(backend.bump_generation(namespace));
    }
    for handle in handles {
        handle.await.expect("bump must succeed");
    }

    let after = backend
        .generation(namespace)
        .await
        .expect("generation read 2");
    assert_eq!(
        after,
        before + 32,
        "32 concurrent bumps must add exactly 32"
    );
}

/// **契约 4**：短 TTL put 后等待，get 必须返回 `Ok(None)`。
///
/// 用于证明 backend 端的 TTL 真正生效，而非只在前端 envelope 检查。
pub async fn assert_ttl_expires(backend: &dyn CacheBackend) {
    let key = "contract-key-ttl";
    backend
        .put(key, b"short-lived".to_vec(), Duration::from_millis(50))
        .await
        .expect("put must succeed");
    tokio::time::sleep(Duration::from_millis(200)).await;
    let after = backend.get(key).await.expect("get must succeed");
    assert!(
        after.is_none(),
        "backend must expire short-TTL entries; got {after:?}",
    );
}

/// 完整契约：一次跑完 4 条断言。
pub async fn run_all(backend: &dyn CacheBackend) {
    assert_missing_key_is_none(backend).await;
    assert_get_put_roundtrip(backend).await;
    assert_generation_atomic(backend).await;
    assert_ttl_expires(backend).await;
}

/// BoxFuture 别名（与 backend impl 中惯用形式保持一致）。
pub fn boxed<T>(
    future: impl std::future::Future<Output = T> + Send + 'static,
) -> BoxFuture<'static, T>
where
    T: Send + 'static,
{
    Box::pin(future)
}
