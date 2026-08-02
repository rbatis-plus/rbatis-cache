//! Memcached backend 契约测试（复用 rbatis-cache `testing` harness）。
//!
//! 需要本地 memcached（默认端口 11211），默认 `#[ignore]`。运行方式：
//! ```sh
//! memcached -d
//! cargo test -p rbatis-memcached -- --ignored
//! ```

use rbatis_cache::testing;
use rbatis_memcached::{MemcachedCacheBackend, MemcachedConfiguration};

fn connect() -> MemcachedCacheBackend {
    MemcachedCacheBackend::connect("contract-test", MemcachedConfiguration::default())
        .expect("connect to local memcached must succeed")
}

#[tokio::test]
#[ignore = "requires a local memcached on 127.0.0.1:11211"]
async fn contract_missing_key_is_none() {
    let backend = connect();
    testing::assert_missing_key_is_none(&backend).await;
}

#[tokio::test]
#[ignore = "requires a local memcached on 127.0.0.1:11211"]
async fn contract_get_put_roundtrip() {
    let backend = connect();
    testing::assert_get_put_roundtrip(&backend).await;
}

#[tokio::test]
#[ignore = "requires a local memcached on 127.0.0.1:11211"]
async fn contract_generation_atomic() {
    let backend = connect();
    testing::assert_generation_atomic(&backend).await;
}

#[tokio::test]
#[ignore = "requires a local memcached on 127.0.0.1:11211"]
async fn contract_ttl_expires() {
    let backend = connect();
    testing::assert_ttl_expires(&backend).await;
}
