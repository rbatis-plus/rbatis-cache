//! Redis backend 契约测试（复用 rbatis-cache `testing` harness）。
//!
//! 需要本地 redis-server，默认 `#[ignore]`。运行方式：
//! ```sh
//! redis-server --daemonize yes
//! cargo test -p rbatis-redis -- --ignored
//! ```

use rbatis_cache::testing;
use rbatis_cache_redis::{RedisCacheBackend, RedisCacheConfig, RedisConfig};

async fn connect() -> RedisCacheBackend {
    let config =
        RedisCacheConfig::from_redis(RedisConfig::standalone().with_url("redis://127.0.0.1:6379"));
    RedisCacheBackend::connect("contract-test", config)
        .await
        .expect("connect to local redis must succeed")
}

#[tokio::test]
#[ignore = "requires a local redis-server on 127.0.0.1:6379"]
async fn contract_missing_key_is_none() {
    let backend = connect().await;
    testing::assert_missing_key_is_none(&backend).await;
}

#[tokio::test]
#[ignore = "requires a local redis-server on 127.0.0.1:6379"]
async fn contract_get_put_roundtrip() {
    let backend = connect().await;
    testing::assert_get_put_roundtrip(&backend).await;
}

#[tokio::test]
#[ignore = "requires a local redis-server on 127.0.0.1:6379"]
async fn contract_generation_atomic() {
    let backend = connect().await;
    testing::assert_generation_atomic(&backend).await;
}

#[tokio::test]
#[ignore = "requires a local redis-server on 127.0.0.1:6379"]
async fn contract_ttl_expires() {
    let backend = connect().await;
    testing::assert_ttl_expires(&backend).await;
}
