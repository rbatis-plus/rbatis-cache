//! `rbatis-redis` — Redis 分布式 backend。
//!
//! Java 对照：`org.mybatis.caches.redis.*`（位于
//! `/workspace-github/redis-cache/src/main/java/org/mybatis/caches/redis/`）。
//!
//! | Java 文件 | Rust 文件 |
//! |---|---|
//! | `RedisCache.java` | `redis_cache.rs` |
//! | `RedisConfig.java` | `redis_config.rs` |
//! | `RedisConfigurationBuilder.java` | `configuration_builder.rs` |
//! | `Serializer.java` + `JDKSerializer.java` + `KryoSerializer.java` | `serializer.rs` |
//! | `RedisCallback.java` | `redis_callback.rs` |
//! | `DummyReadWriteLock.java` | `dummy_read_write_lock.rs` |

#![forbid(unsafe_code)]

mod configuration_builder;
mod dummy_read_write_lock;
mod redis_cache;
mod redis_callback;
mod redis_config;
mod serializer;

pub use configuration_builder::RedisConfigurationBuilder;
pub use dummy_read_write_lock::DummyReadWriteLock;
pub use redis_cache::{RedisCacheBackend, RedisMetricsSnapshot};
pub use redis_callback::RedisCallback;
pub use redis_config::{RedisCacheConfig, RedisConfig};
pub use serializer::{
    serializer_by_name, JdkSerializer, KryoSerializer, Serializer, SerializerImpl,
};
