//! Redis 回调函数式接口。
//!
//! 对应 Java：`org.mybatis.caches.redis.RedisCallback`
//! （位于 `/workspace-github/redis-cache/src/main/java/org/mybatis/caches/redis/RedisCallback.java`）。
//!
//! Java 侧定义为函数式接口 `T doWithRedis(Jedis jedis)`，本 crate 用
//! 闭包 + 共享 [`redis::aio::ConnectionManager`] 实现等价语义。

use redis::aio::ConnectionManager;
use redis::RedisResult;

/// Redis 回调：在传入的连接上执行一次操作。
///
/// 对应 Java: `RedisCallback#doWithRedis(Jedis)`。
pub type RedisCallback<'a, T> = &'a dyn Fn(&mut ConnectionManager) -> RedisResult<T>;
