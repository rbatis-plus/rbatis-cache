//! 缓存条目的线缆格式：MessagePack envelope。
//!
//! 对应 Java 包 `org.mybatis.caches.redis.serializer.Serializer`：
//! MyBatis-Redis 把 Java 序列化交给 `JDKSerializer` / `KryoSerializer`，
//! 本 crate 统一采用 [`MessagePack`](rmp_serde)（更紧凑、跨语言、与 rbs::Value
//! 类型自然对应），并在 envelope 内显式携带 version / generation / expires_at_ms
//! / table_tags，便于任何 backend 透明地解释存储内容。

use std::collections::BTreeSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::CacheError;
use crate::key::CacheKey;
use crate::Result;

/// 单条缓存条目在 backend 中的字节表示。
///
/// - `version`：协议版本，未来若变更编解码格式，可作兼容性开关。
/// - `generation`：构造时的 namespace generation，使条目自动随失效失效。
/// - `expires_at_ms`：Unix epoch 毫秒；由 [`CacheEnvelope::is_fresh`] 判定。
/// - `table_tags`：构造时的关系名集合（冗余存储以便诊断 / 未来表级失效）。
/// - `payload`：原始数据库 / 加密状态的字节（由调用方约定）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheEnvelope {
    /// 协议版本。
    pub version: u16,
    /// 构造时的 namespace generation。
    pub generation: u64,
    /// 过期时间（Unix epoch 毫秒）。
    pub expires_at_ms: u64,
    /// 关系名集合。
    pub table_tags: BTreeSet<String>,
    /// 载荷字节。
    pub payload: Vec<u8>,
}

impl CacheEnvelope {
    /// 用给定的 TTL 与 key 的元数据构造 envelope。
    ///
    /// 对应 Java 侧 `Serializer#serialize` 之外的协议头生成逻辑。
    pub fn new(key: &CacheKey, payload: Vec<u8>, ttl: Duration) -> Self {
        Self {
            version: 1,
            generation: key.generation(),
            expires_at_ms: now_ms().saturating_add(duration_ms(ttl)),
            table_tags: key.table_tags().clone(),
            payload,
        }
    }

    /// 编码为 MessagePack 字节流。
    ///
    /// 对应 Java 侧 `KryoSerializer#writeObject` / `JDKSerializer#writeObject`。
    pub fn encode(&self) -> Result<Vec<u8>> {
        rmp_serde::to_vec_named(self).map_err(|error| CacheError::Codec(error.to_string()))
    }

    /// 从 MessagePack 字节流解码。
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        rmp_serde::from_slice(bytes).map_err(|error| CacheError::Codec(error.to_string()))
    }

    /// 当前 generation 与时间下是否仍然新鲜。
    pub fn is_fresh(&self, generation: u64) -> bool {
        self.version == 1 && self.generation == generation && self.expires_at_ms > now_ms()
    }
}

/// 当前 Unix epoch 毫秒。系统时间回退时返回 0，使 `is_fresh` 拒绝旧条目。
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

/// Duration -> 毫秒。溢出饱和到 `u64::MAX`。
fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}
