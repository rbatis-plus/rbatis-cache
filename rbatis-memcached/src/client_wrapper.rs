//! Memcached 客户端封装。
//!
//! 对应 Java：`org.mybatis.caches.memcached.MemcachedClientWrapper`
//! （位于 `/workspace-github/memcached-cache/src/main/java/org/mybatis/caches/memcached/MemcachedClientWrapper.java`）。
//!
//! Java 侧的关键设计是 **group 失效机制**：memcached 没有"按前缀批量
//! 删除"接口，每个 cache id 通过 `incr` 一个 group counter 让旧 key
//! 自动失效（实际键形如 `{prefix}_{groupId}_{key}`）。
//!
//! 本 crate 在 `rbatis-cache` SPI 上对应为：每条 [`CacheKey`] 的
//! generation 计数（参见 [`crate::memcached_cache`]）即同一思想。

#![allow(missing_docs)]

use std::sync::Arc;

use memcache::Client;

use crate::compressor_transcoder::CompressorTranscoder;

/// 客户端封装：保存一个同步 memcache `Client` 与压缩开关。
///
/// 对应 `MemcachedClientWrapper`（保留构造/getObject/putObject/removeObject
/// 公开方法面，字段命名与 Java 等价）。
pub struct MemcachedClientWrapper {
    pub(crate) client: Arc<Client>,
    compression: bool,
}

impl MemcachedClientWrapper {
    /// 构造：从服务器 URL 列表建立客户端。
    pub fn new(
        servers: Vec<(String, u16)>,
        compression: bool,
    ) -> Result<Self, memcache::MemcacheError> {
        let urls: Vec<String> = servers
            .into_iter()
            .map(|(host, port)| format!("memcache://{host}:{port}?timeout=2"))
            .collect();
        let client = Client::connect(urls)?;
        Ok(Self {
            client: Arc::new(client),
            compression,
        })
    }

    /// 写：与 Java 侧 `putObject(key, value, id)` 等价，但 `id` 通过
    /// [`key_prefix`] 表达，不依赖 group（group 失效由 SPI 的 generation 承担）。
    ///
    /// 对应 Java: `MemcachedClientWrapper#putObject(Object, Object, String)`。
    pub fn put_object(
        &self,
        key: &str,
        value: &[u8],
        ttl_seconds: u32,
    ) -> Result<(), memcache::MemcacheError> {
        let payload = if self.compression {
            CompressorTranscoder::encode(value).unwrap_or_else(|_| value.to_vec())
        } else {
            value.to_vec()
        };
        self.client.set(key, payload.as_slice(), ttl_seconds)
    }

    /// 读。
    ///
    /// 对应 Java: `MemcachedClientWrapper#getObject(Object)`。
    pub fn get_object(&self, key: &str) -> Result<Option<Vec<u8>>, memcache::MemcacheError> {
        let value = self.client.get::<Vec<u8>>(key)?;
        let value = match value {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        if self.compression {
            Ok(CompressorTranscoder::decode(&value).ok())
        } else {
            Ok(Some(value))
        }
    }

    /// 删。
    ///
    /// 对应 Java: `MemcachedClientWrapper#removeObject(Object)`。
    pub fn remove_object(&self, key: &str) -> Result<bool, memcache::MemcacheError> {
        self.client.delete(key)
    }
}

impl std::fmt::Debug for MemcachedClientWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemcachedClientWrapper")
            .field("compression", &self.compression)
            .finish_non_exhaustive()
    }
}
