//! 序列化器抽象。
//!
//! 对应 Java：`org.mybatis.caches.redis.Serializer`
//! （位于 `/workspace-github/redis-cache/src/main/java/org/mybatis/caches/redis/Serializer.java`）。
//!
//! ## 设计差异说明
//! Java 侧由 MyBatis-Redis 提供两种实现：`JDKSerializer`（Java 原生
//! 序列化）与 `KryoSerializer`（Kryo5 高性能序列化），由 `redis.properties`
//! 中 `redis.serializer` 选择。
//!
//! 本 crate 不重写上述两种序列化器——`rbatis-cache` 的 [`CacheEnvelope`]
//! 统一使用 MessagePack，跨 backend 保持一致格式。这是 Rust 侧的设计
//! 选择（无 Java 直接对应实现），文件中以注释说明。

#![allow(missing_docs)]

/// 序列化器接口：与 backend 解耦的字节 ↔ 对象转换层。
///
/// 在本 crate 中由 [`rbatis_cache::CacheEnvelope`] 承担，留 trait 仅
/// 满足"与 Java 同名类型存在"的对照表语义。
pub trait Serializer: Send + Sync {
    /// 将对象序列化为字节。
    fn serialize(&self, value: &[u8]) -> Result<Vec<u8>, String>;

    /// 将字节反序列化为对象。
    fn deserialize(&self, bytes: &[u8]) -> Result<Vec<u8>, String>;
}

/// Java 侧默认序列化器（Java 原生序列化）的对位占位。
///
/// 真实实现由 [`rbatis_cache::CacheEnvelope`] 提供 MessagePack 编解码，
/// 这里只保留 trait 实现以维持 Java 对照。
pub struct JdkSerializer;

impl Serializer for JdkSerializer {
    fn serialize(&self, value: &[u8]) -> Result<Vec<u8>, String> {
        // 直接透传字节：Java 侧会把 Object 转成 byte[]，本 crate 的 envelope
        // payload 已经是 MessagePack 字节流，序列化层对它无操作。
        Ok(value.to_vec())
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<Vec<u8>, String> {
        Ok(bytes.to_vec())
    }
}

/// Java 侧 Kryo 序列化器的对位占位。
///
/// 同 [`JdkSerializer`]，实际字节转换由 core 承担。
pub struct KryoSerializer;

impl Serializer for KryoSerializer {
    fn serialize(&self, value: &[u8]) -> Result<Vec<u8>, String> {
        Ok(value.to_vec())
    }
    fn deserialize(&self, bytes: &[u8]) -> Result<Vec<u8>, String> {
        Ok(bytes.to_vec())
    }
}
