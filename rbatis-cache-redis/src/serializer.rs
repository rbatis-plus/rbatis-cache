//! 序列化器抽象与两种实现。
//!
//! 对应 Java：
//! - `org.mybatis.caches.redis.Serializer`（`Serializer.java`，50 行）
//! - `org.mybatis.caches.redis.JDKSerializer`（`JDKSerializer.java`，57 行）
//! - `org.mybatis.caches.redis.KryoSerializer`（`KryoSerializer.java`，124 行）
//!
//! （位于 `/workspace-github/redis-cache/src/main/java/org/mybatis/caches/redis/`）
//!
//! ## Java 语义与本 crate 的映射
//!
//! | Java | Rust |
//! |---|---|
//! | `Serializer#serialize(Object)` / `unserialize(byte[])` | [`Serializer::serialize`] / [`Serializer::deserialize`] |
//! | `Serializer#reset()`（默认 no-op；Kryo 用 ThreadLocal 持有状态） | [`Serializer::reset`]（默认 no-op） |
//! | `JDKSerializer`：JDK 原生序列化，自描述、带类元数据、体积大 | [`JdkSerializer`]：MessagePack **named**（带字段名，自描述） |
//! | `KryoSerializer`：Kryo5 紧凑二进制，位置编码、体积小、速度快 | [`KryoSerializer`]：MessagePack **compact**（位置编码，无字段名） |
//!
//! Javadoc 语义要点翻译保留：Java `Serializer#reset()` 的文档说明——
//! "使用 `ThreadLocal` 存储的实现（如 `KryoSerializer`）应覆写此方法，
//! 防止 Web 容器（Tomcat 等）中线程复用导致的 ClassLoader 钉死 /
//! Metaspace 泄漏"。Rust 的 serde 实现无线程局部状态，两个实现都
//! 继承默认 no-op，行为与 Javadoc 约定一致。

use serde::de::DeserializeOwned;
use serde::Serialize;

/// 序列化器接口。
///
/// 对应 Java: `Serializer`（函数式接口 + 默认方法）。
pub trait Serializer: Send + Sync {
    /// 把任意可序列化对象编码为字节。
    ///
    /// 对应 Java: `Serializer#serialize(Object)`。
    fn serialize<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, String>;

    /// 把字节解码回对象。
    ///
    /// 对应 Java: `Serializer#unserialize(byte[])`。
    fn deserialize<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, String>;

    /// 释放当前线程持有的序列化器资源（默认 no-op）。
    ///
    /// 对应 Java: `Serializer#reset()`——Javadoc 原文要求"使用
    /// `ThreadLocal` 的实现应覆写本方法以防 Web 容器线程复用导致的
    /// ClassLoader 泄漏"。Rust 实现无线程局部状态，保留默认 no-op。
    fn reset(&self) {}
}

/// 默认序列化器（对应 Java 默认的 `JDKSerializer`）。
///
/// 采用 MessagePack **named** 编码（`rmp_serde::to_vec_named`）：字段以
/// 名字写入，自描述、可读性好、体积偏大——与 JDK 原生序列化"携带
/// 类元数据"的角色一致。
pub struct JdkSerializer;

impl Serializer for JdkSerializer {
    fn serialize<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, String> {
        rmp_serde::to_vec_named(value).map_err(|error| error.to_string())
    }

    fn deserialize<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, String> {
        rmp_serde::from_slice(bytes).map_err(|error| error.to_string())
    }
}

/// 紧凑序列化器（对应 Java 的 `KryoSerializer`）。
///
/// 采用 MessagePack **compact** 编码（`rmp_serde::to_vec`）：字段按声明
/// 顺序位置编码、无字段名，体积最小——与 Kryo5"位置编码 + 类型注册"
/// 的角色一致。注意读写双方必须使用同一类型定义（与 Kryo 的注册
/// 约束等价）。
pub struct KryoSerializer;

impl Serializer for KryoSerializer {
    fn serialize<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, String> {
        rmp_serde::to_vec(value).map_err(|error| error.to_string())
    }

    fn deserialize<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, String> {
        rmp_serde::from_slice(bytes).map_err(|error| error.to_string())
    }
}

/// 序列化器枚举：按配置名分发的统一类型。
///
/// 由于 [`Serializer`] 带泛型方法、不是 dyn-compatible，用枚举承担
/// "按配置选择实现"的角色（对应 Java 侧 `redis.serializer` 的反射
/// 实例化路径）。
pub enum SerializerImpl {
    /// 默认 named 编码（对应 `jdk`）。
    Jdk(JdkSerializer),
    /// 紧凑编码（对应 `kryo`）。
    Kryo(KryoSerializer),
}

impl Serializer for SerializerImpl {
    fn serialize<T: Serialize>(&self, value: &T) -> Result<Vec<u8>, String> {
        match self {
            Self::Jdk(inner) => inner.serialize(value),
            Self::Kryo(inner) => inner.serialize(value),
        }
    }

    fn deserialize<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, String> {
        match self {
            Self::Jdk(inner) => inner.deserialize(bytes),
            Self::Kryo(inner) => inner.deserialize(bytes),
        }
    }
}

/// 按名字选择序列化器（对应 `redis.properties` 中
/// `redis.serializer=jdk|kryo` 的配置项语义）。
pub fn serializer_by_name(name: &str) -> SerializerImpl {
    match name.to_ascii_lowercase().as_str() {
        "kryo" => SerializerImpl::Kryo(KryoSerializer),
        _ => SerializerImpl::Jdk(JdkSerializer),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Serialize, serde::Deserialize)]
    struct Point {
        x: i32,
        y: i32,
    }

    #[test]
    fn jdk_serializer_named_roundtrip() {
        let serializer = JdkSerializer;
        let point = Point { x: 1, y: 2 };
        let bytes = serializer.serialize(&point).expect("serialize");
        let decoded: Point = serializer.deserialize(&bytes).expect("deserialize");
        assert_eq!(decoded, point);
    }

    #[test]
    fn kryo_serializer_compact_roundtrip() {
        let serializer = KryoSerializer;
        let point = Point { x: 1, y: 2 };
        let bytes = serializer.serialize(&point).expect("serialize");
        let decoded: Point = serializer.deserialize(&bytes).expect("deserialize");
        assert_eq!(decoded, point);
    }

    #[test]
    fn compact_is_smaller_than_named() {
        let point = Point { x: 1, y: 2 };
        let named = JdkSerializer.serialize(&point).expect("named");
        let compact = KryoSerializer.serialize(&point).expect("compact");
        assert!(compact.len() < named.len());
    }

    #[test]
    fn reset_is_noop() {
        JdkSerializer.reset();
        KryoSerializer.reset();
    }
}
