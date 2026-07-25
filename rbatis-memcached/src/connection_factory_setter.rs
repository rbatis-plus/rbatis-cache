//! ConnectionFactory setter。
//!
//! 对应 Java：`org.mybatis.caches.memcached.ConnectionFactorySetter`
//! （位于 `/workspace-github/memcached-cache/src/main/java/org/mybatis/caches/memcached/ConnectionFactorySetter.java`）。

#![allow(missing_docs)]

use std::fmt;

use crate::abstract_property_setter::{PropertySetter, TypedPropertySetter};
use crate::configuration::{ConnectionFactoryKind, MemcachedConfiguration};

/// [`ConnectionFactoryKind`] 的字符串序列化形式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // alpha 阶段保留 setter registry 接入点
pub struct ConnectionFactoryName(pub ConnectionFactoryKind);

impl fmt::Display for ConnectionFactoryName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let text = match self.0 {
            ConnectionFactoryKind::Binary => "binary",
            ConnectionFactoryKind::Text => "text",
            ConnectionFactoryKind::Sasl => "sasl",
        };
        f.write_str(text)
    }
}

impl std::str::FromStr for ConnectionFactoryName {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input.trim().to_ascii_lowercase().as_str() {
            "binary" => Ok(Self(ConnectionFactoryKind::Binary)),
            "text" => Ok(Self(ConnectionFactoryKind::Text)),
            "sasl" => Ok(Self(ConnectionFactoryKind::Sasl)),
            _ => Err(format!("unknown connection factory: {input}")),
        }
    }
}

/// ConnectionFactory setter：识别协议名并写入 enum。
///
/// 对应 Java: `ConnectionFactorySetter#convert(String)`。
#[allow(dead_code)] // alpha 阶段保留 setter registry 接入点
pub fn build_connection_factory_setter(
    property_key: impl Into<String>,
    property_name: impl Into<String>,
    default_value: ConnectionFactoryKind,
    setter: impl Fn(&mut MemcachedConfiguration, ConnectionFactoryKind) + Send + Sync + 'static,
) -> impl PropertySetter {
    TypedPropertySetter::<ConnectionFactoryName>::new(
        property_key,
        property_name,
        ConnectionFactoryName(default_value),
        move |cfg, name| setter(cfg, name.0),
    )
}
