//! InetSocketAddress 列表 setter。
//!
//! 对应 Java：`org.mybatis.caches.memcached.InetSocketAddressListPropertySetter`
//! （位于 `/workspace-github/memcached-cache/src/main/java/org/mybatis/caches/memcached/InetSocketAddressListPropertySetter.java`）。

#![allow(missing_docs)]

use std::fmt;

use crate::abstract_property_setter::{PropertySetter, TypedPropertySetter};
use crate::configuration::MemcachedConfiguration;

/// 字符串表示形如 `"host:port,host:port"`。
#[derive(Debug, Clone)]
pub struct AddressList(pub Vec<(String, u16)>);

impl fmt::Display for AddressList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, (host, port)) in self.0.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, "{host}:{port}")?;
        }
        Ok(())
    }
}

impl std::str::FromStr for AddressList {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let list: Vec<(String, u16)> = input
            .split(',')
            .filter_map(|token| {
                let token = token.trim();
                let (host, port) = token.split_once(':')?;
                let port = port.parse::<u16>().ok()?;
                Some((host.to_owned(), port))
            })
            .collect();
        Ok(Self(list))
    }
}

/// 解析空格分隔的 `"host:port host:port ..."` 字符串。
pub fn parse_address_list(input: &str) -> Vec<(String, u16)> {
    input
        .split_whitespace()
        .filter_map(|token| {
            let (host, port) = token.split_once(':')?;
            let port = port.parse::<u16>().ok()?;
            Some((host.to_owned(), port))
        })
        .collect()
}

/// InetSocketAddress-list setter。
///
/// 对应 Java: `InetSocketAddressListPropertySetter#convert(String)`。
pub fn build_address_list_setter(
    property_key: impl Into<String>,
    property_name: impl Into<String>,
    default_value: Vec<(String, u16)>,
    setter: impl Fn(&mut MemcachedConfiguration, Vec<(String, u16)>) + Send + Sync + 'static,
) -> impl PropertySetter {
    TypedPropertySetter::<AddressList>::new(
        property_key,
        property_name,
        AddressList(default_value),
        move |cfg, list| setter(cfg, list.0),
    )
}
