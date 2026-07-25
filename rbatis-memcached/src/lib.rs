//! `rbatis-memcached` — Memcached 分布式 backend。
//!
//! Java 对照：`org.mybatis.caches.memcached.*`（位于
//! `/workspace-github/memcached-cache/src/main/java/org/mybatis/caches/memcached/`）。
//!
//! | Java 文件 | Rust 文件 |
//! |---|---|
//! | `MemcachedCache.java` | `memcached_cache.rs` |
//! | `MemcachedClientWrapper.java` | `client_wrapper.rs` |
//! | `MemcachedConfiguration.java` | `configuration.rs` |
//! | `MemcachedConfigurationBuilder.java` | `configuration_builder.rs` |
//! | `CompressorTranscoder.java` | `compressor_transcoder.rs` |
//! | `LoggingMemcachedCache.java` | `logging_memcached_cache.rs` |
//! | `DummyReadWriteLock.java` | `dummy_read_write_lock.rs` |
//! | `StringUtils.java` | `string_utils.rs` |
//! | `AbstractPropertySetter.java` | `abstract_property_setter.rs` |
//! | `BooleanPropertySetter.java` | `boolean_property_setter.rs` |
//! | `IntegerPropertySetter.java` | `integer_property_setter.rs` |
//! | `StringPropertySetter.java` | `string_property_setter.rs` |
//! | `TimeUnitSetter.java` | `time_unit_setter.rs` |
//! | `InetSocketAddressListPropertySetter.java` | `inet_socket_address_list_property_setter.rs` |
//! | `ConnectionFactorySetter.java` | `connection_factory_setter.rs` |
//!
//! ## Rust 侧增强
//!
//! - [`consistent_hash::ConsistentHashRing`]：无 Java 对应。

#![forbid(unsafe_code)]

mod abstract_property_setter;
mod boolean_property_setter;
mod client_wrapper;
mod compressor_transcoder;
mod configuration;
mod configuration_builder;
mod connection_factory_setter;
mod consistent_hash;
mod dummy_read_write_lock;
mod inet_socket_address_list_property_setter;
mod integer_property_setter;
mod logging_memcached_cache;
mod memcached_cache;
mod string_property_setter;
mod string_utils;
mod time_unit_setter;

pub use abstract_property_setter::{PropertySetter, TypedPropertySetter};
pub use boolean_property_setter::build_boolean_setter;
pub use client_wrapper::MemcachedClientWrapper;
pub use compressor_transcoder::CompressorTranscoder;
pub use configuration::{ConnectionFactoryKind, MemcachedConfiguration};
pub use configuration_builder::MemcachedConfigurationBuilder;
pub use consistent_hash::ConsistentHashRing;
pub use dummy_read_write_lock::DummyReadWriteLock;
pub use inet_socket_address_list_property_setter::build_address_list_setter;
pub use integer_property_setter::build_integer_setter;
pub use logging_memcached_cache::LoggingMemcachedCache;
pub use memcached_cache::{MemcachedCacheBackend, MemcachedMetricsSnapshot};
pub use string_property_setter::build_string_setter;
pub use string_utils::StringUtils;
pub use time_unit_setter::build_time_unit_setter;
