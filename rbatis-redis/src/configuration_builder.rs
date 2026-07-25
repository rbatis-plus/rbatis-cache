//! 配置加载器：从 `redis.properties` 解析出 [`RedisConfig`]。
//!
//! 对应 Java：`org.mybatis.caches.redis.RedisConfigurationBuilder`
//! （位于 `/workspace-github/redis-cache/src/main/java/org/mybatis/caches/redis/RedisConfigurationBuilder.java`）。
//!
//! Java 侧使用单例 + `Properties` 加载 `redis.properties`（或由
//! `-Dredis.properties=path` 指定的文件）。本 crate 用 `std::env::var`
//! 读取 `REDIS_PROPERTIES` 环境变量，缺省回退 [`RedisConfig::standalone`]。

#![allow(missing_docs)]

use std::time::Duration;

use crate::redis_config::RedisConfig;

/// 配置加载器。
///
/// 对应 `RedisConfigurationBuilder`（单例）的等价物——本 crate 无需单例，
/// 直接调用 [`RedisConfigurationBuilder::from_env`]。
pub struct RedisConfigurationBuilder;

impl RedisConfigurationBuilder {
    /// 读取环境变量 `REDIS_PROPERTIES` 指向的 properties 文件并解析。
    ///
    /// 格式与 Java 侧一致：`key=value` 行，`#` 起首行为注释。
    pub fn from_env() -> RedisConfig {
        let path = std::env::var("REDIS_PROPERTIES").ok();
        match path {
            Some(path) => Self::from_file(&path),
            None => RedisConfig::standalone(),
        }
    }

    /// 显式指定文件路径加载。
    pub fn from_file(path: &str) -> RedisConfig {
        let content = match std::fs::read_to_string(path) {
            Ok(c) => c,
            Err(_) => return RedisConfig::standalone(),
        };
        Self::parse(&content)
    }

    /// 解析 properties 文本。
    pub fn parse(text: &str) -> RedisConfig {
        let mut config = RedisConfig::standalone();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((key, value)) = line.split_once('=') {
                apply_kv(&mut config, key.trim(), value.trim());
            }
        }
        config
    }
}

/// 单条 `key=value` 写入 [`RedisConfig`] 的字段。
///
/// 字段映射与 Java 侧 `RedisConfigurationBuilder#parse*` 等价。
fn apply_kv(config: &mut RedisConfig, key: &str, value: &str) {
    match key {
        "redis.host" => config.url = format!("redis://{value}"),
        "redis.port" => {
            // 把 url 里的端口替换。
            if let Some(at) = config.url.rfind(':') {
                let scheme_end = config.url.find("://").map_or(0, |i| i + 3);
                let db_end = config.url[scheme_end..].find('/');
                let end = db_end.map_or(config.url.len(), |o| scheme_end + o);
                config
                    .url
                    .replace_range(scheme_end..end, &format!("127.0.0.1:{value}"));
                let _ = at;
            }
        }
        "redis.password" => config.password = Some(value.to_owned()),
        "redis.database" => {
            if let Ok(n) = value.parse() {
                config.database = n;
            }
        }
        "redis.connectionTimeout" | "redis.soTimeout" => {
            if let Ok(ms) = value.parse::<u64>() {
                let _ =
                    std::mem::replace(&mut config.connection_timeout, Duration::from_millis(ms));
            }
        }
        "redis.clientName" => config.client_name = Some(value.to_owned()),
        "redis.ssl" => config.ssl = value.eq_ignore_ascii_case("true"),
        "redis.serializer" => config.serializer = value.to_owned(),
        _ => {}
    }
}
