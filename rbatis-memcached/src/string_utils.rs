//! 字符串工具。
//!
//! 对应 Java：`org.mybatis.caches.memcached.StringUtils`
//! （位于 `/workspace-github/memcached-cache/src/main/java/org/mybatis/caches/memcached/StringUtils.java`）。

#![allow(missing_docs)]

/// 字符串工具：剥离首尾 ASCII 空白。
///
/// 对应 `StringUtils#trimToNull(String)` / `StringUtils#isEmpty(String)`。
pub struct StringUtils;

impl StringUtils {
    /// 是否为空白。
    pub fn is_empty(text: &str) -> bool {
        text.trim().is_empty()
    }

    /// 去除首尾空白后返回；空字符串返回 `None`。
    pub fn trim_to_none(text: &str) -> Option<String> {
        let trimmed = text.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_owned())
        }
    }
}
