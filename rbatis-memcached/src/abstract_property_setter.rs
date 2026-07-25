//! 属性 setter 抽象基类。
//!
//! 对应 Java：`org.mybatis.caches.memcached.AbstractPropertySetter`
//! （位于 `/workspace-github/memcached-cache/src/main/java/org/mybatis/caches/memcached/AbstractPropertySetter.java`）。
//!
//! Java 侧通过 JavaBean introspection 把 `properties` 中的字符串值转换为
//! `MemcachedConfiguration` 字段的强类型值；本 crate 用 trait + 注册表
//! 实现等价语义。

#![allow(missing_docs)]

use crate::configuration::MemcachedConfiguration;

/// setter 注册表 trait：把字符串值写入 [`MemcachedConfiguration`]。
///
/// 对应 Java 抽象基类 `AbstractPropertySetter<T>`，本 crate 简化为 trait
/// 方法直写（不维护泛型参数 `T`），由具体实现负责类型转换。
pub trait PropertySetter: Send + Sync {
    /// properties 中的 key（与 Java `propertyKey` 同义）。
    fn property_key(&self) -> &str;

    /// [`MemcachedConfiguration`] 中的字段名（Java `propertyName`）。
    fn property_name(&self) -> &str;

    /// 默认值（Java `defaultValue`）。
    fn default_value(&self) -> String;

    /// 把字符串写入 configuration 中对应字段。
    fn apply(&self, configuration: &mut MemcachedConfiguration, value: &str);
}

/// 简化的类型化 setter：把字符串解析为 `T` 后写入 configuration。
///
/// 对应 Java `AbstractPropertySetter<T>` 的"类型化子类型"。
pub struct TypedPropertySetter<T> {
    /// properties 文件中的 key。
    pub property_key: String,
    /// 字段名。
    pub property_name: String,
    /// 默认值。
    pub default_value: String,
    /// 转换函数（从 `T` 到字段写入）。
    setter: Box<dyn Fn(&mut MemcachedConfiguration, T) + Send + Sync>,
}

impl<T> TypedPropertySetter<T>
where
    T: std::str::FromStr + ToString + Send + Sync + 'static,
{
    /// 用 setter 闭包构造。
    pub fn new<F>(
        property_key: impl Into<String>,
        property_name: impl Into<String>,
        default_value: T,
        setter: F,
    ) -> Self
    where
        F: Fn(&mut MemcachedConfiguration, T) + Send + Sync + 'static,
    {
        Self {
            property_key: property_key.into(),
            property_name: property_name.into(),
            default_value: default_value.to_string(),
            setter: Box::new(setter),
        }
    }
}

impl<T> PropertySetter for TypedPropertySetter<T>
where
    T: std::str::FromStr + ToString + Send + Sync + 'static,
{
    fn property_key(&self) -> &str {
        &self.property_key
    }
    fn property_name(&self) -> &str {
        &self.property_name
    }
    fn default_value(&self) -> String {
        self.default_value.clone()
    }
    fn apply(&self, configuration: &mut MemcachedConfiguration, value: &str) {
        if let Ok(parsed) = value.parse::<T>() {
            (self.setter)(configuration, parsed);
        }
    }
}
