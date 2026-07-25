//! 进程内 [`LocalBackend`] 离线契约测试：复用 `rbatis_cache::testing`
//! 中的 4 条标准断言。
//!
//! 对应 Java：`mybatis-caffeine` 的本地集成测试。

use rbatis_cache::testing::{
    assert_generation_atomic, assert_get_put_roundtrip, assert_missing_key_is_none,
    assert_ttl_expires,
};
use rbatis_cache::LocalBackend;

fn backend() -> LocalBackend {
    LocalBackend::new()
}

#[test]
fn missing_key_is_none() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build runtime");
    runtime.block_on(async move {
        let backend = backend();
        assert_missing_key_is_none(&backend).await;
    });
}

#[test]
fn put_then_get_roundtrips() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build runtime");
    runtime.block_on(async move {
        let backend = backend();
        assert_get_put_roundtrip(&backend).await;
    });
}

#[test]
fn generation_bumps_are_atomic() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build runtime");
    runtime.block_on(async move {
        let backend = backend();
        assert_generation_atomic(&backend).await;
    });
}

#[test]
fn short_ttl_expires() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build runtime");
    runtime.block_on(async move {
        let backend = backend();
        assert_ttl_expires(&backend).await;
    });
}
