# rbatis-cache

`rbatis-cache` 是 RBatis 二级缓存生态的工作区（cargo workspace），对标
Java 包 `org.mybatis.caches.*` 的多个 MyBatis 适配器。本仓库以 monorepo
形式统一管理 SPI + 所有 backend，仿 `rbdc`（root package + 扁平 members）
与 `rbatis-plus`（`workspace.dependencies`）的布局。

## 仓库结构

```
rbatis-cache/                ← workspace 根（root package = rbatis-cache）
├── Cargo.toml               ← [workspace] + workspace.package/dependencies/lints
├── src/                     ← 二级缓存 SPI（必选）
│   ├── lib.rs               仅 mod/re-export，crate 级文档
│   ├── backend.rs           ← CacheBackend trait（4 个方法）+ CachePolicy + InvalidationStrategy
│   ├── error.rs             ← CacheError / CacheResult
│   ├── envelope.rs          ← CacheEnvelope（MessagePack 编解码 + is_fresh）
│   ├── interceptor.rs       ← CacheInterceptor：解析 → 旁路 → cache → singleflight → loader
│   ├── key.rs               ← CacheKey：BLAKE3 + 长度前缀化
│   ├── local_backend.rs     ← LocalBackend（dashmap 字节级 backend，离线可用）
│   ├── metrics.rs           ← CacheMetrics + Snapshot
│   ├── sql.rs               ← SqlMetadata（sqlparser 解析 + 表标签）
│   └── testing.rs           ← 契约测试 harness（feature = "testing"）
├── tests/                   ← core 集成测试
├── rbatis-redis/            ← Redis backend（对标 mybatis-redis）
├── rbatis-memcached/        ← Memcached backend（对标 mybatis-memcached）
└── .github/workflows/ci.yml ← fmt/clippy/test/doc 门禁
```

## Crate 矩阵

| Crate | 角色 | Java 对照 |
|---|---|---|
| `rbatis-cache` | SPI + LocalBackend + 拦截器 + 契约 harness | （无直接对应——RBatis 自身） |
| `rbatis-redis` | Redis 分布式 backend | `org.mybatis.caches.redis.*`（6 个文件） |
| `rbatis-memcached` | Memcached 分布式 backend | `org.mybatis.caches.memcached.*`（15 个文件） |

> 进程内 backend 由 `rbatis-cache::LocalBackend` 提供（对应
> Caffeine 适配器的等价实现；命名上不再单独抽出 `rbatis-moka` member），
> 后续若需要 W-TinyLFU 等 Caffeine 高级特性，可在此基础上叠加。

## 已落实不变量

- 仅缓存 sqlparser 解析后的 `SELECT` 单语句（非事务）；
- BLAKE3 key 隔离 `version + data_source + driver + tenant + namespace + statement_id + generation + canonical_sql + parameters`；
- MessagePack envelope + parser 抽取的 `table_tags`；
- 通过 generation bump 实现 namespace 级无扫描失效；
- 每个 key 上的 singleflight 防雪崩；
- backend 错误全部 fail-open 到 loader 并可观测（`CacheMetricsSnapshot`）；
- 缓存字节代表数据库 / 加密状态，由调用方在 envelope 外层做校验/解密。

## 与上游 rbatis 的关系

本 workspace 是 rbatis 生态的扩展。`Cargo.toml` 中 root package 显式声明
依赖 `rbatis`：

- 本地开发：`path = "../rbatis"`（指向同仓的 rbatis 仓库）；
- crates.io 发布：注释中的 `git = "https://github.com/rbatis-plus/rbatis.git"`（待 PR 合入 master 后启用）。

## 快速上手

```rust
use rbatis_cache::{CacheInterceptor, CachePolicy, CacheRequest, CacheKeyInput, LocalBackend};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() {
    // 1. 构造 backend（默认用 LocalBackend，亦可换成 rbatis-redis/memcached）
    let backend = Arc::new(LocalBackend::new());

    // 2. 构造拦截器
    let interceptor = CacheInterceptor::new(backend, CachePolicy::default());

    // 3. 发起一次带缓存的查询
    let request = CacheRequest {
        key: CacheKeyInput {
            version: "v1",
            data_source: "primary",
            driver: "sqlite",
            tenant: None,
            namespace: "order_mapper",
            statement_id: "find_by_id",
            sql: "select * from orders where id = ?",
            parameters: b"\x91\x2a", // MessagePack 编码的参数
        },
        in_transaction: false,
    };

    let payload = interceptor.get_or_load(request, || async {
        // 实际加载：数据库 / 加密层 / RPC
        Ok::<Vec<u8>, rbatis_cache::CacheError>(b"database result".to_vec())
    }).await.unwrap();

    println!("payload = {} bytes", payload.len());
}
```

## 契约测试

`rbatis-cache` 自身提供的契约 harness 在 `rbatis_cache::testing` 模块下，
需在 dev-dependencies 中启用 `testing` feature：

```rust
use rbatis_cache::testing::{assert_missing_key_is_none, run_all};

#[tokio::test]
async fn contract() {
    let backend = LocalBackend::new();
    run_all(&backend).await;
}
```

每个 backend 仓库的 `tests/` 目录复用同一套断言，离线或容器化环境运行。

## 这是 alpha 契约

RBatis 执行器集成（通过 `rbatis::intercept::Intercept` trait 的 `Intercept`
绑定）与 backend 在各自 crate / 仓库中开发。