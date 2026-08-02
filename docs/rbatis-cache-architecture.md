# rbatis-cache 架构与代码导读

> 本文档基于本地 **codegraph** 索引（`/Users/wandl/workspaces/workspace-github-easy-4-rust/rbatis-cache`，36 个 Rust 文件、412 个符号节点、952 条边）的一手源码梳理。
>
> 仓库是 RBatis 二级缓存生态的 Cargo workspace，对标 Java MyBatis 的 `org.mybatis.caches.*` 适配器族。
>
> 当前版本：`rbatis-cache 0.1.0-alpha.2`（workspace root package）
> 参考资料：
> - `cratename = rbatis-cache`：本地仓库根（本路径）
> - 与 `rbatis` 关系：上游 ORM 在 `../rbatis`（也即 `workspace-github-easy-4-rust/rbatis`）；本仓库正为其做下一个版本的 L2 缓存生态，Caffeine 化改造已先在 `rbatis` 主仓库 `df87ac41` 落地

---

## 目录

1. 一句话定位与对前版 `rbatis` 内置缓存的差异化
2. 仓库布局与规模
3. 工作区核心 crate：`rbatis-cache`（SPI）
   - 3.1 模块表
   - 3.2 `CacheBackend` trait
   - 3.3 `CachePolicy` + `InvalidationStrategy`
   - 3.4 `CacheKey` / `CacheKeyInput`（BLAKE3 + 长度前缀化）
   - 3.5 `CacheEnvelope`（MessagePack 线缆格式）
   - 3.6 `CacheInterceptor`（解析 → 旁路 → cache → singleflight → loader）
   - 3.7 `CacheMetrics` + `CacheMetricsSnapshot`
   - 3.8 `SqlMetadata` + `StatementKind`
   - 3.9 `CacheError` 四变体
4. `rbatis-redis`（分布式 backend）
5. `rbatis-memcached`（分布式 backend）
6. 契约测试 harness 与集成测试
7. 与 `rbatis` 已合入缓存（`df87ac41`）的关系
8. ASCII 流程图：`get_or_load` 单次拦截
9. 关键设计权衡（FAQ）
10. codegraph 速查命令
11. 推荐阅读顺序
12. 已知 TODO/未达成的能力

---

## 1. 一句话定位与差异化

**rbatis-cache = RBatis 的二级缓存生态 SPI，以 monorepo 形式同时管理"SPI + 进程内 backend + 2 个分布式 backend"。**

> Java 对照：`org.mybatis.caches.caffeine.*` + `org.mybatis.caches.redis.*` + `org.mybatis.caches.memcached.*` 三个独立 Maven 仓库、本仓库只用一个 Cargo workspace。

### 1.1 与上游 `rbatis` 内置缓存的差异化

本仓库 `rbatis-cache` 跟 `workspace-github-easy-4-rust/rbatis/src/plugin/cache/` 是 **并行** 的两条线：

| 维度 | `rbatis/src/plugin/cache/`（已合入 `df87ac41`） | `rbatis-cache`（workspace root） |
|---|---|---|
| 定位 | rbatis 主仓库的内置 L2 | 可选扩展包（monorepo SPI + 多 backend） |
| 缓存 value 类型 | `Arc<Value>`（rbs 动态类型，已在内存中） | `Vec<u8>` 字节流（更通用，跨进程/跨语言） |
| 序列化 | 无（值已经是内存对象） | **MessagePack** envelope + `version/generation/expires_at_ms/table_tags` |
| Key | `CacheKey { namespace, sql, args, version, digest: u128 }`，**xxh3-128** | `CacheKey` 显式枚举 8 维隔离边界，**BLAKE3** 长度前缀化 |
| Backend | `MemoryCacheStore` 单 moka 进程内 backend | SPI 4 个方法，加 `LocalBackend` + `RedisCacheBackend` + `MemcachedCacheBackend` 3 个实现 |
| Generation | namespace epoch → 进入 store key | namespace `bump_generation()` 原子递增，由 envelope 携带的 generation 与当前比较判定新鲜度 |
| Transaction | `TransactionCacheMode::{Bypass, Defer}` + `TransactionalCacheBuffer` | **保守**：`in_transaction=true` 一律旁路（`interceptor.rs:84`），等执行器侧把命中/失效串通 |
| Metric | `MemoryCacheStore::hits/misses AtomicU64` | 拦截器内统一：`CacheMetricsSnapshot { hits/misses/bypasses/backend_errors/loads/invalidations }` |
| SPI 协议版本 | 单一实现，不暴露 | `version: u16` 写进 envelope，未来可切换编解码 |

> **TL;DR**：`rbatis/src/plugin/cache/` 适合在单一 Rust 进程内用；`rbatis-cache` 是给"多进程/多语言 backend（Redis / Memcached）"准备的字节级契约。

### 1.2 一句话对标 Java

| Java 适配器 | Rust 实现 |
|---|---|
| `org.mybatis.caches.caffeine.CaffeineCache`（38 行薄壳） | `rbatis-cache::LocalBackend`（dashmap + 字节级，比 Java 版本对位更严格，支持 generation） |
| `org.mybatis.caches.redis.*`（6 个 Java 文件） | `rbatis-redis/*`（6 个 Rust 文件，文件一一对应） |
| `org.mybatis.caches.memcached.*`（15 个 Java 文件） | `rbatis-memcached/*`（15 个 Rust 文件） |

---

## 2. 仓库布局与规模

```
rbatis-cache/                                    Cargo workspace (root = rbatis-cache)
├── Cargo.toml                                   [workspace] + workspace.package/dependencies/lints
├── src/                                         SPI + LocalBackend + 拦截器 + 契约 harness       ~ 887 行
│   ├── lib.rs                          69 行    re-export + crate 级 doc
│   ├── backend.rs                      ~ 70 行   CacheBackend trait + CachePolicy + InvalidationStrategy
│   ├── envelope.rs                     ~ 85 行   CacheEnvelope（MessagePack + is_fresh）
│   ├── error.rs                        ~ 45 行   CacheError 四变体
│   ├── interceptor.rs                  ~ 199 行  CacheInterceptor（解析 → 旁路 → singleflight → loader → 写）
│   ├── key.rs                          ~ 118 行  CacheKey + CacheKeyInput（BLAKE3 + 长度前缀化）
│   ├── local_backend.rs                ~ 201 行  LocalBackend（dashmap + byte-level）
│   ├── metrics.rs                      ~ 92 行   CacheMetrics + CacheMetricsSnapshot
│   ├── sql.rs                          ~ 72 行   SqlMetadata + StatementKind（sqlparser 0.62）
│   └── testing.rs                      ~ 152 行  契约 harness（feature = "testing"）
├── tests/                                     core SPI 集成测试（cache_contract.rs / local_backend_contract.rs）
├── rbatis-redis/                                 Redis 分布式 backend                                  ~ 740 行
│   ├── src/lib.rs                       31 行
│   ├── src/redis_cache.rs               232 行   RedisCacheBackend（含 timeout + circuit breaker）
│   ├── src/redis_config.rs              120 行   RedisConfig + RedisCacheConfig
│   ├── src/configuration_builder.rs      92 行   RedisConfigurationBuilder
│   ├── src/serializer.rs                163 行   JdkSerializer / KryoSerializer（Type alias 兼容入口但内部固定 MessagePack）
│   ├── src/redis_callback.rs             15 行
│   └── src/dummy_read_write_lock.rs      48 行
└── rbatis-memcached/                             Memcached 分布式 backend                          ~ 1 285 行
    ├── src/lib.rs                       61 行
    ├── src/memcached_cache.rs           184 行   MemcachedCacheBackend（spawn_blocking 模式）
    ├── src/client_wrapper.rs             93 行   MemcachedClientWrapper（提供 u64 generation 辅助方法）
    ├── src/configuration.rs              78 行   MemcachedConfiguration + ConnectionFactoryKind
    ├── src/configuration_builder.rs      99 行   MemcachedConfigurationBuilder（链式 setter）
    ├── src/compressor_transcoder.rs      34 行
    ├── src/logging_memcached_cache.rs   125 行
    ├── src/consistent_hash.rs            61 行   ConsistentHashRing（BLAKE3 hash）
    ├── src/dummy_read_write_lock.rs      41 行
    ├── src/string_utils.rs               26 行
    ├── src/abstract_property_setter.rs  85 行
    ├── src/boolean_property_setter.rs    19 行
    ├── src/integer_property_setter.rs    19 行
    ├── src/string_property_setter.rs     19 行
    ├── src/time_unit_setter.rs           33 行
    ├── src/inet_socket_address_list_property_setter.rs 71 行
    └── src/connection_factory_setter.rs  56 行
```

codegraph 总览：

```
Files:     37  (rust 36, yaml 1)
Nodes:     412
Edges:     952
DB Size:   1.27 MB
```

---

## 3. 工作区核心 crate：`rbatis-cache`

### 3.1 模块表

| 模块 | 行数 | 角色 |
|---|---:|---|
| `backend.rs` | ~ 70 | `CacheBackend` trait + `CachePolicy` + `InvalidationStrategy` |
| `envelope.rs` | ~ 85 | `CacheEnvelope`（MessagePack 编解码 + `is_fresh` 检查） |
| `error.rs` | ~ 45 | `CacheError` 四变体（`Sql` / `Codec` / `Backend` / `Loader`） |
| `interceptor.rs` | ~ 199 | `CacheInterceptor::get_or_load`（核心入口；singleflight 用 `Arc<Mutex<()>>` + `Arc::strong_count==2` 启发式清理） |
| `key.rs` | ~ 118 | `CacheKey` + `CacheKeyInput`，BLAKE3 + 长度前缀化的 8 维隔离边界 |
| `local_backend.rs` | ~ 201 | 进程内 backend（`dashmap::DashMap<digest, Entry>` + `DashMap<namespace, Arc<AtomicU64>>`） |
| `metrics.rs` | ~ 92 | 6 个 `AtomicU64` 计数 + `CacheMetricsSnapshot` |
| `sql.rs` | ~ 72 | `SqlMetadata`（sqlparser 0.62；`GenericDialect`；`visit_relations` 抽表名） |
| `testing.rs` | ~ 152 | 4 个契约断言函数（missing/roundtrip/generation/ttl）+ `run_all` |

### 3.2 `CacheBackend` trait (`src/backend.rs:62-77`)

```rust
pub trait CacheBackend: Send + Sync + 'static {
    fn get<'a>(&'a self, key: &'a str)
        -> BoxFuture<'a, Result<Option<Vec<u8>>>>;
    fn put<'a>(&'a self, key: &'a str, value: Vec<u8>, ttl: Duration)
        -> BoxFuture<'a, Result<()>>;
    fn generation<'a>(&'a self, namespace: &'a str)
        -> BoxFuture<'a, Result<u64>>;            // 缺失视为 0
    fn bump_generation<'a>(&'a self, namespace: &'a str)
        -> BoxFuture<'a, Result<u64>>;            // 原子 + 返回新值
}
```

设计契约（注释清楚写在文件头）：
1. `get` 返回的字节可被 `CacheEnvelope::decode` 还原；
2. `bump_generation` 是原子的，且必须返回 `bump` **之后**的新值；
3. 任何方法失败 → 转 `CacheError::Backend`，**绝不**向上抛底层 client 异常；
4. BoxFuture：让 backend 可作 `Arc<dyn CacheBackend>`，能成为 `CacheInterceptor` 的字段。

> 对位：`org.apache.ibatis.cache.Cache` 的 4 个方法（`getId/putObject/getObject/removeObject/clear`）。本 crate 把 `clear` 替换成"按 namespace bump generation"——更精细、可观测、单 backend 内原子。

### 3.3 `CachePolicy` + `InvalidationStrategy` (`src/backend.rs:33-52`)

```rust
#[derive(Debug, Clone)]
pub struct CachePolicy {
    pub ttl: Duration,
    pub max_value_size: usize,
    pub invalidation: InvalidationStrategy,
}
impl Default for CachePolicy {
    fn default() -> Self {
        Self {
            ttl: Duration::from_mins(5),
            max_value_size: 1024 * 1024,
            invalidation: InvalidationStrategy::NamespaceGeneration,
        }
    }
}

pub enum InvalidationStrategy {
    NamespaceGeneration,    // 默认：commit 后 bump 整个 namespace
    TableGeneration,        // 预留：parser 抽取的关系级 generation
}
```

### 3.4 `CacheKey` / `CacheKeyInput` (`src/key.rs`)

```rust
#[derive(Debug, Clone, Copy)]
pub struct CacheKeyInput<'a> {
    pub version: &'a str,          // 协议版本
    pub data_source: &'a str,      // 多数据源隔离
    pub driver: &'a str,           // "mysql" / "sqlite"
    pub tenant: Option<&'a str>,   // 多租户
    pub namespace: &'a str,        // Mapper / 应用层
    pub statement_id: &'a str,     // 语句 ID
    pub sql: &'a str,
    pub parameters: &'a [u8],      // 调用方编码（推荐 MessagePack）
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheKey {
    digest: String,                // BLAKE3 hex
    namespace: String,
    generation: u64,               // 写 envelope 时也带此 generation
    table_tags: BTreeSet<String>,  // sqlparser 抽取的关系名
}
```

`CacheKey::build` 步骤（`key.rs:60-85`）：

1. `SqlMetadata::parse(sql)` → `canonical_sql` + `table_tags`（小写去重排序）+ `kind`
2. BLAKE3 hasher 按顺序 `update_component`：`version`, `data_source`, `driver`, `tenant||"-"`, `namespace`, `statement_id`, `canonical_sql`, `generation.to_le_bytes()`, `parameters`
3. `update_component` 每个分量先写 8 字节 `len`（`u64`）再写数据，保证 `("ab","c")` vs `("a","bc")` 永不撞车
4. digest = `blake3.finalize().to_hex().to_string()`

> **算法选择 BLAKE3 而不是 xxh3**：32 字节（256-bit）输出抗碰撞更稳，且 `to_hex()` 后端安全字符串可直接做 Redis/Memcached key。

### 3.5 `CacheEnvelope` (`src/envelope.rs`)

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheEnvelope {
    pub version: u16,
    pub generation: u64,
    pub expires_at_ms: u64,            // Unix epoch ms
    pub table_tags: BTreeSet<String>,
    pub payload: Vec<u8>,
}

impl CacheEnvelope {
    pub fn new(key: &CacheKey, payload: Vec<u8>, ttl: Duration) -> Self
    pub fn encode(&self) -> Result<Vec<u8>>                // rmp_serde::to_vec_named
    pub fn decode(bytes: &[u8]) -> Result<Self>            // rmp_serde::from_slice
    pub fn is_fresh(&self, generation: u64) -> bool {
        self.version == 1 && self.generation == generation && self.expires_at_ms > now_ms()
    }
}
```

- `is_fresh` 把 TTL 与 generation 两层新鲜度判定集中到一处：
  - `version != 1` → 不新鲜（保留协议升级通道）
  - `envelope.generation != 当前 generation` → 不新鲜（namespace 被 bump 过）
  - `expires_at_ms` 已经过期 → 不新鲜
- `now_ms()` 用 `SystemTime::now()`，时间回退时返回 `u64::MAX` 让旧条目**立即**失效

### 3.6 `CacheInterceptor` (`src/interceptor.rs:39-194`)

```rust
pub struct CacheInterceptor<B> {
    backend: Arc<B>,
    policy: CachePolicy,
    metrics: Arc<CacheMetrics>,
    flights: DashMap<String, Arc<Mutex<()>>>,   // digest -> 共享锁
}

impl<B: CacheBackend> CacheInterceptor<B> {
    pub fn new(backend: Arc<B>, policy: CachePolicy) -> Self;
    pub fn metrics(&self) -> Arc<CacheMetrics>;
    pub async fn get_or_load<F, Fut>(&self, request: CacheRequest<'_>, loader: F) -> Result<Vec<u8>>;
    pub async fn invalidate_after_commit(&self, namespace: &str) -> Result<u64>;
}
```

**`get_or_load` 全流程**（`interceptor.rs:74-138`）：

```
1. 解析 SQL (SqlMetadata::parse) ── 失败 ── bypass(loader)
2. in_transaction || kind != Select ── bypass
3. backend.generation(ns) ── 失败 ── record_backend_error + load(loader)
4. CacheKey::build(input, generation) ── 失败 ── load
5. cached(&key) → Some ── record_hit + 返回
6. record_miss
7. flights.entry(digest).or_insert_with(lock).clone()
8. flight.lock().await
9. cached(&key) 二次确认 ── Some → record_hit + drop guard + release_flight + 返回
10. payload = loader().await?                       // 真正执行 DB load
11. if payload.len() <= max_value_size {
        envelope = CacheEnvelope::new(...).encode()?
        backend.put(digest, envelope, ttl).await    // 失败 → record_backend_error（fail-open）
    }
12. drop guard; release_flight; Ok(payload)
```

**Singleflight 实现**（与 `rbatis` 主仓库的 `Notify` 实现对比）：

| 维度 | `rbatis/src/plugin/cache/singleflight.rs` | `rbatis-cache/src/interceptor.rs` |
|---|---|---|
| 同步原语 | `tokio::sync::Notify` + `Arc<LoadState>` | `tokio::sync::Mutex<()>` + `Arc` |
| 唤醒 | `Notify::notify_waiters()` | leader drop guard 时 follower 已释放 |
| Entry 清理 | `complete_load` 主动 remove | `Arc::strong_count == 2` 启发式：`flights` 表持 1 + 调用者持 1 时无并发 follower 再删 |
| 泄漏风险 | 无（显式 remove） | 极小（强引用计=2）但 follower 失败时可能留条目 |

**`release_flight`**（`interceptor.rs:181-194`）启发式细节：

```rust
fn release_flight(&self, key: &str, flight: &Arc<Mutex<()>>) {
    let occupied = match self.flights.entry(key.to_owned()) {
        Entry::Occupied(entry) => entry,
        Entry::Vacant(_) => return,
    };
    if Arc::ptr_eq(occupied.get(), flight) && Arc::strong_count(occupied.get()) == 2 {
        occupied.remove();
    }
}
```

`Arc::strong_count==2` 是"follower 已经释放了它的引用"信号。

**`invalidate_after_commit`**（`interceptor.rs:141-145`）：

```rust
pub async fn invalidate_after_commit(&self, namespace: &str) -> Result<u64> {
    let generation = self.backend.bump_generation(namespace).await?;
    self.metrics.record_invalidation();
    Ok(generation)
}
```

由上游"事务完成事件"调用（与 `rbatis` 主仓库的 `CacheTransactionListener` 对应——本仓库需要在执行器集成时挂上）。

### 3.7 `CacheMetrics` (`src/metrics.rs`)

```rust
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheMetricsSnapshot {
    pub hits: u64, pub misses: u64, pub bypasses: u64,
    pub backend_errors: u64, pub loads: u64, pub invalidations: u64,
}
#[derive(Debug, Default)]
pub struct CacheMetrics {
    hits: AtomicU64, misses: AtomicU64, bypasses: AtomicU64,
    backend_errors: AtomicU64, loads: AtomicU64, invalidations: AtomicU64,
}
```

- 6 个 `AtomicU64`；`record_*` 用 `pub(crate)`，只允许拦截器调用
- `snapshot()` 用 `Relaxed` 序——只做"观测"，不必严格一致

### 3.8 `SqlMetadata` + `StatementKind` (`src/sql.rs`)

```rust
pub enum StatementKind { Select, Other }
pub struct SqlMetadata {
    pub canonical_sql: String,
    pub table_tags: BTreeSet<String>,    // sqlparser visit_relations → 小写去重排序
    pub kind: StatementKind,
}
impl SqlMetadata {
    pub fn parse(sql: &str) -> Result<Self> {
        let statements = Parser::parse_sql(&GenericDialect{}, sql)?;
        let kind = if statements.len() == 1 && matches!(statements[0], Statement::Query(_)) {
            StatementKind::Select
        } else {
            StatementKind::Other
        };
        visit_relations(&statements, |r| { table_tags.insert(r.to_string().to_ascii_lowercase()); ... });
        ...
    }
}
```

兜底策略：
- 多语句 → `Other` → 旁路（**保守**——拒绝缓存可能误命中多个语句的结果）
- 解析失败 → `Err(CacheError::Sql)` → 拦截器进入 `bypass`
- 表名全部小写去重排序（`BTreeSet` 自然排序）

### 3.9 `CacheError` 四变体 (`src/error.rs`)

```rust
pub enum CacheError {
    Sql(String),       // SQL 解析失败 / 无法分类（保守 → 旁路）
    Codec(String),     // MessagePack 编解码失败（数据被篡改或版本不兼容）
    Backend(String),   // 后端 Redis / Memcached / Moka 操作失败
    Loader(String),    // 调用方提供的数据库 loader 闭包失败
}
impl std::error::Error for CacheError {}
```

设计要点：**所有 backend 必须把内部错误统一映射**，绝不向上泄漏底层 client 库的细节（Redis 错误码、Memcached 协议错误等）。

---

## 4. `rbatis-redis`（分布式 backend）

### 4.1 文件映射表

| Java 文件 | Rust 文件 |
|---|---|
| `RedisCache.java` | `src/redis_cache.rs` |
| `RedisConfig.java` | `src/redis_config.rs`（`RedisConfig` + `RedisCacheConfig`） |
| `RedisConfigurationBuilder.java` | `src/configuration_builder.rs` |
| `Serializer.java` + `JDKSerializer.java` + `KryoSerializer.java` | `src/serializer.rs`（3 个 type alias，但当前内部固定 MessagePack） |
| `RedisCallback.java` | `src/redis_callback.rs` |
| `DummyReadWriteLock.java` | `src/dummy_read_write_lock.rs` |

### 4.2 `RedisCacheBackend` (`src/redis_cache.rs:60-210`)

```rust
pub struct RedisCacheBackend {
    name: String,
    connection: ConnectionManager,                       // redis::aio::ConnectionManager（tokio）
    config: RedisCacheConfig,
    metrics: RedisMetrics,
    consecutive_failures: AtomicU32,                     // 熔断用
    circuit_open_until_ms: AtomicU64,
}

pub struct RedisMetricsSnapshot {
    pub operations: u64, pub errors: u64,
    pub timeouts: u64, pub circuit_opens: u64,
    pub invalidations: u64,
}

impl RedisCacheBackend {
    pub async fn connect(name: impl Into<String>, config: RedisCacheConfig) -> Result<Self>;
}
```

**Rust 侧增强（无 Java 对应）**：字节级 operation timeout + 熔断：

```rust
async fn run<F, T>(&self, op: F) -> Result<T> {
    if self.circuit_open_until_ms.load(Acquire) > now_ms() {
        return Err(CacheError::Backend("redis circuit is open".to_owned()));
    }
    match tokio::time::timeout(self.config.operation_timeout, op).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(e)) => { self.record_failure(); Err(CacheError::Backend(e.to_string())) }
        Err(_) => { self.record_failure(); Err(CacheError::Backend("redis operation timed out".to_string())) }
    }
}
fn record_failure(&self) {
    let f = self.consecutive_failures.fetch_add(1, AcqRel).saturating_add(1);
    if f >= self.config.circuit_failure_threshold.max(1) {
        self.circuit_open_until_ms.store(now_ms() + circuit_cooldown_ms, Release);
        self.metrics.circuit_opens.fetch_add(1, Relaxed);
        self.consecutive_failures.store(0, Release);
    }
}
```

4 个 SPI 方法：
- `get`：直接 GET `{prefix}:entry:{digest}`，`redis.get` 命令
- `put`：`PSETEX {prefix}:entry:{digest} {ttl_ms} {value}`（毫秒级精度 TTL）
- `generation`：`GET {prefix}:generation:{blake3(namespace)}`，缺省视为 `0`
- `bump_generation`：`INCR {prefix}:generation:{blake3(namespace)}`（天然原子 + 返回新值）

> 当前未启用 cluster/sentinel/Pub-Sub 失效广播——`RedisCacheConfig` 已留扩展点。

---

## 5. `rbatis-memcached`（分布式 backend）

### 5.1 文件映射表

| Java 文件 | Rust 文件 |
|---|---|
| `MemcachedCache.java` | `src/memcached_cache.rs` |
| `MemcachedClientWrapper.java` | `src/client_wrapper.rs` |
| `MemcachedConfiguration.java` | `src/configuration.rs` |
| `MemcachedConfigurationBuilder.java` | `src/configuration_builder.rs` |
| `CompressorTranscoder.java` | `src/compressor_transcoder.rs` |
| `LoggingMemcachedCache.java` | `src/logging_memcached_cache.rs` |
| `DummyReadWriteLock.java` | `src/dummy_read_write_lock.rs` |
| `StringUtils.java` | `src/string_utils.rs` |
| `AbstractPropertySetter.java` | `src/abstract_property_setter.rs` |
| `BooleanPropertySetter.java` | `src/boolean_property_setter.rs` |
| `IntegerPropertySetter.java` | `src/integer_property_setter.rs` |
| `StringPropertySetter.java` | `src/string_property_setter.rs` |
| `TimeUnitSetter.java` | `src/time_unit_setter.rs` |
| `InetSocketAddressListPropertySetter.java` | `src/inet_socket_address_list_property_setter.rs` |
| `ConnectionFactorySetter.java` | `src/connection_factory_setter.rs` |

### 5.2 `MemcachedCacheBackend` (`src/memcached_cache.rs:53-184`)

```rust
pub struct MemcachedCacheBackend {
    name: String,
    config: MemcachedConfiguration,
    wrapper: Arc<MemcachedClientWrapper>,     // 同步 memcache client，包在 spawn_blocking 里
    metrics: MemcachedMetrics,
}

pub struct MemcachedMetricsSnapshot {
    pub operations: u64, pub errors: u64,
    pub timeouts: u64, pub invalidations: u64,
}
```

**Rust 侧独有用法**：
- 因为 `memcache` crate 是**同步**客户端，所以每个 SPI 方法都用 `tokio::task::spawn_blocking` 把阻塞调用丢到线程池——`async` 链不阻塞
- 给 `MemcachedClientWrapper` 加 `client_get_u64` / `client_incr` 助手方法（`memcached_cache.rs:160-175`）实现"读 u64 generation"和"atomic INCR"。`client_incr` 先 `add(0)` 兜底（key 不存在时 `incr` 会失败）

### 5.3 `ConsistentHashRing` (`src/consistent_hash.rs`)

```rust
pub struct ConsistentHashRing { points: BTreeMap<u64, usize>, node_count: usize }
impl ConsistentHashRing {
    pub fn new(node_count: usize, virtual_nodes: u16) -> Self { ... }   // ketama 默认 160 虚拟点
    pub fn node_for(&self, key: &str) -> usize { ... }                  // 取首个 >= hash，环绕到首点
}
```

> Java 对照：spymemcached 内部 ketama-hash 自动路由，不需要应用层关心；本 crate 显式提供便于未来做"指定路由/探活/分桶"。

### 5.4 `MemcachedConfigurationBuilder` 链式 setter

15 个 Rust 文件中"PropertySetter" / "ConfigurationBuilder" 模式完全对位 Java 提供链式 fluent API：`with_servers(...).with_compression(...).with_key_prefix(...).build()`。

---

## 6. 契约测试 harness 与集成测试

### 6.1 harness (`src/testing.rs`)

| 断言 | 测试什么 |
|---|---|
| `assert_missing_key_is_none` | 未写入的 key 返回 `Ok(None)` |
| `assert_get_put_roundtrip` | put → get 字节相等；二次 put → get 覆盖最新值 |
| `assert_generation_atomic` | **并发 32 次 `bump_generation` 必须 generation += 32**（最严的契约！） |
| `assert_ttl_expires` | 50 ms TTL → 200 ms 后 `get` 返回 `None`（验证 **后端** TTL 真生效） |
| `run_all` | 一次跑完 4 条 |

`DynBackend = Arc<dyn CacheBackend>` 类型别名 + `dyn_backend(impl)` 工厂函数让 backend 测试方无需关心具体类型。

### 6.2 workspace 集成测试

`tests/` 目录：
- `cache_contract.rs` —— 直接对 `rbatis-cache::LocalBackend` 跑 `run_all`
- `local_backend_contract.rs` —— `LocalBackend` 自己的特定行为

**未来**：`rbatis-redis` / `rbatis-memcached` 在自己的 `dev-dependencies` 引入 `rbatis-cache = { path = "../rbatis-cache", features = ["testing"] }`，把同样的 `run_all` 套到自己 backend 上。

### 6.3 CI 门禁

`.github/workflows/ci.yml`：fmt / clippy / test / doc 四关。`workspace.lints.rust` 已经设置 `unsafe_code = "forbid"`、`missing_docs = "warn"`，clippy 大量 pedantic 已开（`all`, `pedantic`）。

---

## 7. 与 `rbatis` 已合入缓存（`df87ac41`）的关系

`rbatis/src/plugin/cache/` 和 `rbatis-cache/` 是 **并行** 项目：

| 维度 | 主仓库缓存（已合入） | rbatis-cache 缓存 |
|---|---|---|
| 状态 | 已经在 master 基准上工作 | alpha（0.1.0-alpha.2），workspace 文档明确"这是一份 alpha 契约" |
| 适配对象 | rbatis 进程内 | rbatis + 任意 backend |
| 工具函数 | `MemoryCacheStore`（moka）已在本仓库 | `LocalBackend`（dashmap）作为离线/单测样例 |
| 关键差异 | 见 §1.1 | 见 §1.1 |

`rbatis-cache` 的目标，是下一版本的 **多 backend（Redis/Memcached）+ 多进程 + 多语言** 场景；当前主仓库 `df87ac41` 仍是进程内最优化版本。两套同时存在，是为了让主仓库缓存先稳定，主仓合入了 Caffeine 化的关键修正（xxh3-128 + 真 SingleFlight + epoch），后续 `rbatis-cache` 也可借鉴。

---

## 8. ASCII 流程图：`get_or_load` 单次拦截

```
              ┌─────────────────────────────────────┐
              │ get_or_load(request, loader)        │
              └──────────────┬──────────────────────┘
                             │
            ┌────────────────▼─────────────────┐
            │ SqlMetadata::parse(sql)  失败?    │
            │                                yes│──▶ bypass(loader)  (record_bypass)
            │                                  no│
            └────────────────┬─────────────────┘
                             ▼
            ┌─────────────────────────────────────┐
            │ in_transaction || kind != Select?   │
            │ yes → bypass(loader)               │
            │ no  ↓                              │
            └──────────────┬──────────────────────┘
                           ▼
            ┌─────────────────────────────────────┐
            │ backend.generation(ns)              │
            │ Err → record_backend_error          │
            │       + load(loader)                │
            │ Ok  ↓                              │
            └──────────────┬──────────────────────┘
                           ▼
            ┌─────────────────────────────────────┐
            │ CacheKey::build(input, generation)  │
            │ Err → load                          │
            │ Ok  ↓                              │
            └──────────────┬──────────────────────┘
                           ▼
            ┌─────────────────────────────────────┐
            │ cached(&key) = backend.get + decode │
            │ + is_fresh(generation)              │
            │ Some → record_hit, return payload  │
            │ None ↓                              │
            └──────────────┬──────────────────────┘
                           ▼  record_miss
            ┌─────────────────────────────────────┐
            │ flights[ digest ] = Arc<Mutex<()>>  │
            │ flight.lock().await                  │
            └──────────────┬──────────────────────┘
                           ▼
            ┌─────────────────────────────────────┐
            │ 二次检查 cached(&key)?               │
            │ Some → record_hit, return           │
            │ None ↓                              │
            └──────────────┬──────────────────────┘
                           ▼
            ┌─────────────────────────────────────┐
            │ payload = load(loader).await?        │   ← record_load +1
            └──────────────┬──────────────────────┘
                           ▼
            ┌─────────────────────────────────────┐
            │ payload.len() <= max_value_size?     │
            │ yes → CacheEnvelope::encode          │
            │       backend.put(digest, envelope, ttl)│
            │       Err → record_backend_error     │
            │ no  → 仅返回 (不写缓存)              │
            └──────────────┬──────────────────────┘
                           ▼
            ┌─────────────────────────────────────┐
            │ drop guard; release_flight; Ok(payload) │
            └─────────────────────────────────────┘

   与之配套的失效路径 (事务完成后由执行器侧调用):
   ┌─────────────────────────────────────┐
   │ invalidate_after_commit(ns)        │   ← record_invalidation +1
   │ generation = backend.bump_generation(ns) │
   └─────────────────────────────────────┘
```

---

## 9. 关键设计权衡（FAQ）

### Q1：为什么用 `Vec<u8>` 而不是 `Arc<Value>` ？

字节级比"内存对象"更通用：可走 `rmp_serde` 跨进程 / 跨语言；Redis/Memcached 后端恰好只接字节流。代价是 envelope 编码 + decode 的性能开销（**评估中**——若主路径里这部分成为热点，未来可以把 envelope 在主仓库就地解码）。

### Q2：为什么 generation 走 "envelope 携带 + 当前比较" 而不是 "key 拼 generation"？

`CacheKey = (digest)` 严格不变 → backend 可缓存同一 digest 的多代 envelope，靠 envelope 自己的 `generation` 区分新旧。Redis/Memcached 这类 KV 不需要"复合 key"；同样的 digest 上后写的 envelope 会覆盖老的。比对在新写入后被读出时完成——这部分代价是字节级比较 1 次 u64。

### Q3：为什么 singleflight 用 `Arc<Mutex<()>>` 而不是 `tokio::sync::Notify` ？

主仓库缓存用 `Notify`（有 follower 主动醒来读 cache 的场景）；`rbatis-cache` 不一样——get_or_load 模式里"leader 完成后所有 follower 都在同一处继续走"——`Mutex` 让 follower 在 `lock().await` 处被 leader drop guard 唤醒，省掉了 Notify 信号竞争。但**清理条目**不得不引入 `Arc::strong_count==2` 启发式，避免泄漏。

### Q4：为什么 `in_transaction=true` 一律旁路（不像主仓库那样有 Defer 模式）？

保守。`rbatis-cache` 是给多 backend 用的——事务中该看 DB 的内容还是 DB，避免缓存的"提交前、后读"不一致造成的脏读风险。语义简单：事务里 **不缓存**。**未来** 如果需要 Defer，需要在执行器层把"事务完成才 flush"也作为 backend 调用（与 `invalidate_after_commit` 对位）。

### Q5：为什么用 BLAKE3 而不是 xxh3？

两点：(a) 输出 32 字节直接 `to_hex` 得 64 字符，安全字符串对 Redis/Memcached 友好；(b) BLAKE3 文档级就强调 streaming API + Merkle tree，更适合作为"协议级"摘要。

### Q6：`message_pack()` 用 `to_vec_named` 而不是 `to_vec`？

`to_vec_named` 把字段名（`version` / `generation` / ...）一起编进流——后端即使在不同版本（rbs 1 vs rbs 2）也能写 self-describing 数据，未来跨版本兼容更稳。

### Q7：`max_value_size` 为什么是 1 MiB（默认）？

经验值——超出 1 MiB 的查询结果，**几乎都**不应该被缓存（让数据库承载）。本 crate 把 envelope 字节大小一并计入，超过 `1 MiB` 就只返回不写。

### Q8：为什么 Redis backend 还做熔断（circuit breaker）？

分布式场景中 Redis 短暂不可用是常见事件（短暂网络抖动、重启、主从切换）。如果 32 次连续 timeout 已经熔断，短暂不向 Redis 发请求——避免连接池被打满。`circuit_failure_threshold` + `circuit_cooldown` 让恢复自愈。

### Q9：为什么 Memcached backend 把同步 client 套在 `spawn_blocking` 里？

`memcache` crate（同步 crate）每个调用都是阻塞 IO；tokio 执行器不能容忍阻塞 -> `spawn_blocking` 把 worker 池（默认 512 线程）分一个线程做阻塞调用，async 调度不被卡死。

### Q10：`rbatis-cache` 与主仓库 `rbatis/src/plugin/cache/` 未来会合并吗？

还不确定。目前来看，主仓库的"内存 + 拦截 + 单飞 + 事务缓冲"是 rbatis 用户最常用的本地场景；`rbatis-cache` 是"分布式 backend"用例。两套同时存在可让 alpha 期间两边独立迭代。

---

## 10. codegraph 速查命令

（已索引：37 文件 / 412 节点 / 952 边；DB 1.27 MB）

```bash
export PATH="/Users/wandl/.nvm/versions/node/v24.18.0/bin:$PATH"
codegraph status                               # 健康度

# SPI
codegraph query "CacheBackend\|CachePolicy\|InvalidationStrategy\|CacheError"
codegraph query "CacheKey\|CacheKeyInput\|update_component"
codegraph query "CacheEnvelope\|is_fresh\|rmp_serde"
codegraph query "CacheInterceptor\|get_or_load\|release_flight\|flights"
codegraph query "SqlMetadata\|visit_relations\|StatementKind"
codegraph query "CacheMetrics\|CacheMetricsSnapshot"

# Backend
codegraph query "LocalBackend\|read_generation\|bump"
codegraph query "RedisCacheBackend\|RedisCacheConfig\|RedisMetricsSnapshot"
codegraph query "MemcachedCacheBackend\|MemcachedConfiguration\|ConsistentHashRing"

# Harness
codegraph query "assert_missing_key_is_none\|assert_generation_atomic\|assert_ttl_expires"
```

---

## 11. 推荐阅读顺序

1. `src/lib.rs` （顶部 doc table 已给 Java 对照表）
2. `src/backend.rs` + `src/key.rs` + `src/envelope.rs`（**契约三件套**）
3. `src/sql.rs` + `src/metrics.rs`
4. `src/error.rs`
5. `src/interceptor.rs`（**SPI 流程核心**：`get_or_load` 单方法流水线）
6. `src/rbatis_intercept.rs`（**执行器集成层**：before/after 两段式钩子）+
   `src/l1.rs` + `src/singleflight.rs` + `src/transactional.rs` + `src/listener.rs`
7. `src/plugin.rs`（`RbatisCacheExt::install_cache` 注册入口）
8. `src/local_backend.rs`（backend 实现的样板）
9. `src/testing.rs`（如何验证 backend 实现）
10. `rbatis-redis/src/redis_cache.rs` + `redis_config.rs`
11. `rbatis-memcached/src/memcached_cache.rs` + `consistent_hash.rs`
12. 跑 `cargo test --all -- --nocapture` 看契约测试 + `tests/cache_test.rs` 端到端

---

## 12. 已知 TODO / 未达成的能力

（直接来自源码注释 + workspace 设计说明）

1. **`InvalidationStrategy::TableGeneration` 是预留变体**——`TableGeneration` 利用 `table_tags` 做精细失效，但目前**只在 enum 里存在**，没有实现路径（`SqlMetadata.table_tags` 解析已完成，拦截器里它尚未参与 generation 路由）。
2. **`RedisCacheBackend` 未启用 cluster / sentinel / Pub-Sub 失效广播**：`RedisCacheConfig` 已留扩展点（如 `key_prefix`），但代码路径无集群相关字段。
3. **`rbatis-redis/src/serializer.rs`：`JdkSerializer` / `KryoSerializer` 为 type alias，但当前实际固定走 MessagePack**（`envelope.rs`）。dist 项是为了兼容 Java 侧的 `Serializer` 调用方，后续可按 enum dispatch 真实现多格式。
4. **`MemcachedCacheBackend` 的 generation 协议细节**：`add(0)` + `increment(1)` 兜底，但需要确认 memcached 服务器支持 `incr` 命令。
5. **执行器集成已落地**（2026-08）：`src/rbatis_intercept.rs::RbatisCacheInterceptor<B>` 实现
   `rbatis::intercept::Intercept` 的 `before`/`after` 两段式钩子，配合
   `src/plugin.rs::RbatisCacheExt::install_cache`（拦截器进 `RBatis::intercepts`、
   监听器进 `RBatis::listeners`，不改动 rbatis 本体）与 `src/listener.rs::CacheTransactionListener`
   （Begin 建缓冲 / Commit 冲刷 / Rollback 丢弃）。能力清单：
   - L1（`src/l1.rs`，per-executor、有界、L2→L1 提升）+
     L2（`CacheBackend` 字节级，BLAKE3 键，envelope 新鲜度）
   - 跨钩子 singleflight（`src/singleflight.rs`，Notify 版，follower 超时降级）
   - 事务模式 `Bypass`（默认）/ `Defer`（事务缓冲 `src/transactional.rs`）
   - `CacheFailureMode::FailOpen`（默认）/ `FailClosed`、`UseCacheFilter`、
     `cache_null` / `null_ttl`、`max_value_size`、`l1_max_entries`、`blocking`
   - FOR UPDATE / FOR SHARE 排除（`src/sql.rs::SqlMetadata::is_cacheable`）
   - DML（`rows_affected > 0`）→ 清 L1 + bump generation；事务内延迟到 commit
   - 端到端测试 `tests/cache_test.rs`（24 个，计数 MockDriver + `TEST_LOCK`
     串行化，覆盖 L1/L2 命中、DML 失效、事务、singleflight、fail-closed）
   - 上游依赖：`fix/transaction-listener` 分支（TransactionListener hook +
     apply_after 短路修复），发布后切 crates.io 版本。
6. **`Default for CacheInterceptor<B>` 缺失**：现在必须显式 `CacheInterceptor::new(backend, policy)`；看未来是否要加 `default()` 走 `LocalBackend`。
7. **CLI / 配置加载样板没内置**：与 RedisConfigurationBuilder / MemcachedConfigurationBuilder 不同，`CachePolicy` 暂时只能手写构造。
8. **测试：** `rbatis-redis` / `rbatis-memcached` 还没有引入 `feature = "testing"` + `run_all` 的本地集成测试（套件 `tests/cache_contract.rs` 仅覆盖 `LocalBackend`）。

---

如果你的目标是给 rbatis 应用加 Redis / Memcached 二级缓存，那么**入口**是：

```rust
use rbatis_cache::{CachePolicy, LocalBackend, RbatisCacheExt, RbatisCacheInterceptor};
use rbatis::RBatis;
use std::sync::Arc;

let rb = RBatis::new();
// 进程内 backend：LocalBackend；分布式：rbatis_redis::RedisCacheBackend ...
let cache = RbatisCacheInterceptor::new("ns", Arc::new(LocalBackend::new()), CachePolicy::default());
let listener = cache.listener();
rb.install_cache(Arc::new(cache), Some(Arc::new(listener)));
```

`RbatisCacheInterceptor`（执行器集成层）与 `CacheInterceptor::get_or_load`（手动流水线）
共用同一 `CacheBackend` SPI：前者面向 rbatis 拦截器链（before/after + 事务监听器），
后者供非 rbatis 场景直接调用。
